"""The experiment manager: owns the simulation clock and the tick protocol."""

from __future__ import annotations

from typing import Mapping

from .delivery import AdmissionResult
from .node import Node
from .observations import MetricSample, NodeObservation, UsageSample
from .proxy import Proxy
from .timeutil import TIME_EPSILON


class ExperimentManager:
    """Advances a fixed-step clock over registered nodes.

    Each tick:

    1. ``start_tick(now)`` is called on every node in registration order.
       A node returning from that call is the readiness acknowledgment.
    2. Only after every node returned, ``proxy.deliver(now)`` orders the tick's
       attempts, calls ``receive()`` on receivers and relays the results.
    3. Metric samples recorded during the tick are appended to
       ``manager.metrics``, observations are pruned against simulated time and
       the clock advances.

    ``manager.metrics`` is the experiment-wide metric history for now: it keeps
    every sample for the whole run, independent of per-node observation
    retention.  This makes it suitable for after-the-fact plotting.

    ``now`` is computed as ``start + step * tick`` so repeated addition does not
    accumulate floating-point drift.
    """

    def __init__(
        self,
        *,
        tick: float = 1.0,
        start: float = 0.0,
        proxy: Proxy | None = None,
    ) -> None:
        if tick <= 0:
            raise ValueError("tick must be positive")
        self.tick = float(tick)
        self.start = float(start)
        self.proxy = proxy if proxy is not None else Proxy()
        self._nodes: dict[str, Node] = {}
        self._step = 0
        # node id -> metric name -> samples, in simulated-time order.  Unlike
        # per-node observation stores this history is not pruned.
        self.metrics: dict[str, dict[str, list[MetricSample]]] = {}
        self._metric_cursors: dict[str, float | None] = {}
        # node id -> end-of-tick resource usage snapshots (also unpruned).
        self.usage: dict[str, list[UsageSample]] = {}

    @property
    def now(self) -> float:
        return self.start + self._step * self.tick

    @property
    def tick_times(self) -> tuple[float, ...]:
        """Timestamps of every tick executed so far."""
        return tuple(self.start + index * self.tick for index in range(self._step))

    @property
    def nodes(self) -> Mapping[str, Node]:
        return dict(self._nodes)

    def add_node(self, node: Node) -> Node:
        if node.id in self._nodes:
            raise ValueError(f"duplicate node id {node.id!r}")
        self._nodes[node.id] = node
        self.metrics.setdefault(node.id, {})
        self._metric_cursors.setdefault(node.id, None)
        self.usage.setdefault(node.id, [])
        return node

    def node(self, node_id: str) -> Node:
        return self._nodes[node_id]

    def tick_once(self) -> list[AdmissionResult]:
        """Run one start -> deliver -> observe -> advance cycle."""
        now = self.now
        ready: set[str] = set()
        for node_id, node in self._nodes.items():
            node.start_tick(now, self.proxy)
            ready.add(node_id)
        if len(ready) != len(self._nodes):
            raise RuntimeError("delivery requested before every node acknowledged readiness")

        results = self.proxy.deliver(self._nodes, now)

        self._collect_metrics(now)

        for node in self._nodes.values():
            node.observations.record_usage(now, node.usage)
            self.usage.setdefault(node.id, []).append(
                UsageSample(now, node.usage.copy())
            )
            node.observations.prune(now)

        self._step += 1
        return results

    def run(self, ticks: int) -> None:
        if ticks < 0:
            raise ValueError("ticks must be non-negative")
        for _ in range(ticks):
            self.tick_once()

    def run_until(self, until: float) -> None:
        """Tick while the clock is still before ``until`` (exclusive)."""
        while self.now < until - TIME_EPSILON:
            self.tick_once()

    def observe(self) -> dict[str, NodeObservation]:
        """Collect the current observation snapshot from every node."""
        return {node_id: node.observe() for node_id, node in self._nodes.items()}

    def metric_samples(self, node_id: str, name: str) -> tuple[MetricSample, ...]:
        """All stored samples of one metric for one node."""
        return tuple(self.metrics.get(node_id, {}).get(name, ()))

    def usage_samples(self, node_id: str) -> tuple[UsageSample, ...]:
        """All stored end-of-tick resource usage snapshots for one node."""
        return tuple(self.usage.get(node_id, ()))

    def _collect_metrics(self, now: float) -> None:
        """Move samples recorded during this tick from nodes into manager state."""
        for node_id, node in self._nodes.items():
            cursor = self._metric_cursors.get(node_id)
            samples = node.observations.metric_samples(since=cursor)
            if cursor is not None:
                # ``since`` is inclusive; only take samples recorded after the
                # previous collection point.
                samples = tuple(
                    sample
                    for sample in samples
                    if sample.timestamp > cursor + TIME_EPSILON
                )
            series = self.metrics.setdefault(node_id, {})
            for sample in samples:
                series.setdefault(sample.name, []).append(sample)
            self._metric_cursors[node_id] = now
