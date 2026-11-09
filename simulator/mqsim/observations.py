"""In-memory observations with simulated-time retention and fixed windows.

Each node owns an :class:`ObservationStore`.  Records are pruned against the
simulated clock, not wall-clock time.  Metric windows aggregate by simulated
timestamp, so a one-second window works the same whether ticks are 1 s or 50 ms.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from .resources import ResourceSettings, ResourceUsage
from .timeutil import TIME_EPSILON


@dataclass(frozen=True)
class MetricSample:
    timestamp: float
    name: str
    value: float


@dataclass(frozen=True)
class LogRecord:
    timestamp: float
    level: str
    message: str
    fields: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class UsageSample:
    timestamp: float
    usage: ResourceUsage


@dataclass(frozen=True)
class MetricWindow:
    """Aggregate of one metric over a window aligned to ``start``."""

    metric: str
    start: float
    end: float
    count: int
    total: float
    mean: float
    last: float
    samples: tuple[MetricSample, ...]


@dataclass(frozen=True)
class NodeObservation:
    """Read-only-ish snapshot returned by a node's observation interface."""

    node_id: str
    timestamp: float
    metrics: Mapping[str, float]
    logs: tuple[LogRecord, ...]
    usage: ResourceUsage
    utilization: Mapping[str, float]


class ObservationStore:
    """Bounded history of metrics, structured logs and resource usage."""

    def __init__(self, retention: float = 60.0, window: float = 1.0) -> None:
        if retention <= 0:
            raise ValueError("retention must be positive simulated seconds")
        if window <= 0:
            raise ValueError("window must be positive simulated seconds")
        self.retention = float(retention)
        self.window = float(window)
        self._now = 0.0
        self._metrics: list[MetricSample] = []
        self._logs: list[LogRecord] = []
        self._usage: list[UsageSample] = []

    # -- recording -----------------------------------------------------

    def record_metric(self, now: float, name: str, value: float) -> None:
        self._advance(now)
        self._metrics.append(MetricSample(now, name, float(value)))
        self._prune()

    def record_metrics(self, now: float, values: Mapping[str, float]) -> None:
        for name, value in values.items():
            self.record_metric(now, name, value)

    def log(self, now: float, message: str, *, level: str = "info", **fields: Any) -> None:
        self._advance(now)
        self._logs.append(LogRecord(now, level, message, dict(fields)))
        self._prune()

    def record_usage(self, now: float, usage: ResourceUsage) -> None:
        self._advance(now)
        self._usage.append(UsageSample(now, usage.copy()))
        self._prune()

    # -- retention -----------------------------------------------------

    def prune(self, now: float | None = None) -> None:
        """Drop records older than ``retention`` seconds before ``now``."""
        if now is not None:
            if now < self._now - TIME_EPSILON:
                raise ValueError("observation clock cannot move backwards")
            self._now = now
        self._prune()

    def _advance(self, now: float) -> None:
        if now < self._now - TIME_EPSILON:
            raise ValueError("observation clock cannot move backwards")
        self._now = max(self._now, now)

    def _prune(self) -> None:
        cutoff = self._now - self.retention
        self._metrics = [s for s in self._metrics if s.timestamp >= cutoff - TIME_EPSILON]
        self._logs = [r for r in self._logs if r.timestamp >= cutoff - TIME_EPSILON]
        self._usage = [s for s in self._usage if s.timestamp >= cutoff - TIME_EPSILON]

    @property
    def now(self) -> float:
        """Latest simulated timestamp observed by this store."""
        return self._now

    # -- read interface ------------------------------------------------

    def metric_samples(
        self, name: str | None = None, *, since: float | None = None
    ) -> tuple[MetricSample, ...]:
        return tuple(
            sample
            for sample in self._metrics
            if (name is None or sample.name == name)
            and (since is None or sample.timestamp >= since - TIME_EPSILON)
        )

    def latest_metrics(self) -> dict[str, float]:
        latest: dict[str, float] = {}
        for sample in self._metrics:
            latest[sample.name] = sample.value
        return latest

    def metric_windows(
        self, name: str, *, size: float | None = None
    ) -> list[MetricWindow]:
        """Aggregate a metric into windows aligned to multiples of ``size``."""
        size = self.window if size is None else float(size)
        if size <= 0:
            raise ValueError("window size must be positive")
        buckets: dict[float, list[MetricSample]] = {}
        for sample in self._metrics:
            if sample.name != name:
                continue
            start = math.floor(sample.timestamp / size + TIME_EPSILON) * size
            buckets.setdefault(start, []).append(sample)
        windows: list[MetricWindow] = []
        for start in sorted(buckets):
            samples = buckets[start]
            total = sum(sample.value for sample in samples)
            windows.append(
                MetricWindow(
                    metric=name,
                    start=start,
                    end=start + size,
                    count=len(samples),
                    total=total,
                    mean=total / len(samples),
                    last=samples[-1].value,
                    samples=tuple(samples),
                )
            )
        return windows

    def logs(
        self, *, since: float | None = None, level: str | None = None
    ) -> tuple[LogRecord, ...]:
        return tuple(
            record
            for record in self._logs
            if (since is None or record.timestamp >= since - TIME_EPSILON)
            and (level is None or record.level == level)
        )

    def usage_samples(self) -> tuple[UsageSample, ...]:
        return tuple(self._usage)

    def latest_usage(self) -> ResourceUsage:
        return self._usage[-1].usage.copy() if self._usage else ResourceUsage()

    def snapshot(
        self, node_id: str, settings: ResourceSettings | None = None
    ) -> NodeObservation:
        usage = self.latest_usage()
        return NodeObservation(
            node_id=node_id,
            timestamp=self._now,
            metrics=self.latest_metrics(),
            logs=self.logs(),
            usage=usage,
            utilization=usage.utilization(settings) if settings is not None else {},
        )
