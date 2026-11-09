"""Traffic producers: nodes that only generate traffic toward system inputs."""

from __future__ import annotations

import math
from dataclasses import dataclass

from .model import Interaction, RetryPolicy, SendingProfile, TrafficSpec
from .node import Node
from .timeutil import TIME_EPSILON


@dataclass(frozen=True)
class ProducerTarget:
    """One system input: a node id and the interaction to deliver there."""

    node: str
    interaction: str

    def __post_init__(self) -> None:
        if not self.node:
            raise ValueError("producer target node must be non-empty")
        if not self.interaction:
            raise ValueError("producer target interaction must be non-empty")


class TrafficProducer(Node):
    """Generates a constant message rate and sends it to configured inputs.

    Volume is accumulated over elapsed simulated time, so the total generated
    by a given timestamp is independent of tick size.  Messages are split into
    attempts of at most ``batch_size`` and broadcast to every target.  The
    producer has no incoming interactions, but it uses ordinary node tasks and
    the proxy, so rejected traffic follows the same retry policy as other flows.
    """

    def __init__(
        self,
        *,
        node_id: str,
        targets: tuple[ProducerTarget, ...] | list[ProducerTarget],
        rate: float,
        batch_size: int = 1,
        bytes_per_message: int = 64,
        start: float = 0.0,
        stop: float | None = None,
        retry: RetryPolicy | None = None,
        observation_retention: float = 60.0,
        observation_window: float = 1.0,
    ) -> None:
        if not targets:
            raise ValueError("a traffic producer needs at least one target")
        if rate < 0:
            raise ValueError("producer rate must be non-negative")
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if stop is not None and stop < start:
            raise ValueError("producer stop cannot be before start")

        self.targets = tuple(targets)
        unique_interactions = tuple(
            dict.fromkeys(target.interaction for target in self.targets)
        )
        super().__init__(
            node_id,
            [Interaction(name) for name in unique_interactions],
            observation_retention=observation_retention,
            observation_window=observation_window,
        )
        self.rate = float(rate)
        self.batch_size = int(batch_size)
        self.bytes_per_message = int(bytes_per_message)
        self.start = float(start)
        self.stop = None if stop is None else float(stop)
        self.profile = SendingProfile(
            bytes_per_message=self.bytes_per_message, retry=retry
        )
        self._last_now = self.start
        self._accumulator = 0.0

    def prepare_tick(self, now: float) -> None:
        if self.rate <= 0 or now <= self._last_now + TIME_EPSILON:
            return
        window_end = now if self.stop is None else min(now, self.stop)
        if window_end <= self._last_now + TIME_EPSILON:
            return

        elapsed = window_end - self._last_now
        self._last_now = window_end
        self._accumulator += self.rate * elapsed
        generated = int(math.floor(self._accumulator + TIME_EPSILON))
        if generated <= 0:
            return
        self._accumulator -= generated

        self.observations.record_metric(now, "generated_messages", generated)
        for target in self.targets:
            remaining = generated
            while remaining > 0:
                chunk = min(remaining, self.batch_size)
                self.schedule(
                    target.interaction,
                    now,
                    [TrafficSpec(target.node, chunk, self.profile)],
                )
                remaining -= chunk
