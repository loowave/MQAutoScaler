"""Built-in node implementations.

``CappedNode`` is a generic service-style node with a per-tick admission limit.
``QueueNode`` (registered as ``kafka``) adds a backlog: it accepts writes,
accumulates them, and drains at a bounded read rate toward downstream consumers.
"""

from __future__ import annotations

from collections import deque

from .model import TrafficSpec
from .node import Node
from .registry import register_node_type
from .timeutil import TIME_EPSILON


def _resolve_capacity(
    instances: int,
    absolute: int | None,
    per_instance: int | None,
    label: str,
) -> int | None:
    if absolute is not None and per_instance is not None:
        raise ValueError(
            f"set either {label}_per_tick or {label}_per_instance_per_tick, not both"
        )
    value = per_instance * instances if per_instance is not None else absolute
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label}_per_tick must be a non-negative integer")
    return value


def _cost(value: float, label: str) -> float:
    if value < 0:
        raise ValueError(f"{label} must be non-negative")
    return float(value)


@register_node_type("dummy")
class CappedNode(Node):
    """A dummy node with an optional per-tick admission capacity.

    Capacity can be an absolute number of messages per tick
    (``capacity_per_tick``) or scale with the configured instance count
    (``capacity_per_instance_per_tick``).  An omitted capacity means the node
    accepts everything.

    Traffic drives virtual resource usage through optional per-message costs:
    ``cpu_per_message`` and ``ram_bytes_per_message`` are per-tick costs,
    ``disk_bytes_per_message`` accumulates over the run, and network usage is
    the accepted bytes per tick.  Per-instance usage is divided by the instance
    count.  No concrete resource formula is imposed on custom nodes.
    """

    def __init__(
        self,
        *,
        node_id: str,
        interactions=(),
        settings=None,
        observation_retention: float = 60.0,
        observation_window: float = 1.0,
        capacity_per_tick: int | None = None,
        capacity_per_instance_per_tick: int | None = None,
        cpu_per_message: float = 0.0,
        ram_bytes_per_message: float = 0.0,
        disk_bytes_per_message: float = 0.0,
    ) -> None:
        super().__init__(
            node_id,
            interactions,
            settings=settings,
            observation_retention=observation_retention,
            observation_window=observation_window,
        )
        self.capacity_per_tick = _resolve_capacity(
            self.settings.instances,
            capacity_per_tick,
            capacity_per_instance_per_tick,
            "capacity",
        )
        self.cpu_per_message = _cost(cpu_per_message, "cpu_per_message")
        self.ram_bytes_per_message = _cost(
            ram_bytes_per_message, "ram_bytes_per_message"
        )
        self.disk_bytes_per_message = _cost(
            disk_bytes_per_message, "disk_bytes_per_message"
        )
        self._available = self.capacity_per_tick

    def prepare_tick(self, now: float) -> None:
        self._available = self.capacity_per_tick
        # CPU, RAM and network describe this tick's processing; disk is a level.
        self.usage.cpu_per_instance = 0.0
        self.usage.ram_per_instance = 0.0
        self.usage.network_per_instance = 0.0

    def capacity_for(self, delivery, now: float) -> int:
        if self._available is None:
            return delivery.batch.count
        return max(0, min(delivery.batch.count, self._available))

    def on_accept(self, delivery, accepted, now: float) -> None:
        super().on_accept(delivery, accepted, now)
        if self._available is not None:
            self._available -= accepted.count
        if accepted.count <= 0:
            return
        instances = self.settings.instances
        self.usage.cpu_per_instance += (
            accepted.count * self.cpu_per_message / instances
        )
        self.usage.ram_per_instance += (
            accepted.count * self.ram_bytes_per_message / instances
        )
        self.usage.disk_per_instance += (
            accepted.count * self.disk_bytes_per_message / instances
        )
        if self.settings.network_per_instance > 0:
            self.usage.network_per_instance += accepted.total_bytes / instances


@register_node_type("queue")
class QueueNode(Node):
    """A buffered message queue with bounded writes, reads and retention.

    The node receives ``write_interaction``, accepts up to its write capacity,
    and accumulates accepted messages in a FIFO backlog.  Each tick it first
    discards messages that have waited ``retention`` seconds, then drains up to
    its read capacity into ``read_interaction`` work toward the receivers of
    that interaction (resolved from the node's outgoing edges).  Rejected reads
    are retried through the normal task machinery; the backlog only contains
    messages that have not yet been handed to a consumer or expired.
    """

    def __init__(
        self,
        *,
        node_id: str,
        interactions=(),
        settings=None,
        observation_retention: float = 60.0,
        observation_window: float = 1.0,
        write_interaction: str,
        read_interaction: str,
        write_capacity_per_tick: int | None = None,
        write_capacity_per_instance_per_tick: int | None = None,
        read_capacity_per_tick: int | None = None,
        read_capacity_per_instance_per_tick: int | None = None,
        retention: float | None = None,
        cpu_per_message: float = 0.0,
        ram_bytes_per_message: float = 0.0,
    ) -> None:
        super().__init__(
            node_id,
            interactions,
            settings=settings,
            observation_retention=observation_retention,
            observation_window=observation_window,
        )
        self.write_interaction = write_interaction
        self.read_interaction = read_interaction

        write_definition = self.interactions.get(write_interaction)
        if write_definition is None:
            raise ValueError(
                f"queue node {node_id!r} does not receive {write_interaction!r}"
            )
        caused = next(
            (
                item
                for item in write_definition.caused
                if item.interaction.name == read_interaction
            ),
            None,
        )
        if caused is None:
            raise ValueError(
                f"{read_interaction!r} is not caused by {write_interaction!r} "
                f"on queue node {node_id!r}"
            )
        self._read_caused = caused

        self.write_capacity = _resolve_capacity(
            self.settings.instances,
            write_capacity_per_tick,
            write_capacity_per_instance_per_tick,
            "write_capacity",
        )
        self.read_capacity = _resolve_capacity(
            self.settings.instances,
            read_capacity_per_tick,
            read_capacity_per_instance_per_tick,
            "read_capacity",
        )
        if retention is not None and retention <= 0:
            raise ValueError("retention must be positive simulated seconds")
        self.retention = retention
        self.cpu_per_message = _cost(cpu_per_message, "cpu_per_message")
        self.ram_bytes_per_message = _cost(
            ram_bytes_per_message, "ram_bytes_per_message"
        )
        self._available_writes = self.write_capacity
        self.backlog_messages = 0
        self.backlog_bytes = 0
        # FIFO batches of (accepted_at, message_count, bytes_per_message).
        # Messages of the same arrival tick share one batch.
        self._backlog: deque[tuple[float, int, int]] = deque()

    # -- resource usage ------------------------------------------------

    def _charge(self, messages: int, byte_count: int) -> None:
        """Add this tick's processing cost for writes or reads."""
        if messages <= 0:
            return
        instances = self.settings.instances
        self.usage.cpu_per_instance += messages * self.cpu_per_message / instances
        if self.settings.network_per_instance > 0:
            self.usage.network_per_instance += byte_count / instances

    def _refresh_backlog_usage(self) -> None:
        """RAM and disk are levels derived from the current backlog."""
        instances = self.settings.instances
        self.usage.ram_per_instance = (
            self.backlog_messages * self.ram_bytes_per_message / instances
        )
        self.usage.disk_per_instance = self.backlog_bytes / instances

    # -- write side ----------------------------------------------------

    def prepare_tick(self, now: float) -> None:
        self._available_writes = self.write_capacity
        self.usage.cpu_per_instance = 0.0
        self.usage.network_per_instance = 0.0
        self._expire(now)
        self._drain(now)

    def capacity_for(self, delivery, now: float) -> int:
        if self._available_writes is None:
            return delivery.batch.count
        return max(0, min(delivery.batch.count, self._available_writes))

    def on_accept(self, delivery, accepted, now: float) -> None:
        super().on_accept(delivery, accepted, now)
        if self._available_writes is not None:
            self._available_writes -= accepted.count
        self._charge(accepted.count, accepted.total_bytes)

    def _add_caused_work(self, interaction_name: str, accepted, now: float) -> None:
        if interaction_name != self.write_interaction:
            super()._add_caused_work(interaction_name, accepted, now)
            return
        self.backlog_messages += accepted.count
        self.backlog_bytes += accepted.total_bytes
        self._backlog.append(
            (now, accepted.count, accepted.bytes_per_message)
        )
        self._refresh_backlog_usage()
        self.observations.record_metric(now, "backlog_messages", self.backlog_messages)

    # -- read side -----------------------------------------------------

    def _expire(self, now: float) -> int:
        """Drop messages that have spent ``retention`` seconds in the backlog."""
        if self.retention is None:
            return 0
        expired = 0
        expired_bytes = 0
        while self._backlog:
            accepted_at, count, bytes_per_message = self._backlog[0]
            if now - accepted_at < self.retention - TIME_EPSILON:
                break
            self._backlog.popleft()
            expired += count
            expired_bytes += count * bytes_per_message
        if expired:
            self.backlog_messages -= expired
            self.backlog_bytes -= expired_bytes
            self._refresh_backlog_usage()
            self.observations.record_metric(now, "expired_messages", expired)
            self.observations.log(
                now,
                "messages_expired",
                level="warning",
                messages=expired,
                retention=self.retention,
            )
        return expired

    def _take(self, count: int) -> tuple[int, int]:
        """Remove up to ``count`` messages from the front of the FIFO backlog."""
        taken = 0
        taken_bytes = 0
        while taken < count and self._backlog:
            accepted_at, size, bytes_per_message = self._backlog[0]
            chunk = min(size, count - taken)
            taken += chunk
            taken_bytes += chunk * bytes_per_message
            if chunk == size:
                self._backlog.popleft()
            else:
                self._backlog[0] = (
                    accepted_at,
                    size - chunk,
                    bytes_per_message,
                )
        if taken:
            self.backlog_messages -= taken
            self.backlog_bytes -= taken_bytes
            self._refresh_backlog_usage()
        return taken, taken_bytes

    def _drain(self, now: float) -> None:
        if self.backlog_messages <= 0:
            self.observations.record_metric(now, "backlog_messages", 0)
            return
        receivers = self.receivers_for(self._read_caused)
        if not receivers:
            self.observations.record_metric(
                now, "backlog_messages", self.backlog_messages
            )
            return

        capacity = (
            self.backlog_messages
            if self.read_capacity is None
            else self.read_capacity
        )
        drained, drained_bytes = self._take(
            min(self.backlog_messages, capacity)
        )
        if drained <= 0:
            self.observations.record_metric(
                now, "backlog_messages", self.backlog_messages
            )
            return

        self._charge(drained, drained_bytes)
        due = now + self._read_caused.delay
        self.schedule(
            self.read_interaction,
            due,
            [
                TrafficSpec(
                    receiver=receiver,
                    count=drained,
                    profile=self._read_caused.profile,
                )
                for receiver in receivers
            ],
        )
        self.observations.record_metric(now, "drained_messages", drained)
        self.observations.record_metric(now, "backlog_messages", self.backlog_messages)


# Kafka is the queue-shaped node in the reference pipeline.
register_node_type("kafka", QueueNode)
