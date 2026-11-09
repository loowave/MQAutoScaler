"""Small test doubles used across the test modules."""

from __future__ import annotations

from mqsim import Delivery, Node


class StubNode(Node):
    """Node with a scripted admission capacity and a receive log.

    ``capacities`` is consumed one entry per ``receive()`` call; once exhausted
    the node accepts the full batch.  ``default_capacity`` can instead force a
    constant capacity.  This keeps resource formulas out of the framework tests.
    """

    def __init__(
        self,
        node_id: str,
        interactions=(),
        *,
        capacities: list[int] | None = None,
        default_capacity: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(node_id, interactions, **kwargs)
        self.capacities = list(capacities) if capacities is not None else None
        self.default_capacity = default_capacity
        self.received: list[tuple[float, str, int, int]] = []

    def capacity_for(self, delivery: Delivery, now: float) -> int:
        if self.capacities:
            return self.capacities.pop(0)
        if self.default_capacity is not None:
            return self.default_capacity
        return delivery.batch.count

    def receive(self, delivery: Delivery, now: float) -> int:
        accepted = super().receive(delivery, now)
        self.received.append((now, delivery.interaction, delivery.batch.count, accepted))
        return accepted
