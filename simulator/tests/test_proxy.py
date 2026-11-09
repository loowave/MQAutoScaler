"""Proxy ordering is deterministic for each receiver."""

from __future__ import annotations

from mqsim import ExperimentManager, Interaction, TrafficSpec

from .doubles import StubNode


class OrderRecordingNode(StubNode):
    def __init__(self, node_id, interactions, order):
        super().__init__(node_id, interactions)
        self.order = order

    def receive(self, delivery, now):
        self.order.append(delivery.sender)
        return super().receive(delivery, now)


def test_attempts_are_ordered_by_sender_not_by_registration_order():
    request = Interaction("request")
    order: list[str] = []
    sink = OrderRecordingNode("sink", [request], order)
    source_b = StubNode("b", [request])
    source_a = StubNode("a", [request])

    manager = ExperimentManager(tick=1.0)
    manager.add_node(source_b)
    manager.add_node(source_a)
    manager.add_node(sink)

    source_b.schedule("request", 0.0, [TrafficSpec("sink", 1)])
    source_a.schedule("request", 0.0, [TrafficSpec("sink", 1)])

    manager.tick_once()

    assert order == ["a", "b"]
