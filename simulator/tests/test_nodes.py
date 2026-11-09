"""The built-in capped dummy node."""

from __future__ import annotations

from mqsim import CappedNode, ExperimentManager, Interaction, Node, TrafficSpec


def test_capped_node_enforces_capacity_per_tick_across_deliveries():
    request = Interaction("request")
    sender = Node("sender", [request])
    node = CappedNode(node_id="node", interactions=[request], capacity_per_tick=3)
    manager = ExperimentManager(tick=1.0)
    manager.add_node(sender)
    manager.add_node(node)

    sender.schedule(
        "request", 0.0, [TrafficSpec("node", 4), TrafficSpec("node", 4)]
    )
    sender.schedule("request", 1.0, [TrafficSpec("node", 2)])

    manager.tick_once()  # t=0: one shared capacity of three
    assert (
        sum(s.value for s in node.observations.metric_samples("accepted_messages"))
        == 3
    )

    manager.tick_once()  # t=1: capacity resets
    assert (
        sum(s.value for s in node.observations.metric_samples("accepted_messages"))
        == 5
    )


def test_capped_node_without_capacity_accepts_everything():
    request = Interaction("request")
    sender = Node("sender", [request])
    node = CappedNode(node_id="node", interactions=[request])
    manager = ExperimentManager(tick=1.0)
    manager.add_node(sender)
    manager.add_node(node)
    sender.schedule("request", 0.0, [TrafficSpec("node", 9)])

    manager.tick_once()

    assert node.recent_metrics()["accepted_messages"] == 9
