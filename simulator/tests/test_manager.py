"""Tick protocol: readiness before delivery and the fixed-step clock."""

from __future__ import annotations

import pytest

from mqsim import ExperimentManager, Interaction, Proxy, TrafficSpec

from .doubles import StubNode


class EventNode(StubNode):
    def __init__(self, node_id, events, interactions=()):
        super().__init__(node_id, interactions)
        self.events = events

    def start_tick(self, now, proxy):
        self.events.append(("start_tick", self.id, now))
        super().start_tick(now, proxy)


class EventProxy(Proxy):
    def __init__(self, events):
        super().__init__()
        self.events = events

    def deliver(self, nodes, now):
        self.events.append(("deliver", tuple(sorted(nodes)), now))
        return super().deliver(nodes, now)


def test_all_nodes_acknowledge_before_any_delivery():
    events: list[tuple] = []
    request = Interaction("request")
    first = EventNode("a", events, [request])
    second = EventNode("b", events, [request])
    proxy = EventProxy(events)
    manager = ExperimentManager(tick=1.0, proxy=proxy)
    manager.add_node(first)
    manager.add_node(second)
    first.schedule("request", 0.0, [TrafficSpec("b", 1)])

    manager.tick_once()

    assert [event[0] for event in events] == ["start_tick", "start_tick", "deliver"]
    assert {events[0][1], events[1][1]} == {"a", "b"}
    assert events[2][1] == ("a", "b")
    assert events[2][2] == 0.0


def test_no_delivery_when_a_node_does_not_finish_start_tick():
    events: list[tuple] = []
    request = Interaction("request")

    class FailingNode(EventNode):
        def start_tick(self, now, proxy):
            self.events.append(("start_tick", self.id, now))
            raise RuntimeError("cannot prepare work")

    proxy = EventProxy(events)
    manager = ExperimentManager(tick=1.0, proxy=proxy)
    manager.add_node(EventNode("a", events, [request]))
    manager.add_node(FailingNode("b", events, [request]))

    with pytest.raises(RuntimeError, match="cannot prepare work"):
        manager.tick_once()

    assert [event[0] for event in events] == ["start_tick", "start_tick"]


def test_clock_advances_in_configurable_fixed_steps():
    manager = ExperimentManager(tick=0.05)
    assert manager.now == 0.0
    manager.run(4)
    assert manager.now == pytest.approx(0.2)
    manager.run(2)
    assert manager.now == pytest.approx(0.3)


def test_run_until_stops_before_the_boundary():
    manager = ExperimentManager(tick=0.25)
    manager.run_until(1.0)
    assert manager.now == pytest.approx(1.0)
    assert manager._step == 4


def test_manager_exposes_executed_tick_times():
    manager = ExperimentManager(tick=0.5, start=1.0)
    assert manager.tick_times == ()

    manager.run(3)

    assert manager.tick_times == (1.0, 1.5, 2.0)


def test_manager_keeps_metric_history_beyond_node_retention():
    class MetricNode(StubNode):
        def prepare_tick(self, now):
            self.observations.record_metric(now, "queue_depth", now)

    node = MetricNode("a", observation_retention=2.0)
    manager = ExperimentManager(tick=1.0)
    manager.add_node(node)

    manager.run(10)

    samples = manager.metric_samples("a", "queue_depth")
    assert [sample.timestamp for sample in samples] == [float(t) for t in range(10)]
    assert [sample.value for sample in samples] == [float(t) for t in range(10)]
    # The node's bounded store has pruned most of the same history.
    assert len(node.observations.metric_samples("queue_depth")) == 3


def test_manager_collects_every_sample_from_a_tick_without_duplicates():
    class ChattyNode(StubNode):
        def prepare_tick(self, now):
            self.observations.record_metric(now, "value", 1)
            self.observations.record_metric(now, "value", 2)

    node = ChattyNode("a")
    manager = ExperimentManager(tick=1.0)
    manager.add_node(node)

    manager.tick_once()
    manager.tick_once()

    samples = manager.metric_samples("a", "value")
    assert [sample.value for sample in samples] == [1.0, 2.0, 1.0, 2.0]
    assert [sample.timestamp for sample in samples] == [0.0, 0.0, 1.0, 1.0]


def test_manager_keeps_resource_usage_history_beyond_node_retention():
    class ResourceNode(StubNode):
        def prepare_tick(self, now):
            self.usage.cpu_per_instance = now

    node = ResourceNode("a", observation_retention=1.0)
    manager = ExperimentManager(tick=1.0)
    manager.add_node(node)

    manager.run(4)

    samples = manager.usage_samples("a")
    assert [sample.timestamp for sample in samples] == [0.0, 1.0, 2.0, 3.0]
    assert [sample.usage.cpu_per_instance for sample in samples] == [
        0.0,
        1.0,
        2.0,
        3.0,
    ]
    # The node's bounded store has pruned most of the same history.
    assert len(node.observations.usage_samples()) == 2
