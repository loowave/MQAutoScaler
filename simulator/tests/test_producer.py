"""TrafficProducer: rate accumulation, batching, broadcast and retry."""

from __future__ import annotations

from mqsim import (
    ExperimentManager,
    Interaction,
    Node,
    ProducerTarget,
    RetryPolicy,
    TrafficProducer,
)

from .doubles import StubNode


class RecordingNode(Node):
    def __init__(self, node_id, interactions=(), **kwargs):
        super().__init__(node_id, interactions, **kwargs)
        self.batches: list[int] = []

    def receive(self, delivery, now):
        self.batches.append(delivery.batch.count)
        return super().receive(delivery, now)


def make_world(rate, tick, *, batch_size=1, start=0.0, stop=None, retry=None, sinks=1):
    request = Interaction("request")
    manager = ExperimentManager(tick=tick)
    nodes = [
        RecordingNode(f"sink{index}", [request])
        for index in range(sinks)
    ]
    for node in nodes:
        manager.add_node(node)
    producer = TrafficProducer(
        node_id="load",
        targets=[ProducerTarget(node.id, "request") for node in nodes],
        rate=rate,
        batch_size=batch_size,
        start=start,
        stop=stop,
        retry=retry,
    )
    manager.add_node(producer)
    return manager, producer, nodes


def generated(producer: TrafficProducer) -> float:
    return sum(
        sample.value
        for sample in producer.observations.metric_samples("generated_messages")
    )


def accepted(node: Node) -> float:
    return sum(
        sample.value
        for sample in node.observations.metric_samples("accepted_messages")
    )


def test_total_traffic_is_independent_of_tick_duration():
    coarse, coarse_producer, coarse_sinks = make_world(10.0, 1.0)
    fine, fine_producer, fine_sinks = make_world(10.0, 0.5)

    coarse.run(3)  # last tick at t=2
    fine.run(5)  # last tick at t=2

    assert generated(coarse_producer) == generated(fine_producer) == 20
    assert accepted(coarse_sinks[0]) == accepted(fine_sinks[0]) == 20


def test_producer_stops_at_its_stop_time():
    manager, producer, sinks = make_world(10.0, 1.0, stop=1.0)
    manager.run(5)  # ticks at 0..4

    assert generated(producer) == 10
    assert accepted(sinks[0]) == 10


def test_generated_traffic_is_split_into_batches_and_broadcast():
    manager, producer, sinks = make_world(4.0, 1.0, batch_size=2, sinks=2)
    manager.run(2)  # t=0 emits nothing, t=1 emits four messages

    assert generated(producer) == 4
    for sink in sinks:
        assert sorted(sink.batches) == [2, 2]
        assert accepted(sink) == 4


def test_producer_retries_rejected_traffic_using_its_task():
    request = Interaction("request")
    manager = ExperimentManager(tick=1.0)
    sink = StubNode("sink", interactions=[request], capacities=[0])
    producer = TrafficProducer(
        node_id="load",
        targets=[ProducerTarget("sink", "request")],
        rate=5.0,
        batch_size=5,
        start=0.0,
        retry=RetryPolicy(delay=1.0, max_attempts=3),
    )
    manager.add_node(sink)
    manager.add_node(producer)

    manager.run(3)  # t=1 rejects, t=2 accepts the retry

    metrics = producer.recent_metrics()
    assert metrics["retried_messages"] == 5
    assert "dropped_messages" not in metrics
    assert metrics["confirmed_messages"] == 5
    assert len(producer.task("request")) == 0
