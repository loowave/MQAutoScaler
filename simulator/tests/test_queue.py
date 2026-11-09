"""The built-in queue (Kafka-like) node."""

from __future__ import annotations

import pytest

from mqsim import (
    CappedNode,
    CausedInteraction,
    ExperimentManager,
    Interaction,
    Node,
    QueueNode,
    ResourceSettings,
    SendingProfile,
    TrafficSpec,
)


def make_queue_world(
    *,
    instances: int = 1,
    write_capacity_per_instance_per_tick: int | None = 30,
    read_capacity_per_instance_per_tick: int | None = 15,
    retention: float | None = None,
    settings: ResourceSettings | None = None,
    cpu_per_message: float = 0.0,
    ram_bytes_per_message: float = 0.0,
):
    read = Interaction("read")
    write = Interaction("write", caused=(CausedInteraction(read),))
    queue = QueueNode(
        node_id="queue",
        interactions=[write],
        settings=settings if settings is not None else ResourceSettings(instances=instances),
        write_interaction="write",
        read_interaction="read",
        write_capacity_per_instance_per_tick=write_capacity_per_instance_per_tick,
        read_capacity_per_instance_per_tick=read_capacity_per_instance_per_tick,
        retention=retention,
        cpu_per_message=cpu_per_message,
        ram_bytes_per_message=ram_bytes_per_message,
    )
    queue.routes = {"read": ("consumer",)}
    consumer = Node("consumer", [read])
    sender = Node("sender", [write])
    manager = ExperimentManager(tick=1.0)
    for node in (sender, queue, consumer):
        manager.add_node(node)
    return manager, sender, queue, consumer


def totals(node: Node, metric: str) -> float:
    return sum(sample.value for sample in node.observations.metric_samples(metric))


def test_queue_accepts_writes_and_drains_at_read_capacity():
    manager, sender, queue, consumer = make_queue_world(
        instances=3, write_capacity_per_instance_per_tick=10,
        read_capacity_per_instance_per_tick=5,
    )
    sender.schedule("write", 0.0, [TrafficSpec("queue", 40)])

    manager.tick_once()  # t=0: accepts 30 of 40, no backlog to drain yet
    assert queue.backlog_messages == 30
    assert totals(sender, "dropped_messages") == 10

    manager.tick_once()  # t=1: drains 15, backlog 15
    assert queue.backlog_messages == 15
    assert totals(consumer, "accepted_messages") == 15

    manager.tick_once()  # t=2: drains the remaining 15
    assert queue.backlog_messages == 0
    assert totals(consumer, "accepted_messages") == 30

    assert totals(queue, "accepted_messages") == 30
    assert totals(queue, "drained_messages") == 30
    assert totals(queue, "rejected_messages") == 10


def test_queue_keeps_backlog_without_downstream_receivers():
    manager, sender, queue, consumer = make_queue_world(
        instances=1, write_capacity_per_instance_per_tick=20,
        read_capacity_per_instance_per_tick=10,
    )
    queue.routes = {}
    sender.schedule("write", 0.0, [TrafficSpec("queue", 12)])

    manager.run(3)

    assert queue.backlog_messages == 12
    assert not consumer.observations.metric_samples("accepted_messages")


def test_queue_expires_messages_after_retention_without_readers():
    manager, sender, queue, consumer = make_queue_world(
        instances=1, write_capacity_per_instance_per_tick=20,
        read_capacity_per_instance_per_tick=10, retention=3.0,
    )
    queue.routes = {}
    sender.schedule("write", 0.0, [TrafficSpec("queue", 20)])

    manager.run(3)  # t=0,1,2: the oldest messages are two seconds old
    assert queue.backlog_messages == 20
    assert queue.backlog_bytes == 20 * 64
    assert queue.usage.disk_per_instance == 20 * 64

    manager.tick_once()  # t=3: age reaches the retention limit
    assert queue.backlog_messages == 0
    assert queue.backlog_bytes == 0
    assert queue.usage.disk_per_instance == 0
    assert totals(queue, "expired_messages") == 20
    assert not consumer.observations.metric_samples("accepted_messages")


def test_queue_expires_oldest_messages_before_reading():
    manager, sender, queue, consumer = make_queue_world(
        instances=1, write_capacity_per_instance_per_tick=20,
        read_capacity_per_instance_per_tick=5, retention=2.0,
    )
    sender.schedule("write", 0.0, [TrafficSpec("queue", 20)])

    manager.run(2)  # t=0: accept; t=1: read 5, backlog 15
    assert queue.backlog_messages == 15

    manager.tick_once()  # t=2: the remaining 15 expire before any read
    assert queue.backlog_messages == 0
    assert totals(queue, "drained_messages") == 5
    assert totals(queue, "expired_messages") == 15
    assert totals(consumer, "accepted_messages") == 5


def test_queue_rejects_non_positive_retention():
    read = Interaction("read")
    write = Interaction("write", caused=(CausedInteraction(read),))
    with pytest.raises(ValueError, match="retention"):
        QueueNode(
            node_id="queue",
            interactions=[write],
            write_interaction="write",
            read_interaction="read",
            retention=0,
        )


def test_queue_requires_read_interaction_to_be_caused_by_write():
    read = Interaction("read")
    unrelated = Interaction("unrelated")
    with pytest.raises(ValueError, match="not caused by"):
        QueueNode(
            node_id="queue",
            interactions=[unrelated],
            write_interaction="unrelated",
            read_interaction="read",
        )


def test_queue_rejects_absolute_and_per_instance_capacity_together():
    read = Interaction("read")
    write = Interaction("write", caused=(CausedInteraction(read),))
    with pytest.raises(ValueError, match="not both"):
        QueueNode(
            node_id="queue",
            interactions=[write],
            write_interaction="write",
            read_interaction="read",
            write_capacity_per_tick=10,
            write_capacity_per_instance_per_tick=2,
        )


def test_queue_without_capacities_accepts_and_drains_everything():
    manager, sender, queue, consumer = make_queue_world(
        instances=2,
        write_capacity_per_instance_per_tick=None,
        read_capacity_per_instance_per_tick=None,
    )
    sender.schedule("write", 0.0, [TrafficSpec("queue", 25)])

    manager.tick_once()  # t=0: accepted; drain runs before delivery
    assert queue.backlog_messages == 25
    manager.tick_once()  # t=1: unlimited drain
    assert queue.backlog_messages == 0
    assert totals(consumer, "accepted_messages") == 25


def test_queue_backlog_drives_ram_disk_cpu_and_network_usage():
    settings = ResourceSettings(
        instances=2,
        cpu_per_instance=1.0,
        ram_per_instance=10000.0,
        disk_per_instance=100000.0,
        network_per_instance=100000.0,
    )
    manager, sender, queue, consumer = make_queue_world(
        write_capacity_per_instance_per_tick=30,
        read_capacity_per_instance_per_tick=5,
        settings=settings,
        cpu_per_message=0.01,
        ram_bytes_per_message=100,
    )
    sender.schedule(
        "write",
        0.0,
        [TrafficSpec("queue", 200, SendingProfile(bytes_per_message=50))],
    )

    manager.tick_once()  # t=0: accept 60 of 200
    assert queue.backlog_messages == 60
    assert queue.backlog_bytes == 60 * 50
    assert queue.usage.cpu_per_instance == pytest.approx(60 * 0.01 / 2)
    assert queue.usage.network_per_instance == pytest.approx(60 * 50 / 2)
    assert queue.usage.ram_per_instance == pytest.approx(60 * 100 / 2)
    assert queue.usage.disk_per_instance == pytest.approx(60 * 50 / 2)

    manager.tick_once()  # t=1: drain 10, per-tick costs reset
    assert queue.backlog_messages == 50
    assert queue.usage.cpu_per_instance == pytest.approx(10 * 0.01 / 2)
    assert queue.usage.network_per_instance == pytest.approx(10 * 50 / 2)
    assert queue.usage.ram_per_instance == pytest.approx(50 * 100 / 2)
    assert queue.usage.disk_per_instance == pytest.approx(50 * 50 / 2)

    utilization = queue.usage.utilization(settings)
    assert utilization["cpu"] == pytest.approx(0.05)
    assert utilization["ram"] == pytest.approx(0.25)
    assert utilization["disk"] == pytest.approx(0.0125)
    assert utilization["network"] == pytest.approx(0.0025)


def test_capped_node_traffic_drives_resource_usage():
    request = Interaction("request")
    sender = Node("sender", [request])
    node = CappedNode(
        node_id="node",
        interactions=[request],
        settings=ResourceSettings(
            instances=2,
            cpu_per_instance=1.0,
            ram_per_instance=10000.0,
            disk_per_instance=10000.0,
            network_per_instance=10000.0,
        ),
        capacity_per_tick=100,
        cpu_per_message=0.01,
        ram_bytes_per_message=100,
        disk_bytes_per_message=50,
    )
    manager = ExperimentManager(tick=1.0)
    manager.add_node(sender)
    manager.add_node(node)
    sender.schedule(
        "request",
        0.0,
        [TrafficSpec("node", 100, SendingProfile(bytes_per_message=200))],
    )

    manager.tick_once()

    assert node.usage.cpu_per_instance == pytest.approx(0.5)
    assert node.usage.ram_per_instance == pytest.approx(5000)
    assert node.usage.disk_per_instance == pytest.approx(2500)
    assert node.usage.network_per_instance == pytest.approx(10000)
    assert node.observe().utilization["cpu"] == pytest.approx(0.5)

    manager.tick_once()  # no traffic: per-tick usage resets, disk persists
    assert node.usage.cpu_per_instance == 0
    assert node.usage.ram_per_instance == 0
    assert node.usage.network_per_instance == 0
    assert node.usage.disk_per_instance == pytest.approx(2500)


def test_capped_node_scales_capacity_by_instance_count():
    request = Interaction("request")
    sender = Node("sender", [request])
    node = CappedNode(
        node_id="node",
        interactions=[request],
        settings=ResourceSettings(instances=4),
        capacity_per_instance_per_tick=25,
    )
    assert node.capacity_per_tick == 100
    manager = ExperimentManager(tick=1.0)
    manager.add_node(sender)
    manager.add_node(node)
    sender.schedule("request", 0.0, [TrafficSpec("node", 120)])

    manager.tick_once()

    assert totals(node, "accepted_messages") == 100
    assert totals(sender, "dropped_messages") == 20
