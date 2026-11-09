"""The reference client -> backend -> Kafka -> consumer -> Postgres pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from mqsim import Node, QueueNode, load_experiment

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = REPO_ROOT / "examples" / "pipeline"
PIPELINE2_DIR = REPO_ROOT / "examples" / "pipeline2"


def load_pipeline():
    return load_experiment(*sorted(PIPELINE_DIR.glob("*.yaml")))


def load_pipeline2():
    return load_experiment(*sorted(PIPELINE2_DIR.glob("*.yaml")))


def totals(node: Node, metric: str) -> float:
    return sum(sample.value for sample in node.observations.metric_samples(metric))


def test_pipeline_topology_and_scaled_capacities():
    experiment = load_pipeline()

    assert set(experiment.nodes) == {"backend", "consumer", "kafka", "postgres"}
    backend = experiment.node("backend")
    kafka = experiment.node("kafka")
    consumer = experiment.node("consumer")
    postgres = experiment.node("postgres")

    assert isinstance(kafka, QueueNode)
    assert backend.settings.instances == 5
    assert kafka.settings.instances == 3
    assert consumer.settings.instances == 2
    assert postgres.settings.instances == 1

    # 5 instances * 40 messages, 3 * 400 writes, 3 * 50 reads.
    assert backend.capacity_per_tick == 200
    assert kafka.write_capacity == 1200
    assert kafka.read_capacity == 150

    assert backend.routes == {"topic_write": ("kafka",)}
    assert kafka.routes == {"topic_read": ("consumer",)}
    assert consumer.routes == {"db_write": ("postgres",)}


def test_pipeline_flow_after_five_ticks():
    experiment = load_pipeline()
    backend = experiment.node("backend")
    kafka = experiment.node("kafka")
    consumer = experiment.node("consumer")
    postgres = experiment.node("postgres")

    experiment.run(5)

    # 180 messages/tick reach the backend from t=1..4.
    assert totals(backend, "accepted_messages") == 720
    # The backend writes the same volume to Kafka from t=2..4.
    assert totals(kafka, "accepted_messages") == 540
    # Kafka serves 150 messages/tick, so it falls behind 30 messages/tick.
    assert totals(kafka, "drained_messages") == 300
    assert kafka.backlog_messages == 240
    # The consumer forwards reads, the database stores them, one tick later.
    assert totals(consumer, "accepted_messages") == 300
    assert totals(postgres, "accepted_messages") == 150

    # Traffic drives per-instance resource utilization.  These are end-of-tick
    # snapshots: the backend processed 180 messages in the last tick, Kafka
    # read 150 and holds 240 messages in its backlog.
    assert experiment.manager.usage_samples("backend")[-1].usage.utilization(
        backend.settings
    )["cpu"] == pytest.approx(0.36)
    kafka_usage = experiment.manager.usage_samples("kafka")[-1].usage
    assert kafka_usage.utilization(kafka.settings)["cpu"] == pytest.approx(0.22)
    assert kafka_usage.disk_per_instance == pytest.approx(240 * 512 / 3)
    assert experiment.manager.usage_samples("consumer")[-1].usage.utilization(
        consumer.settings
    )["cpu"] == pytest.approx(0.75)
    assert experiment.manager.usage_samples("postgres")[-1].usage.utilization(
        postgres.settings
    )["disk"] == pytest.approx(150 * 1024 / 10_000_000)

    # Nothing overflows in the reference scenario.
    for node in (backend, kafka, consumer, postgres):
        assert not node.observations.metric_samples("dropped_messages")

    backlog_windows = kafka.observations.metric_windows("backlog_messages")
    assert [(window.start, window.last) for window in backlog_windows] == [
        (0.0, 0.0),
        (1.0, 0.0),
        (2.0, 180.0),
        (3.0, 210.0),
        (4.0, 240.0),
    ]


def test_pipeline_drains_once_the_producer_stops():
    experiment = load_pipeline()
    kafka = experiment.node("kafka")

    experiment.run(10)  # still producing: the backlog grows by 30 per tick
    assert kafka.backlog_messages == 390

    experiment.run(100)  # no new traffic; the queue works off its backlog
    assert kafka.backlog_messages == 0
    # The producer emits 180 messages/tick for 60 simulated seconds; every
    # message accepted by Kafka is eventually read by the consumer.
    assert totals(kafka, "accepted_messages") == 10_800
    assert totals(kafka, "drained_messages") == 10_800


def test_overloaded_pipeline_expires_messages_from_kafka():
    experiment = load_pipeline2()
    backend = experiment.node("backend")
    kafka = experiment.node("kafka")
    consumer = experiment.node("consumer")
    postgres = experiment.node("postgres")

    assert kafka.retention == 10.0
    assert kafka.read_capacity == 60
    assert consumer.capacity_per_tick == 60

    experiment.run(20)

    # The backend accepts everything, but Kafka serves only 60 of the
    # 180 messages arriving each tick.
    assert totals(backend, "accepted_messages") == 3420
    assert totals(kafka, "accepted_messages") == 3240
    assert totals(kafka, "drained_messages") == 1020

    # From t=16 on, the queue discards 120 messages per tick.
    assert totals(kafka, "expired_messages") == 480
    expired_windows = kafka.observations.metric_windows("expired_messages")
    assert [(w.start, w.total) for w in expired_windows] == [
        (16.0, 120.0),
        (17.0, 120.0),
        (18.0, 120.0),
        (19.0, 120.0),
    ]

    # The backlog plateaus because intake equals reads plus expiry.
    assert kafka.backlog_messages == 1740
    backlog_windows = kafka.observations.metric_windows("backlog_messages")
    assert [w.last for w in backlog_windows[-5:]] == [1740.0] * 5

    assert totals(consumer, "accepted_messages") == 1020
    assert totals(postgres, "accepted_messages") == 960

    # Kafka's disk and RAM track its growing backlog; the consumer is only
    # half as busy as in pipeline, while Postgres accumulates stored rows.
    kafka_usage = experiment.manager.usage_samples("kafka")[-1].usage
    assert kafka_usage.disk_per_instance == pytest.approx(1740 * 512 / 3)
    assert kafka_usage.utilization(kafka.settings)["disk"] == pytest.approx(
        1740 * 512 / 3 / 2_000_000
    )
    assert experiment.manager.usage_samples("consumer")[-1].usage.utilization(
        consumer.settings
    )["cpu"] == pytest.approx(0.3)
    assert experiment.manager.usage_samples("postgres")[-1].usage.utilization(
        postgres.settings
    )["disk"] == pytest.approx(960 * 1024 / 10_000_000)

    # No capacity rejections anywhere: Kafka's read rate matches the
    # consumer's capacity and the bottleneck is retention, not admission.
    for node in (backend, kafka, consumer, postgres):
        assert not node.observations.metric_samples("dropped_messages")
