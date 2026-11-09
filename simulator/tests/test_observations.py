"""Observation retention and fixed one-second windows follow simulated time."""

from __future__ import annotations

import pytest

from mqsim import ExperimentManager, Node, ObservationStore, ResourceUsage

from .doubles import StubNode


def test_retention_is_measured_in_simulated_seconds():
    store = ObservationStore(retention=5.0)
    store.log(0.0, "old entry")
    store.record_usage(0.0, ResourceUsage(cpu_per_instance=0.5))
    store.record_metric(0.0, "m", 1.0)
    store.record_metric(3.0, "m", 2.0)

    store.prune(6.0)  # cutoff = 1.0

    assert [sample.timestamp for sample in store.metric_samples("m")] == [3.0]
    assert store.logs() == ()
    assert store.usage_samples() == ()


def test_manager_prunes_observation_history_using_the_simulated_clock():
    node = Node("n", observation_retention=2.0)
    manager = ExperimentManager(tick=1.0)
    manager.add_node(node)
    node.observations.record_metric(0.0, "m", 1.0)

    manager.run(4)  # ticks at 0, 1, 2, 3; prune at 3 has cutoff 1

    assert node.observations.metric_samples("m") == ()


def test_one_second_windows_are_independent_of_tick_duration():
    store = ObservationStore(retention=60.0, window=1.0)
    for timestamp in (0.0, 0.25, 0.5, 0.75, 1.0, 1.25):
        store.record_metric(timestamp, "q", timestamp)

    windows = store.metric_windows("q")

    assert [(window.start, window.count) for window in windows] == [(0.0, 4), (1.0, 2)]
    assert windows[0].mean == pytest.approx(0.375)
    assert windows[0].last == 0.75
    assert windows[0].end == 1.0
    assert windows[1].last == 1.25


def test_node_exposes_metrics_logs_and_virtual_resource_usage():
    node = StubNode("n", observation_retention=10.0)
    node.usage.cpu_per_instance = 0.5
    node.usage.ram_per_instance = 0.25

    node.observations.record_metric(0.0, "accepted_messages", 4)
    node.observations.log(0.0, "received", interaction="x", accepted=4)
    node.observations.record_usage(0.0, node.usage)

    observation = node.observe()

    assert observation.node_id == "n"
    assert observation.metrics["accepted_messages"] == 4
    assert observation.logs[0].fields == {"interaction": "x", "accepted": 4}
    assert observation.usage.cpu_per_instance == 0.5
    assert observation.utilization["cpu"] == pytest.approx(0.5)
