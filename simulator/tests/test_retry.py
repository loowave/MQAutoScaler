"""Partial acceptance, retry attempts, limits and expiry."""

from __future__ import annotations

import pytest

from mqsim import (
    EntryState,
    ExperimentManager,
    Interaction,
    RetryPolicy,
    SendingProfile,
    TrafficSpec,
)

from .doubles import StubNode


def build(profile: SendingProfile, *, count: int, capacities=None, default_capacity=None):
    request = Interaction("request")
    sender = StubNode("sender", [request])
    receiver = StubNode(
        "receiver",
        interactions=[request],
        capacities=capacities,
        default_capacity=default_capacity,
    )
    manager = ExperimentManager(tick=1.0)
    manager.add_node(sender)
    manager.add_node(receiver)
    sender.schedule("request", 0.0, [TrafficSpec("receiver", count, profile)])
    return manager, sender, receiver


def test_partial_acceptance_retries_only_the_rejected_remainder():
    profile = SendingProfile(
        bytes_per_message=8, retry=RetryPolicy(delay=1.0, max_attempts=3)
    )
    manager, sender, receiver = build(profile, count=10, capacities=[4])

    results = manager.tick_once()

    assert len(results) == 1
    assert results[0].accepted == 4
    assert results[0].rejected == 6
    assert results[0].rejected_batch.bytes_per_message == 8

    task = sender.task("request")
    assert len(task) == 1
    retry_entry = task.head
    assert retry_entry.state is EntryState.RETRY
    assert retry_entry.due == 1.0
    assert retry_entry.message_count == 6
    assert [spec.attempt for spec in retry_entry.specs] == [2]
    assert [spec.count for spec in retry_entry.specs] == [6]

    manager.tick_once()
    assert receiver.received[-1] == (1.0, "request", 6, 6)
    assert len(task) == 0


def test_retry_stops_at_the_attempt_limit():
    profile = SendingProfile(retry=RetryPolicy(delay=1.0, max_attempts=2))
    manager, sender, _ = build(profile, count=5, default_capacity=0)

    manager.tick_once()
    assert sender.task("request").head.message_count == 5

    manager.tick_once()
    assert len(sender.task("request")) == 0
    assert sender.recent_metrics()["retried_messages"] == 5
    assert sender.recent_metrics()["dropped_messages"] == 5

    dropped = [log for log in sender.recent_logs() if log.message == "work_dropped"]
    assert len(dropped) == 1
    assert dropped[0].fields["attempts"] == 2


def test_retry_respects_expiry():
    profile = SendingProfile(
        retry=RetryPolicy(delay=2.0, max_attempts=5, expires_after=1.0)
    )
    manager, sender, _ = build(profile, count=3, default_capacity=0)

    manager.tick_once()

    assert len(sender.task("request")) == 0
    assert sender.recent_metrics()["dropped_messages"] == 3


def test_retry_is_scheduled_at_the_fixed_delay():
    profile = SendingProfile(retry=RetryPolicy(delay=2.5, max_attempts=4))
    manager, sender, receiver = build(profile, count=4, capacities=[1])

    manager.tick_once()
    assert sender.task("request").head.due == pytest.approx(2.5)

    manager.run(2)  # ticks at t=1 and t=2; retry is due only at t=2.5
    assert len(receiver.received) == 1

    manager.run(1)  # tick at t=3
    assert receiver.received[-1] == (3.0, "request", 3, 3)
    assert len(sender.task("request")) == 0
