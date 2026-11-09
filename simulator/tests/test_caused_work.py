"""Caused interactions update existing tasks using only accepted work."""

from __future__ import annotations

from mqsim import (
    CausedInteraction,
    ExperimentManager,
    Interaction,
    SendingProfile,
    TrafficSpec,
)

from .doubles import StubNode


def test_receive_adds_caused_work_from_the_accepted_portion_only():
    reply = Interaction("reply")
    request = Interaction(
        "request",
        caused=(
            CausedInteraction(
                reply,
                delay=0.5,
                messages_per_accepted=1,
                receivers=("receiver",),
                profile=SendingProfile(bytes_per_message=16),
            ),
        ),
    )
    sender = StubNode("sender", [request])
    receiver = StubNode("receiver", interactions=[request], capacities=[6])
    manager = ExperimentManager(tick=1.0)
    manager.add_node(sender)
    manager.add_node(receiver)

    reply_task = receiver.task("reply")  # created at node initialization
    receiver.schedule("reply", 0.5, [TrafficSpec("receiver", 1)])
    sender.schedule("request", 0.0, [TrafficSpec("receiver", 10)])

    manager.tick_once()

    assert receiver.task("reply") is reply_task
    entry = reply_task.head
    assert entry.due == 0.5
    # the pre-existing spec plus one caused spec, merged into the existing head
    assert len(entry.specs) == 2
    caused = entry.specs[-1]
    assert caused.receiver == "receiver"
    assert caused.count == 6  # accepted prefix, not the requested 10
    assert caused.attempt == 1
    assert caused.profile.bytes_per_message == 16
    assert receiver.received == [(0.0, "request", 10, 6)]


def test_caused_work_is_scaled_by_messages_per_accepted():
    reply = Interaction("reply")
    request = Interaction(
        "request",
        caused=(CausedInteraction(reply, delay=1.0, messages_per_accepted=2,
                                  receivers=("receiver",)),),
    )
    receiver = StubNode("receiver", interactions=[request], capacities=[3])
    manager = ExperimentManager(tick=1.0)
    manager.add_node(receiver)

    # self-sent request: the node is both sender and receiver
    receiver.schedule("request", 0.0, [TrafficSpec("receiver", 5)])
    manager.tick_once()

    assert receiver.task("reply").head.message_count == 6  # 3 accepted * 2
    assert receiver.task("reply").head.due == 1.0


def test_work_scheduled_during_delivery_is_eligible_on_a_later_tick():
    reply = Interaction("reply")
    request = Interaction(
        "request",
        caused=(CausedInteraction(reply, delay=0.0, receivers=("receiver",)),),
    )
    receiver = StubNode("receiver", interactions=[request], capacities=[2, 2])
    manager = ExperimentManager(tick=1.0)
    manager.add_node(receiver)
    receiver.schedule("request", 0.0, [TrafficSpec("receiver", 2)])

    results = manager.tick_once()
    assert len(results) == 1  # only the request was delivered in this tick
    assert receiver.task("reply").head is not None
    assert receiver.task("reply").head.due == 0.0

    manager.tick_once()
    assert receiver.received[-1] == (1.0, "reply", 2, 2)
    assert len(receiver.task("reply")) == 0
