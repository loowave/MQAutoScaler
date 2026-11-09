"""Task deque behaviour: due ordering, pop_due and in-place head updates."""

from __future__ import annotations

from mqsim import EntryState, Task, TrafficSpec


def spec(receiver: str = "x", count: int = 1) -> TrafficSpec:
    return TrafficSpec(receiver, count)


def test_pop_due_returns_none_for_future_head_and_empty_task():
    task = Task("t")
    task.add_work(2.0, [spec(count=2)])
    task.add_work(1.0, [spec(count=1)])

    assert task.pop_due(0.5) is None
    assert task.pop_due(0.999) is None

    first = task.pop_due(1.0)
    assert first is not None
    assert first.due == 1.0
    assert first.message_count == 1

    assert task.pop_due(1.5) is None
    second = task.pop_due(2.0)
    assert second is not None and second.due == 2.0
    assert task.pop_due(3.0) is None
    assert len(task) == 0


def test_entries_are_kept_in_due_time_order():
    task = Task("t")
    task.add_work(3.0, [spec()])
    task.add_work(1.0, [spec()])
    task.add_work(2.0, [spec()])

    assert [entry.due for entry in task] == [1.0, 2.0, 3.0]


def test_new_work_updates_a_compatible_pending_head_in_place():
    task = Task("t")
    first = task.add_work(1.0, [spec("a")])
    second = task.add_work(1.0, [spec("b")])

    assert first is second
    assert len(task) == 1
    assert [entry.receiver for entry in first.specs] == ["a", "b"]

    separate = task.add_work(2.0, [spec("c")])
    assert separate is not first
    assert len(task) == 2


def test_retry_entries_are_inserted_separately_even_with_the_same_due_time():
    task = Task("t")
    pending = task.add_work(2.0, [spec("a")])
    retried = task.requeue(2.0, [spec("b")])

    assert len(task) == 2
    assert pending.state is EntryState.PENDING
    assert retried.state is EntryState.RETRY
    assert task.head is pending
