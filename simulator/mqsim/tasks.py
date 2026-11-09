"""Node-owned tasks: long-lived, due-time ordered queues of scheduled work.

A task groups flows that need to coordinate and exposes a mutable ``state``
dictionary so a result from one flow can affect another flow's later emissions.
Scheduled work is represented by :class:`TaskEntry`; delivery attempts and
admission results live in :mod:`mqsim.delivery` instead.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Iterator

from .model import TrafficSpec
from .timeutil import TIME_EPSILON, is_due, same_time


class EntryState(str, Enum):
    """Delivery state of a task entry."""

    PENDING = "pending"
    RETRY = "retry"


@dataclass
class TaskEntry:
    """A due timestamp plus the traffic specs to dispatch at that time."""

    due: float
    specs: list[TrafficSpec] = field(default_factory=list)
    state: EntryState = EntryState.PENDING

    def __post_init__(self) -> None:
        self.specs = list(self.specs)

    @property
    def message_count(self) -> int:
        return sum(spec.count for spec in self.specs)


class Task:
    """A due-time ordered queue of work owned by one node and interaction.

    Entries are kept ordered by ``due``.  Newly arriving work whose due time
    matches the current head is merged into that head while the head is still
    pending; otherwise a separate entry is inserted in due-time order.  Retry
    entries are always inserted separately because their attempt state is not
    interchangeable with freshly caused work.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.entries: deque[TaskEntry] = deque()
        self.state: dict[str, Any] = {}

    def __len__(self) -> int:
        return len(self.entries)

    def __iter__(self) -> Iterator[TaskEntry]:
        return iter(self.entries)

    @property
    def head(self) -> TaskEntry | None:
        return self.entries[0] if self.entries else None

    def add_work(self, due: float, specs: Iterable[TrafficSpec]) -> TaskEntry:
        """Schedule caused or external work, coalescing into a compatible head."""
        specs = list(specs)
        head = self.head
        if head is not None and head.state is EntryState.PENDING and same_time(head.due, due):
            head.specs.extend(specs)
            return head
        entry = TaskEntry(due=due, specs=specs)
        self._insert(entry)
        return entry

    def requeue(self, due: float, specs: Iterable[TrafficSpec]) -> TaskEntry:
        """Re-insert rejected work for a retry; never merges with pending work."""
        entry = TaskEntry(due=due, specs=list(specs), state=EntryState.RETRY)
        self._insert(entry)
        return entry

    def pop_due(self, now: float) -> TaskEntry | None:
        """Pop the head when due; return ``None`` for an empty or future head."""
        head = self.head
        if head is None or not is_due(head.due, now):
            return None
        return self.entries.popleft()

    def _insert(self, entry: TaskEntry) -> None:
        index = 0
        while index < len(self.entries) and self.entries[index].due <= entry.due + TIME_EPSILON:
            index += 1
        self.entries.insert(index, entry)

    def __repr__(self) -> str:
        return f"Task(name={self.name!r}, entries={list(self.entries)!r})"
