"""Declarative descriptions of interactions and the traffic they schedule.

These types are plain Python values that callers instantiate.  A node receives
an :class:`Interaction`, accepts some messages, and the interaction's
:class:`CausedInteraction` entries describe how much follow-up work is added,
when it becomes due, and where it is sent.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class Batch:
    """A unit of traffic: a message count and the size of each message."""

    count: int
    bytes_per_message: int = 64

    def __post_init__(self) -> None:
        if self.count < 0:
            raise ValueError("batch count must be non-negative")
        if self.bytes_per_message < 0:
            raise ValueError("bytes_per_message must be non-negative")

    @property
    def total_bytes(self) -> int:
        return self.count * self.bytes_per_message


@dataclass(frozen=True)
class RetryPolicy:
    """A fixed-delay retry policy with an attempt limit and an optional expiry.

    ``max_attempts`` counts the first delivery attempt.  ``expires_after`` is a
    duration in simulated seconds measured from the time work becomes due; the
    absolute deadline is computed when the work is scheduled.
    """

    delay: float = 1.0
    max_attempts: int = 3
    expires_after: float | None = None

    def __post_init__(self) -> None:
        if self.delay < 0:
            raise ValueError("retry delay must be non-negative")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.expires_after is not None and self.expires_after < 0:
            raise ValueError("expires_after must be non-negative or None")


@dataclass(frozen=True)
class SendingProfile:
    """How a node sends one flow: payload size and retry behaviour."""

    bytes_per_message: int = 64
    retry: RetryPolicy | None = None

    def __post_init__(self) -> None:
        if self.bytes_per_message < 0:
            raise ValueError("bytes_per_message must be non-negative")


@dataclass(frozen=True)
class TrafficSpec:
    """One scheduled outbound flow: a receiver plus how it is sent.

    ``count`` is the number of messages in the flow.  ``attempt`` and
    ``deadline`` carry retry state; freshly scheduled work starts at attempt 1
    with no deadline.  A spec becomes one delivery attempt when it is dispatched.
    """

    receiver: str
    count: int
    profile: SendingProfile = field(default_factory=SendingProfile)
    attempt: int = 1
    deadline: float | None = None

    def __post_init__(self) -> None:
        if not self.receiver:
            raise ValueError("receiver must be a non-empty node id")
        if self.count < 0:
            raise ValueError("traffic spec count must be non-negative")
        if self.attempt < 1:
            raise ValueError("attempt must be at least 1")

    @property
    def batch(self) -> Batch:
        return Batch(self.count, self.profile.bytes_per_message)

    def retried(self, rejected_count: int, *, deadline: float | None = None) -> "TrafficSpec":
        """Return the rejected remainder as the next attempt."""
        return replace(
            self,
            count=rejected_count,
            attempt=self.attempt + 1,
            deadline=self.deadline if deadline is None else deadline,
        )


@dataclass(frozen=True)
class Interaction:
    """A kind of work a node can receive, plus the work it causes.

    ``caused`` is usually built from :class:`CausedInteraction` instances.  A
    node registers an interaction together with its caused interactions; each
    registered interaction gets one long-lived task owned by the node.
    """

    name: str
    caused: tuple[CausedInteraction, ...] = ()

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("interaction name must be non-empty")
        object.__setattr__(self, "caused", tuple(self.caused))
        for caused in self.caused:
            if not isinstance(caused, CausedInteraction):
                raise TypeError("caused entries must be CausedInteraction instances")


@dataclass(frozen=True)
class CausedInteraction:
    """Work that handling an interaction adds to a caused interaction's task.

    ``delay`` is the simulated time between accepting the triggering work and
    the caused work becoming due.  ``messages_per_accepted`` scales the accepted
    message count into the amount of caused work.  ``receivers`` lists the node
    ids that receive one flow each; ``profile`` controls payload size and retry.
    """

    interaction: Interaction
    delay: float = 0.0
    messages_per_accepted: int = 1
    receivers: tuple[str, ...] = ()
    profile: SendingProfile = field(default_factory=SendingProfile)

    def __post_init__(self) -> None:
        if not isinstance(self.interaction, Interaction):
            raise TypeError("interaction must be an Interaction instance")
        if self.delay < 0:
            raise ValueError("caused-work delay must be non-negative")
        if self.messages_per_accepted < 0:
            raise ValueError("messages_per_accepted must be non-negative")
        object.__setattr__(self, "receivers", tuple(self.receivers))
        if any(not receiver for receiver in self.receivers):
            raise ValueError("receiver ids must be non-empty")

    def amount(self, accepted: Batch) -> int:
        """Number of caused messages produced by an accepted batch."""
        return accepted.count * self.messages_per_accepted
