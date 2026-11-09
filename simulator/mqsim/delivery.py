"""Delivery-side value types kept distinct from scheduled work.

* :class:`DeliveryAttempt` -- what a sender hands to the proxy.
* :class:`Delivery` -- what a receiver's ``receive()`` sees for one tick.
* :class:`AdmissionResult` -- what the proxy relays back to the sender.
"""

from __future__ import annotations

from dataclasses import dataclass

from .model import Batch, SendingProfile


@dataclass(frozen=True)
class DeliveryAttempt:
    """One outbound batch submitted by a sender for the current tick."""

    sender: str
    receiver: str
    interaction: str
    batch: Batch
    task: str
    profile: SendingProfile
    attempt_number: int = 1
    deadline: float | None = None
    seq: int = -1

    def __post_init__(self) -> None:
        if self.batch.count < 1:
            raise ValueError("delivery attempts carry at least one message")
        if self.attempt_number < 1:
            raise ValueError("attempt_number must be at least 1")


@dataclass(frozen=True)
class Delivery:
    """The receiver-facing view of one delivery: which interaction to handle."""

    sender: str
    receiver: str
    interaction: str
    batch: Batch


@dataclass(frozen=True)
class AdmissionResult:
    """The receiver's admission decision for one attempt.

    ``accepted`` is a prefix length: the receiver takes the first ``accepted``
    messages and rejects the rest.
    """

    attempt: DeliveryAttempt
    accepted: int

    def __post_init__(self) -> None:
        if not 0 <= self.accepted <= self.attempt.batch.count:
            raise ValueError("accepted must be a prefix length within the attempt batch")

    @property
    def requested(self) -> int:
        return self.attempt.batch.count

    @property
    def rejected(self) -> int:
        return self.requested - self.accepted

    @property
    def accepted_batch(self) -> Batch:
        return Batch(self.accepted, self.attempt.batch.bytes_per_message)

    @property
    def rejected_batch(self) -> Batch:
        return Batch(self.rejected, self.attempt.batch.bytes_per_message)
