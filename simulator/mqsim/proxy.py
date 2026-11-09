"""Tick-scoped delivery: order attempts deterministically and relay results."""

from __future__ import annotations

from dataclasses import replace
from typing import Mapping

from .delivery import AdmissionResult, Delivery, DeliveryAttempt


class Proxy:
    """Collects one tick's outgoing attempts and delivers them to receivers.

    The proxy owns no state beyond the current tick.  Attempts are grouped by
    receiver and ordered by ``(sender, task, submission sequence)``, which makes
    delivery reproducible regardless of the order nodes were registered.
    Admission decisions for the whole tick are collected before any result is
    relayed to a sender.
    """

    def __init__(self) -> None:
        self._pending: list[DeliveryAttempt] = []
        self._next_seq = 0

    @property
    def pending(self) -> tuple[DeliveryAttempt, ...]:
        return tuple(self._pending)

    def submit(self, attempt: DeliveryAttempt) -> DeliveryAttempt:
        """Accept an attempt for the current tick and stamp its sequence number."""
        if attempt.seq != -1:
            raise ValueError("attempt has already been submitted")
        stamped = replace(attempt, seq=self._next_seq)
        self._next_seq += 1
        self._pending.append(stamped)
        return stamped

    def deliver(self, nodes: Mapping[str, "Node"], now: float) -> list[AdmissionResult]:  # noqa: F821
        """Call every receiver, then relay all admission results to senders."""
        pending, self._pending = self._pending, []

        by_receiver: dict[str, list[DeliveryAttempt]] = {}
        for attempt in pending:
            if attempt.receiver not in nodes:
                raise KeyError(f"unknown receiver node {attempt.receiver!r}")
            by_receiver.setdefault(attempt.receiver, []).append(attempt)

        results: list[AdmissionResult] = []
        for receiver_id in sorted(by_receiver):
            receiver = nodes[receiver_id]
            attempts = sorted(
                by_receiver[receiver_id],
                key=lambda attempt: (attempt.sender, attempt.task, attempt.seq),
            )
            for attempt in attempts:
                delivery = Delivery(
                    sender=attempt.sender,
                    receiver=attempt.receiver,
                    interaction=attempt.interaction,
                    batch=attempt.batch,
                )
                accepted = receiver.receive(delivery, now)
                results.append(AdmissionResult(attempt=attempt, accepted=accepted))

        for result in results:
            sender = nodes.get(result.attempt.sender)
            if sender is None:
                raise KeyError(f"unknown sender node {result.attempt.sender!r}")
            sender.handle_admission_result(result, now)

        return results
