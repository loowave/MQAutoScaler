"""The node base class: owned tasks, admission, retries and observations.

A concrete node supplies its own resource formulas by overriding
:meth:`Node.capacity_for` and :meth:`Node.on_accept` (and optionally the
dispatch/result hooks).  The base class implements the framework contract:
tick preparation, due-work dispatch, caused-work registration and fixed-delay
retry of rejected remainders.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from .delivery import AdmissionResult, Delivery, DeliveryAttempt
from .model import Batch, CausedInteraction, Interaction, TrafficSpec
from .observations import NodeObservation, ObservationStore
from .proxy import Proxy
from .resources import ResourceSettings, ResourceUsage
from .tasks import Task, TaskEntry
from .timeutil import TIME_EPSILON


class Node:
    """A simulated component that owns tasks, resource state and a policy.

    The interaction definitions passed at construction create one long-lived
    :class:`~mqsim.tasks.Task` per interaction, including interactions that are
    only reachable through ``caused`` links.  Those tasks persist for the life
    of the node; ``receive()`` adds to them rather than replacing them.
    """

    def __init__(
        self,
        node_id: str,
        interactions: Iterable[Interaction] = (),
        *,
        settings: ResourceSettings | None = None,
        observation_retention: float = 60.0,
        observation_window: float = 1.0,
    ) -> None:
        if not node_id:
            raise ValueError("node id must be non-empty")
        self.id = node_id
        self.settings = settings if settings is not None else ResourceSettings()
        self.usage = ResourceUsage()
        self.observations = ObservationStore(
            retention=observation_retention, window=observation_window
        )
        self.interactions: dict[str, Interaction] = {}
        for interaction in interactions:
            self._register(interaction)
        self.tasks: dict[str, Task] = {
            name: Task(name) for name in self.interactions
        }
        # Optional per-node routing derived from the service graph.  When a
        # caused interaction has no route here, its declared receivers are used.
        self.routes: dict[str, tuple[str, ...]] = {}

    # -- construction --------------------------------------------------

    def _register(self, interaction: Interaction) -> Interaction:
        """Register an interaction and its caused interactions, depth first."""
        if not isinstance(interaction, Interaction):
            raise TypeError("node interactions must be Interaction instances")
        existing = self.interactions.get(interaction.name)
        if existing is not None:
            return existing
        self.interactions[interaction.name] = interaction
        for caused in interaction.caused:
            self._register(caused.interaction)
        return interaction

    def task(self, interaction: str) -> Task:
        return self.tasks[interaction]

    def schedule(
        self, interaction: str, due: float, specs: Iterable[TrafficSpec]
    ) -> TaskEntry:
        """Seed work on one of this node's tasks (also useful for external traffic)."""
        task = self.tasks[interaction]
        normalized = []
        for spec in specs:
            deadline = spec.deadline
            retry = spec.profile.retry
            if deadline is None and retry is not None and retry.expires_after is not None:
                deadline = due + retry.expires_after
            normalized.append(replace(spec, deadline=deadline))
        return task.add_work(due, normalized)

    # -- tick protocol -------------------------------------------------

    def start_tick(self, now: float, proxy: Proxy) -> None:
        """Prepare local work and submit this tick's outgoing attempts.

        Returning from this method is the readiness acknowledgment: the manager
        waits for every node before asking the proxy to deliver.
        """
        self.prepare_tick(now)
        for task in self.tasks.values():
            while True:
                entry = task.pop_due(now)
                if entry is None:
                    break
                for spec in self.prepare_dispatch(task, entry, now):
                    if spec.count <= 0:
                        continue
                    proxy.submit(
                        DeliveryAttempt(
                            sender=self.id,
                            receiver=spec.receiver,
                            interaction=task.name,
                            batch=Batch(spec.count, spec.profile.bytes_per_message),
                            task=task.name,
                            profile=spec.profile,
                            attempt_number=spec.attempt,
                            deadline=spec.deadline,
                        )
                    )
                    self.observations.record_metric(now, "sent_messages", spec.count)

    def receive(self, delivery: Delivery, now: float) -> int:
        """Accept a prefix of the delivered batch and add caused work.

        Returns the accepted prefix length.  The accepted portion updates the
        node's state and scales the caused-interaction work; rejected messages
        are not used to create caused work.
        """
        requested = delivery.batch.count
        capacity = int(self.capacity_for(delivery, now))
        accepted = max(0, min(capacity, requested))
        accepted_batch = Batch(accepted, delivery.batch.bytes_per_message)

        if accepted > 0:
            self.on_accept(delivery, accepted_batch, now)
            self._add_caused_work(delivery.interaction, accepted_batch, now)

        self.observations.record_metric(now, "accepted_messages", accepted)
        if accepted < requested:
            self.observations.record_metric(now, "rejected_messages", requested - accepted)
        self.observations.log(
            now,
            "received",
            interaction=delivery.interaction,
            sender=delivery.sender,
            requested=requested,
            accepted=accepted,
        )
        return accepted

    def handle_admission_result(self, result: AdmissionResult, now: float) -> None:
        """Update the sender's task from one admission result.

        Only the rejected remainder is requeued for retry; the accepted portion
        is considered handled by the receiver.
        """
        attempt = result.attempt
        task = self.tasks[attempt.task]

        self.observations.record_metric(now, "confirmed_messages", result.accepted)
        self.on_result(task, result, now)

        if result.rejected == 0:
            return

        retry = attempt.profile.retry
        can_retry = retry is not None and attempt.attempt_number < retry.max_attempts
        if can_retry and attempt.deadline is not None:
            can_retry = now + retry.delay <= attempt.deadline + TIME_EPSILON

        if can_retry:
            assert retry is not None
            task.requeue(
                now + retry.delay,
                [
                    TrafficSpec(
                        receiver=attempt.receiver,
                        count=result.rejected,
                        profile=attempt.profile,
                        attempt=attempt.attempt_number + 1,
                        deadline=attempt.deadline,
                    )
                ],
            )
            self.observations.record_metric(now, "retried_messages", result.rejected)
            self.observations.log(
                now,
                "retry_scheduled",
                interaction=attempt.interaction,
                receiver=attempt.receiver,
                rejected=result.rejected,
                next_attempt=attempt.attempt_number + 1,
                due=now + retry.delay,
            )
        else:
            self.observations.record_metric(now, "dropped_messages", result.rejected)
            self.observations.log(
                now,
                "work_dropped",
                level="warning",
                interaction=attempt.interaction,
                receiver=attempt.receiver,
                rejected=result.rejected,
                attempts=attempt.attempt_number,
            )

    # -- overridable policy hooks -------------------------------------

    def capacity_for(self, delivery: Delivery, now: float) -> int:
        """How many messages of ``delivery`` this node accepts.

        The base implementation accepts everything.  Concrete nodes override
        this with their own resource or backlog formula.
        """
        return delivery.batch.count

    def on_accept(self, delivery: Delivery, accepted: Batch, now: float) -> None:
        """Update node state for accepted work; the base tracks activity counters."""
        self.usage.messages += accepted.count
        self.usage.bytes += accepted.total_bytes

    def prepare_tick(self, now: float) -> None:
        """Hook for local work that happens before this tick's dispatch."""

    def prepare_dispatch(
        self, task: Task, entry: TaskEntry, now: float
    ) -> Iterable[TrafficSpec]:
        """Hook to adapt an entry's specs using the task's shared state."""
        return entry.specs

    def on_result(
        self, task: Task, result: AdmissionResult, now: float
    ) -> None:
        """Hook for shared task state after an admission result arrives."""

    # -- caused work ---------------------------------------------------

    def _add_caused_work(self, interaction_name: str, accepted: Batch, now: float) -> None:
        interaction = self.interactions.get(interaction_name)
        if interaction is None:
            raise KeyError(
                f"node {self.id!r} received undeclared interaction {interaction_name!r}"
            )
        for caused in interaction.caused:
            count = caused.amount(accepted)
            receivers = self.receivers_for(caused)
            if count <= 0 or not receivers:
                continue
            due = now + caused.delay
            specs = [
                TrafficSpec(
                    receiver=receiver,
                    count=count,
                    profile=caused.profile,
                    deadline=self._deadline(due, caused),
                )
                for receiver in receivers
            ]
            task = self.tasks[caused.interaction.name]
            task.add_work(due, specs)
            self.observations.record_metric(now, "caused_messages", count)
            self.observations.log(
                now,
                "caused_work_added",
                caused_interaction=caused.interaction.name,
                messages=count,
                due=due,
            )

    @staticmethod
    def _deadline(due: float, caused: CausedInteraction) -> float | None:
        retry = caused.profile.retry
        if retry is None or retry.expires_after is None:
            return None
        return due + retry.expires_after

    def receivers_for(self, caused: CausedInteraction) -> tuple[str, ...]:
        """Resolve the receivers of one caused interaction.

        Graph-built nodes install ``routes`` from their outgoing edges; manually
        built nodes fall back to the receivers declared on the interaction.
        """
        routed = self.routes.get(caused.interaction.name)
        return routed if routed is not None else caused.receivers

    # -- read interface ------------------------------------------------

    def observe(self) -> NodeObservation:
        """Recent metrics, structured logs and virtual resource usage."""
        return self.observations.snapshot(self.id, self.settings)

    def recent_metrics(self) -> dict[str, float]:
        return self.observations.latest_metrics()

    def recent_logs(self):
        return self.observations.logs()

    def resource_usage(self) -> ResourceUsage:
        return self.usage.copy()
