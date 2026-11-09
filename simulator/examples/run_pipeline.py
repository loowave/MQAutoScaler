"""Run the client -> backend -> Kafka -> consumer -> Postgres pipeline.

Besides printing per-node totals, this writes one matplotlib figure per node
with every metric the node records over simulated time.
"""

from __future__ import annotations

from pathlib import Path

from metric_plots import write_plots
from mqsim import load_experiment

COUNTERS = (
    "generated_messages",
    "accepted_messages",
    "rejected_messages",
    "dropped_messages",
    "retried_messages",
    "drained_messages",
    "expired_messages",
    "caused_messages",
    "sent_messages",
)


def main() -> None:
    config_dir = Path(__file__).parent / "pipeline"
    experiment = load_experiment(*sorted(config_dir.glob("*.yaml")))
    experiment.run(30)

    print(f"now={experiment.manager.now}")
    for node_id, node in sorted(experiment.nodes.items()):
        totals = {
            name: sum(
                sample.value for sample in node.observations.metric_samples(name)
            )
            for name in COUNTERS
        }
        parts = [f"{name}={value:.0f}" for name, value in totals.items() if value]
        if node.observations.metric_samples("backlog_messages"):
            parts.append(f"backlog={node.recent_metrics()['backlog_messages']:.0f}")
        print(f"{node_id}: {', '.join(parts)}")

    kafka = experiment.node("kafka")
    print(
        "kafka backlog per second:",
        [
            (window.start, window.last)
            for window in kafka.observations.metric_windows("backlog_messages")
        ][-10:],
    )

    output_dir = Path(__file__).parent / "plots/pipeline"
    paths = write_plots(experiment.manager, output_dir)
    print(f"wrote {len(paths)} metric plots:")
    for path in paths:
        print(f"  {path}")


if __name__ == "__main__":
    main()
