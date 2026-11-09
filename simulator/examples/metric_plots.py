"""Shared matplotlib helpers for experiment metric and resource plots."""

from __future__ import annotations

import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402  (backend must be selected first)
from matplotlib.ticker import PercentFormatter  # noqa: E402

from mqsim import ExperimentManager

# Metrics that describe a level at one instant.  Everything else is treated as
# a counter, so repeated samples at the same timestamp are summed per tick.
GAUGE_METRICS = {"backlog_messages"}

RESOURCES = ("cpu", "ram", "disk", "network")


def per_tick_values(
    name: str, samples, timeline=()
) -> tuple[list[float], list[float]]:
    """Collapse same-timestamp samples into one value per tick.

    Every timestamp in ``timeline`` is present in the result, even when the
    metric recorded nothing there; missing ticks are filled with zero.
    """
    values: dict[float, list[float]] = {}
    for sample in samples:
        values.setdefault(sample.timestamp, []).append(sample.value)
    times = sorted(set(timeline) | set(values))
    if name in GAUGE_METRICS:
        return times, [
            values[timestamp][-1] if timestamp in values else 0.0
            for timestamp in times
        ]
    return times, [
        sum(values[timestamp]) if timestamp in values else 0.0
        for timestamp in times
    ]


def node_panels(manager: ExperimentManager, node_id: str) -> list[tuple]:
    """Return ``(title, times, values)`` panels for one node."""
    panels: list[tuple[str, list[float], list[float]]] = []
    timeline = manager.tick_times
    metrics = manager.metrics.get(node_id, {})
    for name in sorted(metrics):
        times, values = per_tick_values(name, metrics[name], timeline)
        panels.append((name, times, values))

    samples = manager.usage_samples(node_id)
    if samples:
        settings = manager.node(node_id).settings
        usage_by_time = {sample.timestamp: sample.usage for sample in samples}
        times = sorted(set(timeline) | set(usage_by_time))
        for resource in RESOURCES:
            if getattr(settings, f"{resource}_per_instance") <= 0:
                continue
            values = [
                usage_by_time[timestamp].utilization(settings)[resource]
                if timestamp in usage_by_time
                else 0.0
                for timestamp in times
            ]
            if any(value != 0 for value in values):
                panels.append((f"utilization_{resource}", times, values))
    return panels


def write_plots(manager: ExperimentManager, output_dir: Path) -> list[Path]:
    """Write one PNG per node, with one subplot per metric or resource."""
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for node_id in sorted(manager.metrics):
        panels = node_panels(manager, node_id)
        if not panels:
            continue
        columns = min(3, len(panels))
        rows = math.ceil(len(panels) / columns)
        figure, axes = plt.subplots(
            rows, columns, figsize=(4.5 * columns, 3.0 * rows), squeeze=False
        )
        for axis, (title, times, values) in zip(axes.flat, panels):
            axis.step(times, values, where="post", marker="o", markersize=3)
            axis.set_title(title)
            axis.set_xlabel("simulated time (s)")
            axis.grid(True, alpha=0.3)
            if title.startswith("utilization_"):
                axis.yaxis.set_major_formatter(PercentFormatter(1.0))
                axis.set_ylim(bottom=0)
        for axis in list(axes.flat)[len(panels):]:
            axis.set_visible(False)
        figure.suptitle(f"{node_id} metrics and resources")
        figure.tight_layout()

        path = output_dir / f"{node_id}.png"
        figure.savefig(path, dpi=120)
        plt.close(figure)
        written.append(path)
    return written
