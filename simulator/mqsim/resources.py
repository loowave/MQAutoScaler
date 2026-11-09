"""Per-instance resource settings and virtual usage.

The framework only stores and exposes these numbers.  Concrete admission and
usage formulas belong to node implementations.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResourceSettings:
    """Configured capacity of one node, expressed per instance."""

    instances: int = 1
    cpu_per_instance: float = 1.0
    ram_per_instance: float = 1.0
    disk_per_instance: float = 0.0
    network_per_instance: float = 0.0

    def __post_init__(self) -> None:
        if self.instances < 1:
            raise ValueError("instances must be at least 1")
        for name in ("cpu", "ram", "disk", "network"):
            if getattr(self, f"{name}_per_instance") < 0:
                raise ValueError(f"{name}_per_instance must be non-negative")

    def total(self, resource: str) -> float:
        """Configured total capacity for ``cpu``, ``ram``, ``disk`` or ``network``."""
        return float(getattr(self, f"{resource}_per_instance")) * self.instances


@dataclass
class ResourceUsage:
    """Current virtual usage of one node, expressed per instance.

    ``messages`` and ``bytes`` are activity counters that are useful for
    observations even though they are not capacity dimensions.
    """

    cpu_per_instance: float = 0.0
    ram_per_instance: float = 0.0
    disk_per_instance: float = 0.0
    network_per_instance: float = 0.0
    messages: int = 0
    bytes: int = 0

    def copy(self) -> "ResourceUsage":
        return ResourceUsage(
            cpu_per_instance=self.cpu_per_instance,
            ram_per_instance=self.ram_per_instance,
            disk_per_instance=self.disk_per_instance,
            network_per_instance=self.network_per_instance,
            messages=self.messages,
            bytes=self.bytes,
        )

    def utilization(self, settings: ResourceSettings) -> dict[str, float]:
        """Return used/capacity ratios; infinite when capacity is zero and used."""
        ratios: dict[str, float] = {}
        for resource in ("cpu", "ram", "disk", "network"):
            used = float(getattr(self, f"{resource}_per_instance"))
            capacity = float(getattr(settings, f"{resource}_per_instance"))
            if capacity > 0:
                ratios[resource] = used / capacity
            else:
                ratios[resource] = 0.0 if used == 0 else float("inf")
        return ratios

    def max_utilization(self, settings: ResourceSettings) -> float:
        return max(self.utilization(settings).values(), default=0.0)
