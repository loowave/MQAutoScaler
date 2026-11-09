"""Time helpers for the fixed-step simulation clock.

Simulated time is expressed in seconds as ``float``.  Tick sizes are expected
to be at least a millisecond, so a nanosecond tolerance is far below one tick
and only absorbs floating-point representation error.
"""

from __future__ import annotations

TIME_EPSILON = 1e-9


def is_due(due: float, now: float) -> bool:
    """Return ``True`` when scheduled work at ``due`` is eligible at ``now``."""
    return due <= now + TIME_EPSILON


def same_time(left: float, right: float) -> bool:
    """Return ``True`` when two simulated timestamps refer to the same instant."""
    return abs(left - right) <= TIME_EPSILON
