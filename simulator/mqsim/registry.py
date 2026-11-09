"""Registry of node types usable from YAML configuration.

A node type is a factory callable accepting the standard node keyword arguments
(``node_id``, ``interactions``, ``settings``, ``observation_retention``,
``observation_window``) plus any ``params`` from the node's YAML section.
"""

from __future__ import annotations

from typing import Callable, Mapping

from .node import Node

NodeFactory = Callable[..., Node]

_NODE_TYPES: dict[str, NodeFactory] = {}


def register_node_type(
    name: str, factory: NodeFactory | None = None
) -> NodeFactory | Callable[[NodeFactory], NodeFactory]:
    """Register (or decorate) a node factory under ``name``."""

    def decorator(candidate: NodeFactory) -> NodeFactory:
        if name in _NODE_TYPES:
            raise ValueError(f"node type {name!r} is already registered")
        _NODE_TYPES[name] = candidate
        return candidate

    if factory is not None:
        return decorator(factory)
    return decorator


def registered_node_types() -> Mapping[str, NodeFactory]:
    return dict(_NODE_TYPES)


def get_node_type(name: str) -> NodeFactory:
    try:
        return _NODE_TYPES[name]
    except KeyError:
        known = ", ".join(sorted(_NODE_TYPES)) or "<none>"
        raise KeyError(f"unknown node type {name!r}; registered types: {known}") from None
