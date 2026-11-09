"""YAML configuration for managers, nodes, interactions, graphs and producers.

Configuration can be split across several files.  Files are merged section by
section: ``manager`` and ``interactions`` are merged by key, ``nodes`` is merged
by node id, and ``edges`` / ``producers`` are concatenated.  This lets a project
keep topology, interactions and workload in separate YAML files if it prefers.

Schema (all sections optional unless stated)::

    manager:
      tick: 0.5
      start: 0.0

    interactions:
      ingress_request:
        caused:
          - interaction: backend_request
            delay: 0.1
            messages_per_accepted: 1
            profile:
              bytes_per_message: 256
              retry:
                delay: 1.0
                max_attempts: 3
                expires_after: 10.0
      backend_request: {}
      backend_reply: {}

    nodes:
      gateway:
        type: dummy
        receives: [ingress_request]        # optional; inferred from edges/producers
        resources:
          instances: 1
          cpu_per_instance: 1
          ram_per_instance: 1
        observation:
          retention: 60.0
          window: 1.0
        params:
          capacity_per_tick: 40

    edges:
      - from: gateway
        to: worker
        interaction: backend_request
      - from: worker
        to: gateway
        interaction: backend_reply

    producers:
      - id: client
        rate: 20.0                          # messages per simulated second
        batch_size: 5
        bytes_per_message: 256
        start: 0.0
        stop: 30.0
        targets:
          - node: gateway
            interaction: ingress_request

Caused-interaction receivers are **not** configured on interactions: they are
resolved from outgoing edges of the same interaction type on each node.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from numbers import Real
from pathlib import Path
from typing import Any, Mapping

import yaml

from . import nodes as _builtin_nodes  # noqa: F401  (registers built-in node types)
from .manager import ExperimentManager
from .model import (
    CausedInteraction,
    Interaction,
    RetryPolicy,
    SendingProfile,
)
from .node import Node
from .observations import NodeObservation
from .producer import ProducerTarget, TrafficProducer
from .registry import NodeFactory, registered_node_types
from .resources import ResourceSettings


class ConfigError(ValueError):
    """Raised when a YAML experiment configuration is invalid."""


_TOP_LEVEL = {"manager", "interactions", "nodes", "edges", "producers"}
_RESERVED_PARAMS = {
    "node_id",
    "interactions",
    "settings",
    "observation_retention",
    "observation_window",
}
# Referenced explicitly so the built-in node classes stay imported (registration
# happens as a module side effect) and linters see the dependency.
_BUILTIN_NODE_TYPES = (_builtin_nodes.CappedNode, _builtin_nodes.QueueNode)


@dataclass(frozen=True)
class ManagerConfig:
    tick: float = 1.0
    start: float = 0.0


@dataclass(frozen=True)
class NodeConfig:
    id: str
    type: str = "dummy"
    resources: ResourceSettings = field(default_factory=ResourceSettings)
    observation_retention: float = 60.0
    observation_window: float = 1.0
    params: Mapping[str, Any] = field(default_factory=dict)
    receives: tuple[str, ...] = ()


@dataclass(frozen=True)
class EdgeConfig:
    from_node: str
    to_node: str
    interaction: str
    id: str | None = None


@dataclass(frozen=True)
class ProducerConfig:
    id: str
    targets: tuple[ProducerTarget, ...]
    rate: float
    batch_size: int = 1
    bytes_per_message: int = 64
    start: float | None = None
    stop: float | None = None
    retry: RetryPolicy | None = None
    observation_retention: float = 60.0
    observation_window: float = 1.0


@dataclass(frozen=True)
class ExperimentConfig:
    manager: ManagerConfig = field(default_factory=ManagerConfig)
    interactions: Mapping[str, Interaction] = field(default_factory=dict)
    nodes: Mapping[str, NodeConfig] = field(default_factory=dict)
    edges: tuple[EdgeConfig, ...] = ()
    producers: tuple[ProducerConfig, ...] = ()


@dataclass(frozen=True)
class BuiltExperiment:
    """A configured experiment ready to run."""

    manager: ExperimentManager
    nodes: Mapping[str, Node]
    producers: Mapping[str, TrafficProducer]
    config: ExperimentConfig

    def node(self, node_id: str) -> Node:
        return self.nodes[node_id]

    def producer(self, producer_id: str) -> TrafficProducer:
        return self.producers[producer_id]

    def run(self, ticks: int) -> None:
        self.manager.run(ticks)

    def run_until(self, until: float) -> None:
        self.manager.run_until(until)

    def observe(self) -> dict[str, NodeObservation]:
        return self.manager.observe()


# ---------------------------------------------------------------------------
# Loading and validation
# ---------------------------------------------------------------------------


def load_config(*paths: str | Path) -> ExperimentConfig:
    """Load and merge one or more YAML files into a validated configuration."""
    if not paths:
        raise ConfigError("at least one YAML path is required")

    manager_raw: dict[str, Any] = {}
    interactions_raw: dict[str, Any] = {}
    nodes_raw: dict[str, NodeConfig] = {}
    edges_raw: list[Any] = []
    producers_raw: list[Any] = []

    for path in paths:
        source = Path(path)
        try:
            text = source.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigError(f"cannot read {source}: {exc}") from exc
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ConfigError(f"{source}: invalid YAML: {exc}") from exc
        if data is None:
            continue
        data = _mapping(data, str(source))
        _only_keys(data, _TOP_LEVEL, str(source))

        if "manager" in data:
            manager_raw.update(_mapping(data["manager"], f"{source}: manager"))
        if "interactions" in data:
            interactions_raw.update(
                _mapping(data["interactions"], f"{source}: interactions")
            )
        if "nodes" in data:
            for node_id, node in _parse_nodes_section(
                data["nodes"], f"{source}: nodes"
            ).items():
                if node_id in nodes_raw:
                    raise ConfigError(f"duplicate node id {node_id!r}")
                nodes_raw[node_id] = node
        if "edges" in data:
            edges_raw.extend(_list(data["edges"], f"{source}: edges"))
        if "producers" in data:
            producers_raw.extend(_list(data["producers"], f"{source}: producers"))

    config = ExperimentConfig(
        manager=_parse_manager(manager_raw),
        interactions=_parse_interactions(interactions_raw),
        nodes=nodes_raw,
        edges=_parse_edges(edges_raw),
        producers=_parse_producers(producers_raw),
    )
    _validate(config)
    return config


def load_experiment(
    *paths: str | Path, node_types: Mapping[str, NodeFactory] | None = None
) -> BuiltExperiment:
    """Load YAML and build a runnable experiment in one step."""
    return build_experiment(load_config(*paths), node_types=node_types)


def build_experiment(
    config: ExperimentConfig,
    *,
    node_types: Mapping[str, NodeFactory] | None = None,
) -> BuiltExperiment:
    """Instantiate the manager, nodes and producers described by ``config``."""
    _validate(config)
    factories: dict[str, NodeFactory] = registered_node_types()
    if node_types:
        factories.update(node_types)

    incoming: dict[str, set[str]] = {node_id: set() for node_id in config.nodes}
    for edge in config.edges:
        incoming[edge.to_node].add(edge.interaction)
    for producer in config.producers:
        for target in producer.targets:
            incoming[target.node].add(target.interaction)

    nodes: dict[str, Node] = {}
    for node_id in sorted(config.nodes):
        node_config = config.nodes[node_id]
        receivable = sorted(incoming[node_id] | set(node_config.receives))
        interactions = [_lookup_interaction(config, name) for name in receivable]
        factory = factories.get(node_config.type)
        if factory is None:
            known = ", ".join(sorted(factories)) or "<none>"
            raise ConfigError(
                f"nodes.{node_id}: unknown node type {node_config.type!r}; "
                f"registered types: {known}"
            )
        try:
            node = factory(
                node_id=node_id,
                interactions=interactions,
                settings=node_config.resources,
                observation_retention=node_config.observation_retention,
                observation_window=node_config.observation_window,
                **node_config.params,
            )
        except TypeError as exc:
            raise ConfigError(f"nodes.{node_id}: cannot build node: {exc}") from exc
        except ValueError as exc:
            raise ConfigError(f"nodes.{node_id}: cannot build node: {exc}") from exc
        if not isinstance(node, Node):
            raise ConfigError(
                f"nodes.{node_id}: node type {node_config.type!r} did not return a Node"
            )
        if node.id != node_id:
            raise ConfigError(
                f"nodes.{node_id}: factory returned node with id {node.id!r}"
            )
        nodes[node_id] = node

    # Edge-derived routing: outgoing edges of an interaction type determine the
    # receivers of work caused by that interaction at this node.
    for node_id, node in nodes.items():
        grouped: dict[str, list[str]] = {}
        for edge in config.edges:
            if edge.from_node == node_id:
                grouped.setdefault(edge.interaction, []).append(edge.to_node)
        node.routes = {
            interaction: tuple(dict.fromkeys(receivers))
            for interaction, receivers in grouped.items()
        }

    manager = ExperimentManager(
        tick=config.manager.tick, start=config.manager.start
    )
    for node_id in sorted(nodes):
        manager.add_node(nodes[node_id])

    producers: dict[str, TrafficProducer] = {}
    for producer_config in sorted(config.producers, key=lambda item: item.id):
        start = (
            config.manager.start
            if producer_config.start is None
            else max(producer_config.start, config.manager.start)
        )
        if producer_config.stop is not None and producer_config.stop < start:
            raise ConfigError(
                f"producers.{producer_config.id}: stop is before the simulation start"
            )
        try:
            producer = TrafficProducer(
                node_id=producer_config.id,
                targets=producer_config.targets,
                rate=producer_config.rate,
                batch_size=producer_config.batch_size,
                bytes_per_message=producer_config.bytes_per_message,
                start=start,
                stop=producer_config.stop,
                retry=producer_config.retry,
                observation_retention=producer_config.observation_retention,
                observation_window=producer_config.observation_window,
            )
        except ValueError as exc:
            raise ConfigError(
                f"producers.{producer_config.id}: cannot build producer: {exc}"
            ) from exc
        manager.add_node(producer)
        producers[producer.id] = producer

    return BuiltExperiment(
        manager=manager, nodes=nodes, producers=producers, config=config
    )


def _validate(config: ExperimentConfig) -> None:
    for edge in config.edges:
        where = f"edge {edge.id!r}" if edge.id else f"edge {edge.from_node}->{edge.to_node}"
        if edge.from_node not in config.nodes:
            raise ConfigError(f"{where}: unknown source node {edge.from_node!r}")
        if edge.to_node not in config.nodes:
            raise ConfigError(f"{where}: unknown destination node {edge.to_node!r}")
        if edge.interaction not in config.interactions:
            raise ConfigError(
                f"{where}: unknown interaction {edge.interaction!r}"
            )

    for node_id, node_config in config.nodes.items():
        for interaction in node_config.receives:
            if interaction not in config.interactions:
                raise ConfigError(
                    f"nodes.{node_id}: unknown interaction {interaction!r}"
                )

    seen_producers: set[str] = set()
    for producer in config.producers:
        where = f"producer {producer.id!r}"
        if producer.id in config.nodes:
            raise ConfigError(f"{where}: id collides with a node id")
        if producer.id in seen_producers:
            raise ConfigError(f"{where}: duplicate producer id")
        seen_producers.add(producer.id)
        for target in producer.targets:
            if target.node not in config.nodes:
                raise ConfigError(f"{where}: unknown target node {target.node!r}")
            if target.interaction not in config.interactions:
                raise ConfigError(
                    f"{where}: unknown interaction {target.interaction!r}"
                )


# ---------------------------------------------------------------------------
# Section parsers
# ---------------------------------------------------------------------------


def _parse_manager(raw: Mapping[str, Any]) -> ManagerConfig:
    entry = _mapping(raw, "manager")
    _only_keys(entry, {"tick", "start"}, "manager")
    tick = _number(entry.get("tick", 1.0), "manager.tick")
    if tick <= 0:
        raise ConfigError("manager.tick must be positive")
    start = _number(entry.get("start", 0.0), "manager.start")
    return ManagerConfig(tick=tick, start=start)


def _parse_nodes_section(raw: Any, where: str) -> dict[str, NodeConfig]:
    entries: list[tuple[str, Any]] = []
    if isinstance(raw, Mapping):
        for node_id, spec in raw.items():
            entries.append((_string(node_id, f"{where}: node id"), spec))
    else:
        for index, item in enumerate(_list(raw, where)):
            entry_where = f"{where}[{index}]"
            spec = _mapping(item, entry_where)
            if "id" not in spec:
                raise ConfigError(f"{entry_where}: node entries need an id")
            node_id = _string(spec["id"], f"{entry_where}.id")
            entries.append(
                (node_id, {key: value for key, value in spec.items() if key != "id"})
            )

    parsed: dict[str, NodeConfig] = {}
    for node_id, spec in entries:
        if node_id in parsed:
            raise ConfigError(f"{where}: duplicate node id {node_id!r}")
        parsed[node_id] = _parse_node(spec, f"nodes.{node_id}")
    return parsed


def _parse_node(raw: Any, where: str) -> NodeConfig:
    entry = _mapping(raw if raw is not None else {}, where)
    _only_keys(entry, {"type", "resources", "observation", "params", "receives"}, where)
    node_type = _string(entry.get("type", "dummy"), f"{where}.type")
    retention, window = _parse_observation(entry.get("observation"), f"{where}.observation")
    params = dict(_mapping(entry.get("params", {}), f"{where}.params"))
    reserved = sorted(_RESERVED_PARAMS & set(params))
    if reserved:
        raise ConfigError(
            f"{where}.params: reserved key(s) cannot be overridden: {', '.join(reserved)}"
        )
    receives = tuple(
        dict.fromkeys(
            _string(item, f"{where}.receives")
            for item in _list(entry.get("receives", []), f"{where}.receives")
        )
    )
    return NodeConfig(
        id=where.removeprefix("nodes."),
        type=node_type,
        resources=_parse_resources(entry.get("resources"), f"{where}.resources"),
        observation_retention=retention,
        observation_window=window,
        params=params,
        receives=receives,
    )


def _parse_resources(raw: Any, where: str) -> ResourceSettings:
    entry = _mapping(raw if raw is not None else {}, where)
    allowed = {
        "instances",
        "cpu_per_instance",
        "ram_per_instance",
        "disk_per_instance",
        "network_per_instance",
    }
    _only_keys(entry, allowed, where)
    values: dict[str, Any] = {}
    if "instances" in entry:
        values["instances"] = _integer(entry["instances"], f"{where}.instances", minimum=1)
    for key in allowed - {"instances"}:
        if key in entry:
            values[key] = _number(entry[key], f"{where}.{key}", minimum=0)
    return ResourceSettings(**values)


def _parse_observation(raw: Any, where: str) -> tuple[float, float]:
    entry = _mapping(raw if raw is not None else {}, where)
    _only_keys(entry, {"retention", "window"}, where)
    retention = _number(entry.get("retention", 60.0), f"{where}.retention")
    window = _number(entry.get("window", 1.0), f"{where}.window")
    if retention <= 0:
        raise ConfigError(f"{where}.retention must be positive")
    if window <= 0:
        raise ConfigError(f"{where}.window must be positive")
    return retention, window


def _parse_interactions(raw: Mapping[str, Any]) -> dict[str, Interaction]:
    specs = _mapping(raw, "interactions")
    built: dict[str, Interaction] = {}
    building: list[str] = []

    def build(name: str) -> Interaction:
        if name in built:
            return built[name]
        if name in building:
            chain = " -> ".join(building + [name])
            raise ConfigError(f"interaction cycle detected: {chain}")
        if name not in specs:
            raise ConfigError(f"interactions: unknown interaction {name!r}")
        where = f"interactions.{name}"
        entry = _mapping(specs[name] if specs[name] is not None else {}, where)
        _only_keys(entry, {"caused"}, where)
        building.append(name)
        try:
            caused_items = entry.get("caused", [])
            caused = tuple(
                _parse_caused(item, f"{where}.caused[{index}]", build)
                for index, item in enumerate(_list(caused_items, f"{where}.caused"))
            )
        finally:
            building.pop()
        interaction = Interaction(name, caused)
        built[name] = interaction
        return interaction

    for name in specs:
        build(_string(name, "interaction name"))
    return built


def _parse_caused(item: Any, where: str, build) -> CausedInteraction:
    if isinstance(item, str):
        name = item
        entry: Mapping[str, Any] = {}
    else:
        entry = _mapping(item, where)
        if "receivers" in entry:
            raise ConfigError(
                f"{where}: receivers are derived from graph edges; "
                "do not set receivers on interactions"
            )
        _only_keys(entry, {"interaction", "delay", "messages_per_accepted", "profile"}, where)
        name = _string(entry.get("interaction"), f"{where}.interaction")
    delay = _number(entry.get("delay", 0.0), f"{where}.delay", minimum=0)
    multiplier = _integer(
        entry.get("messages_per_accepted", 1),
        f"{where}.messages_per_accepted",
        minimum=0,
    )
    return CausedInteraction(
        build(name),
        delay=delay,
        messages_per_accepted=multiplier,
        receivers=(),
        profile=_parse_profile(entry.get("profile"), f"{where}.profile"),
    )


def _parse_profile(raw: Any, where: str) -> SendingProfile:
    entry = _mapping(raw if raw is not None else {}, where)
    _only_keys(entry, {"bytes_per_message", "retry"}, where)
    return SendingProfile(
        bytes_per_message=_integer(
            entry.get("bytes_per_message", 64), f"{where}.bytes_per_message", minimum=0
        ),
        retry=_parse_retry(entry.get("retry"), f"{where}.retry"),
    )


def _parse_retry(raw: Any, where: str) -> RetryPolicy | None:
    if raw is None or raw is False:
        return None
    if raw is True:
        raise ConfigError(f"{where}: retry must be a mapping, not true")
    entry = _mapping(raw, where)
    _only_keys(entry, {"delay", "max_attempts", "expires_after"}, where)
    expires_after = entry.get("expires_after")
    return RetryPolicy(
        delay=_number(entry.get("delay", 1.0), f"{where}.delay", minimum=0),
        max_attempts=_integer(
            entry.get("max_attempts", 3), f"{where}.max_attempts", minimum=1
        ),
        expires_after=(
            None
            if expires_after is None
            else _number(expires_after, f"{where}.expires_after", minimum=0)
        ),
    )


def _parse_edges(raw: list[Any]) -> tuple[EdgeConfig, ...]:
    edges: list[EdgeConfig] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(raw):
        where = f"edges[{index}]"
        entry = _mapping(item, where)
        _only_keys(entry, {"id", "from", "to", "interaction"}, where)
        edge_id = entry.get("id")
        if edge_id is not None:
            edge_id = _string(edge_id, f"{where}.id")
            if edge_id in seen_ids:
                raise ConfigError(f"{where}: duplicate edge id {edge_id!r}")
            seen_ids.add(edge_id)
        edges.append(
            EdgeConfig(
                from_node=_string(entry.get("from"), f"{where}.from"),
                to_node=_string(entry.get("to"), f"{where}.to"),
                interaction=_string(entry.get("interaction"), f"{where}.interaction"),
                id=edge_id,
            )
        )
    return tuple(edges)


def _parse_producers(raw: list[Any]) -> tuple[ProducerConfig, ...]:
    producers: list[ProducerConfig] = []
    for index, item in enumerate(raw):
        where = f"producers[{index}]"
        entry = _mapping(item, where)
        _only_keys(
            entry,
            {
                "id",
                "targets",
                "target",
                "interaction",
                "rate",
                "batch_size",
                "bytes_per_message",
                "start",
                "stop",
                "retry",
                "observation",
            },
            where,
        )
        if "rate" not in entry:
            raise ConfigError(f"{where}: rate is required")

        if "targets" in entry:
            if "target" in entry or "interaction" in entry:
                raise ConfigError(
                    f"{where}: use either targets or target+interaction, not both"
                )
            targets = tuple(
                _parse_producer_target(target, f"{where}.targets[{target_index}]")
                for target_index, target in enumerate(
                    _list(entry["targets"], f"{where}.targets")
                )
            )
        else:
            if "target" not in entry or "interaction" not in entry:
                raise ConfigError(
                    f"{where}: provide targets, or target and interaction"
                )
            targets = (
                ProducerTarget(
                    node=_string(entry["target"], f"{where}.target"),
                    interaction=_string(
                        entry["interaction"], f"{where}.interaction"
                    ),
                ),
            )

        start = (
            None
            if entry.get("start") is None
            else _number(entry["start"], f"{where}.start")
        )
        stop = (
            None
            if entry.get("stop") is None
            else _number(entry["stop"], f"{where}.stop")
        )
        if start is not None and stop is not None and stop < start:
            raise ConfigError(f"{where}: stop cannot be before start")
        retention, window = _parse_observation(
            entry.get("observation"), f"{where}.observation"
        )
        producers.append(
            ProducerConfig(
                id=_string(entry.get("id"), f"{where}.id"),
                targets=targets,
                rate=_number(entry["rate"], f"{where}.rate", minimum=0),
                batch_size=_integer(
                    entry.get("batch_size", 1), f"{where}.batch_size", minimum=1
                ),
                bytes_per_message=_integer(
                    entry.get("bytes_per_message", 64),
                    f"{where}.bytes_per_message",
                    minimum=0,
                ),
                start=start,
                stop=stop,
                retry=_parse_retry(entry.get("retry"), f"{where}.retry"),
                observation_retention=retention,
                observation_window=window,
            )
        )
    return tuple(producers)


def _parse_producer_target(raw: Any, where: str) -> ProducerTarget:
    entry = _mapping(raw, where)
    _only_keys(entry, {"node", "interaction"}, where)
    return ProducerTarget(
        node=_string(entry.get("node"), f"{where}.node"),
        interaction=_string(entry.get("interaction"), f"{where}.interaction"),
    )


# ---------------------------------------------------------------------------
# Scalar helpers
# ---------------------------------------------------------------------------


def _mapping(value: Any, where: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(f"{where} must be a mapping")
    return value


def _list(value: Any, where: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ConfigError(f"{where} must be a list")
    return value


def _only_keys(entry: Mapping[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(entry) - allowed)
    if unknown:
        raise ConfigError(f"{where}: unknown key(s): {', '.join(unknown)}")


def _string(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{where} must be a non-empty string")
    return value


def _number(value: Any, where: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ConfigError(f"{where} must be a number")
    number = float(value)
    if minimum is not None and number < minimum:
        raise ConfigError(f"{where} must be >= {minimum}")
    return number


def _integer(value: Any, where: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{where} must be an integer")
    if minimum is not None and value < minimum:
        raise ConfigError(f"{where} must be >= {minimum}")
    return value


def _lookup_interaction(config: ExperimentConfig, name: str) -> Interaction:
    try:
        return config.interactions[name]
    except KeyError:
        raise ConfigError(f"unknown interaction {name!r}") from None
