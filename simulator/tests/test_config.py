"""YAML loading, validation, building and edge-derived routing."""

from __future__ import annotations

from pathlib import Path

import pytest

from mqsim import (
    CappedNode,
    ConfigError,
    ManagerConfig,
    Node,
    build_experiment,
    load_config,
    load_experiment,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_DIR = REPO_ROOT / "examples" / "config"


def write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_load_merges_split_yaml_files(tmp_path):
    topology = write(
        tmp_path,
        "topology.yaml",
        """
manager:
  tick: 0.5
  start: 2.0
interactions:
  request:
    caused:
      - interaction: reply
        delay: 1.0
        messages_per_accepted: 2
        profile:
          bytes_per_message: 128
          retry:
            delay: 0.5
            max_attempts: 2
  reply: {}
nodes:
  first:
    params:
      capacity_per_tick: 5
""",
    )
    workload = write(
        tmp_path,
        "workload.yaml",
        """
nodes:
  second: {}
edges:
  - from: first
    to: second
    interaction: reply
producers:
  - id: load
    rate: 2
    targets:
      - node: first
        interaction: request
""",
    )

    config = load_config(topology, workload)

    assert config.manager == ManagerConfig(tick=0.5, start=2.0)
    assert set(config.interactions) == {"request", "reply"}
    caused = config.interactions["request"].caused[0]
    assert caused.interaction is config.interactions["reply"]
    assert caused.delay == 1.0
    assert caused.messages_per_accepted == 2
    assert caused.receivers == ()
    assert caused.profile.bytes_per_message == 128
    assert caused.profile.retry.delay == 0.5
    assert set(config.nodes) == {"first", "second"}
    assert config.nodes["first"].params == {"capacity_per_tick": 5}
    assert len(config.edges) == 1
    assert config.producers[0].rate == 2
    assert config.producers[0].targets[0].node == "first"


def test_builds_the_example_experiment_with_edges_only_routing():
    experiment = load_experiment(*sorted(EXAMPLE_DIR.glob("*.yaml")))

    assert experiment.manager.tick == 0.5
    assert set(experiment.nodes) == {"gateway", "worker"}
    assert set(experiment.producers) == {"client"}

    gateway = experiment.node("gateway")
    worker = experiment.node("worker")
    assert isinstance(gateway, CappedNode)
    assert gateway.routes == {"backend_request": ("worker",)}
    assert worker.routes == {"backend_reply": ("gateway",)}
    assert gateway.settings.instances == 1
    assert worker.settings.instances == 2
    assert experiment.config.interactions["ingress_request"].caused[0].receivers == ()

    producer = experiment.producer("client")
    assert producer.targets[0].node == "gateway"
    assert producer.targets[0].interaction == "ingress_request"


def test_configured_experiment_runs_traffic_through_the_graph(tmp_path):
    path = write(
        tmp_path,
        "experiment.yaml",
        """
manager:
  tick: 1.0
interactions:
  ingress:
    caused:
      - interaction: backend
  backend:
    caused:
      - interaction: reply
  reply: {}
nodes:
  gateway:
    params:
      capacity_per_tick: 100
  worker:
    params:
      capacity_per_tick: 100
edges:
  - from: gateway
    to: worker
    interaction: backend
  - from: worker
    to: gateway
    interaction: reply
producers:
  - id: client
    rate: 10
    batch_size: 5
    stop: 1.0
    targets:
      - node: gateway
        interaction: ingress
""",
    )
    experiment = load_experiment(path)

    experiment.run(2)  # ticks at t=0 and t=1
    gateway = experiment.node("gateway")
    worker = experiment.node("worker")
    producer = experiment.producer("client")

    assert gateway.task("backend").head.message_count == 10
    assert gateway.task("backend").head.due == 1.0
    assert producer.recent_metrics()["generated_messages"] == 10

    experiment.run(1)  # t=2: gateway forwards backend work to worker
    assert gateway.task("backend").head is None
    assert worker.task("reply").head.message_count == 10
    assert (
        sum(
            sample.value
            for sample in worker.observations.metric_samples("accepted_messages")
        )
        == 10
    )

    experiment.run(1)  # t=3: worker replies to gateway
    assert worker.task("reply").head is None
    assert (
        sum(
            sample.value
            for sample in gateway.observations.metric_samples("accepted_messages")
        )
        == 20  # ten ingress at t=1 plus ten replies at t=3
    )


def test_custom_node_type_is_resolved_from_the_registry(tmp_path):
    class RejectAll(Node):
        def capacity_for(self, delivery, now):
            return 0

    path = write(
        tmp_path,
        "custom.yaml",
        """
manager:
  tick: 1.0
interactions:
  request: {}
nodes:
  sink:
    type: reject_all
producers:
  - id: load
    rate: 5
    batch_size: 5
    targets:
      - node: sink
        interaction: request
""",
    )
    experiment = load_experiment(path, node_types={"reject_all": RejectAll})

    assert isinstance(experiment.node("sink"), RejectAll)
    experiment.run(2)  # t=0 emits nothing, t=1 emits five rejected messages
    assert experiment.node("sink").recent_metrics()["accepted_messages"] == 0
    assert experiment.producer("load").recent_metrics()["dropped_messages"] == 5


def test_nodes_can_be_given_as_a_list(tmp_path):
    path = write(
        tmp_path,
        "list.yaml",
        """
nodes:
  - id: alpha
    params:
      capacity_per_tick: 7
""",
    )
    config = load_config(path)
    assert set(config.nodes) == {"alpha"}
    assert config.nodes["alpha"].params == {"capacity_per_tick": 7}


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("bogus: {}", "unknown key"),
        (
            """
nodes: {a: {}}
interactions: {x: {}}
edges:
  - {from: a, to: b, interaction: x}
""",
            "unknown destination node",
        ),
        (
            """
nodes: {a: {}}
interactions: {x: {}}
edges:
  - {from: a, to: a, interaction: y}
""",
            "unknown interaction",
        ),
        (
            """
interactions:
  a:
    caused:
      - interaction: b
  b:
    caused:
      - interaction: a
""",
            "cycle",
        ),
        (
            """
interactions:
  a:
    caused:
      - interaction: b
        receivers: [x]
  b: {}
""",
            "derived from graph edges",
        ),
        (
            """
nodes: {a: {}}
interactions: {x: {}}
producers:
  - id: p
    rate: 1
    targets: [{node: nope, interaction: x}]
""",
            "unknown target node",
        ),
        (
            """
nodes: {a: {}}
interactions: {x: {}}
producers:
  - id: a
    rate: 1
    targets: [{node: a, interaction: x}]
""",
            "collides with a node id",
        ),
        (
            """
nodes:
  a:
    params:
      node_id: override
""",
            "reserved key",
        ),
    ],
)
def test_invalid_configuration_is_rejected(tmp_path, text, match):
    path = write(tmp_path, "invalid.yaml", text)
    with pytest.raises(ConfigError, match=match):
        load_config(path)


def test_unknown_node_type_is_rejected_when_building(tmp_path):
    path = write(
        tmp_path,
        "type.yaml",
        """
nodes:
  a:
    type: missing
""",
    )
    config = load_config(path)
    with pytest.raises(ConfigError, match="unknown node type"):
        build_experiment(config)


def test_producer_stop_before_simulation_start_is_rejected(tmp_path):
    path = write(
        tmp_path,
        "producer.yaml",
        """
manager:
  start: 10.0
interactions:
  request: {}
nodes:
  sink: {}
producers:
  - id: load
    rate: 1
    stop: 5.0
    targets:
      - node: sink
        interaction: request
""",
    )
    config = load_config(path)
    with pytest.raises(ConfigError, match="before the simulation start"):
        build_experiment(config)
