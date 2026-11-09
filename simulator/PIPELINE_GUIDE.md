# Building a pipeline

A short recipe for going from "I want these services talking" to a running
simulation with plots. `INTERFACES.md` is the full reference; this is the
practical path through it.

All commands below assume you are in the `simulator/` directory:

```bash
uv sync
.venv/bin/python examples/run_pipeline.py
```

## The mental model

Four YAML sections describe a pipeline:

| Section | Answers |
| --- | --- |
| `manager` | How fast does simulated time advance? |
| `interactions` | What work exists, and what does accepting it cause? |
| `nodes` | Who processes work, with how much capacity and resources? |
| `edges` | For each caused interaction, who receives it? |
| `producers` | Where does new traffic enter? |

Edges are the routing source of truth. An interaction definition never names a
receiver; it only says "accepting `X` causes `Y` after `delay` seconds". When a
node accepts `X`, the caused `Y` goes to every node at the other end of that
node's outgoing `Y` edges.

One tick runs `start_tick` on every node, then delivers all work due at that
timestamp, then records metrics. Work caused during delivery is due on a later
tick at the earliest, even when its delay is zero.

## 1. Define interactions

Name each stage separately and keep the graph acyclic:

```yaml
# interactions.yaml
interactions:
  client_request:
    caused:
      - interaction: topic_write
        profile:
          bytes_per_message: 512
          retry: {delay: 1.0, max_attempts: 3, expires_after: 10.0}

  topic_write:
    caused:
      - interaction: topic_read
        profile: {bytes_per_message: 512}

  topic_read:
    caused:
      - interaction: db_write
        profile: {bytes_per_message: 1024}

  db_write: {}
```

`caused` entries default to `delay: 0.0` and `messages_per_accepted: 1`.
`profile.bytes_per_message` drives network/disk accounting; `retry` controls
what happens when the receiver rejects part of a batch.

## 2. Define nodes

Every node needs a `type` from the registry (`dummy`, `kafka`/`queue`, or a
custom one) and, optionally, `resources`, `observation` and `params`.

```yaml
# nodes.yaml
nodes:
  backend:
    type: dummy
    resources:
      instances: 5
      cpu_per_instance: 1.0
      ram_per_instance: 1000000
      network_per_instance: 200000
    params:
      capacity_per_instance_per_tick: 40   # 40 * 5 = 200 messages/tick
      cpu_per_message: 0.01
      ram_bytes_per_message: 2048

  kafka:
    type: kafka
    resources: {instances: 3, cpu_per_instance: 1.0,
                ram_per_instance: 2000000, disk_per_instance: 2000000}
    params:
      write_interaction: topic_write
      read_interaction: topic_read
      write_capacity_per_instance_per_tick: 400
      read_capacity_per_instance_per_tick: 50   # 3 * 50 = 150 reads/tick
      cpu_per_message: 0.002
      ram_bytes_per_message: 1024
      retention: 10.0                          # optional, simulated seconds

  postgres:
    type: dummy
    resources: {instances: 1, cpu_per_instance: 2.0, disk_per_instance: 10000000}
    params:
      capacity_per_instance_per_tick: 500
      cpu_per_message: 0.01
      disk_bytes_per_message: 1024
```

Capacity is **per simulated tick**, not per second. At `tick: 1.0` the numbers
above are also per second; at `tick: 0.5` halve them for the same throughput.
`capacity_per_tick` is the absolute form; `_per_instance_per_tick` is
multiplied by `resources.instances`.

A `queue` node also needs outgoing edges for its `read_interaction` — without
them, drained messages have nowhere to go.

## 3. Wire the edges

```yaml
# edges.yaml
edges:
  - {from: backend, to: kafka, interaction: topic_write}
  - {from: kafka, to: consumer, interaction: topic_read}
  - {from: consumer, to: postgres, interaction: db_write}
```

An edge is valid only when the source node can cause that interaction
(directly or through an interaction it receives). Multiple edges for the same
interaction broadcast to several receivers.

## 4. Add traffic

Producers are special nodes with no inputs. They generate `rate` messages per
simulated second between `start` and `stop`, split into attempts of at most
`batch_size`, and broadcast each attempt to every target:

```yaml
# producers.yaml
producers:
  - id: client
    rate: 180.0
    batch_size: 10
    bytes_per_message: 512
    start: 0.0
    stop: 60.0
    targets:
      - {node: backend, interaction: client_request}
```

`rate` is per second; the producer's fractional accumulator keeps the generated
total independent of tick size. A rejection from one target is retried on its
own with the producer's `retry` policy; without one, the rejected remainder is
dropped.

## 5. Configure the clock

```yaml
# manager.yaml
manager:
  tick: 1.0
  start: 0.0
```

Smaller ticks give finer interleaving; they do not change rates, because rates
are defined in simulated seconds. Observation windows remain 1 second
regardless of tick size.

## 6. Load, run and plot

```python
from pathlib import Path
from mqsim import load_experiment
from metric_plots import write_plots

experiment = load_experiment(*sorted(Path("examples/pipeline").glob("*.yaml")))
experiment.run(30)

print(experiment.manager.now)
print(experiment.node("kafka").recent_metrics())

write_plots(experiment.manager, Path("examples/plots/mypipeline"))
```

`load_experiment` merges the files in the order given — merge files freely
(manager/topology/workload splits are conventional, not required). After
running, `manager.metrics` and `manager.usage` hold unpruned histories for
plotting, while each node keeps its own retention-limited `observations`.

`write_plots` writes one PNG per node: every metric recorded over simulated
time plus `utilization_cpu/ram/disk/network` panels where capacity is
configured. All executed ticks appear on the x-axis; ticks without a sample
are zero-filled. Counter metrics are summed per tick; `backlog_messages` is
treated as a level and plotted at its last value per tick.

## Checklist

- Interaction graph is acyclic; each hop has a distinct interaction name.
- Every caused interaction has an outgoing edge from the node that causes it.
- Queue `read_interaction` is caused by its `write_interaction` and has
  outgoing edges.
- Capacities are per tick; producer rates are per second.
- `stop` is later than `start`, and `experiment.run(...)` covers the window
  you want to inspect.
- Node ids referenced by edges, producers and queue params exist.

Configuration mistakes (unknown keys, undefined nodes/interactions, cycles,
duplicate ids) raise `ConfigError` with the YAML file and key location.

## Going further

- Change capacities/retention in a copy of `examples/pipeline2/` to reproduce
  overload and queue expiry.
- Add a node type without touching the config schema:

  ```python
  from mqsim import Node, register_node_type

  @register_node_type("my_node")
  class MyNode(Node):
      def capacity_for(self, delivery, now):
          return delivery.batch.count

  ```

  See `INTERFACES.md` for the hooks (`prepare_tick`, `capacity_for`,
  `on_accept`, `_add_caused_work`) and the resource/observation APIs.
