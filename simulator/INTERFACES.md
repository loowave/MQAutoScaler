# mqsim — public interfaces

A fixed-step simulation core for interacting services. Experiments are built
directly in Python or loaded from YAML.

New to the project? Start with [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md), a short
step-by-step recipe for building and plotting a pipeline. Everything below is
the reference. Paths are relative to the `simulator/` directory, which contains
the `mqsim` package, tests and examples.

## Clock and experiment manager

```python
manager = ExperimentManager(tick=0.05, start=0.0)   # seconds
manager.add_node(node_a)
manager.add_node(node_b)
manager.tick_once()      # one start -> deliver -> observe -> advance cycle
manager.run(100)         # 100 ticks
manager.run_until(10.0)  # tick while now < 10.0
manager.now              # current simulated time
manager.tick_times       # timestamps of every executed tick
manager.observe()        # {node_id: NodeObservation}
manager.metrics          # {node_id: {metric: [MetricSample, ...]}}
manager.metric_samples("kafka", "backlog_messages")
manager.usage            # {node_id: [UsageSample, ...]}
manager.usage_samples("kafka")
```

After each tick the manager also appends every metric sample recorded during
that tick to `manager.metrics`, and records an end-of-tick resource usage
snapshot (after delivery) in `manager.usage`. Unlike the per-node observation
stores, these experiment-wide histories are currently **not pruned**, so they
can be used for after-the-fact plotting.

Each tick calls `node.start_tick(now, proxy)` on every registered node in
registration order. Returning from it is the readiness acknowledgment. Only
after all nodes return does the manager call `proxy.deliver(now)`. Work
scheduled during the delivery phase (including at `due == now`) is eligible on
a later tick.

`now` is computed as `start + step * tick`, so fixed steps do not accumulate
floating-point drift. Time comparisons use a 1 ns tolerance (`TIME_EPSILON`).

## YAML configuration

```python
from mqsim import load_experiment

experiment = load_experiment("config/manager.yaml", "config/topology.yaml",
                             "config/workload.yaml")
experiment.run(100)
experiment.observe()
```

`load_config(*paths)` merges files section by section: `manager` and
`interactions` merge by key, `nodes` merges by node id, and `edges` /
`producers` are concatenated. `build_experiment(config, node_types=...)` turns a
validated config into a runnable `BuiltExperiment` (manager, nodes, producers).

```yaml
manager:
  tick: 0.5
  start: 0.0

interactions:
  ingress_request:
    caused:
      - interaction: backend_request      # caused by accepting the parent
        delay: 0.0                        # simulated seconds until due
        messages_per_accepted: 1          # amount per accepted message
        profile:
          bytes_per_message: 64
          retry: {delay: 1.0, max_attempts: 3, expires_after: 10.0}
  backend_request:
    caused: [{interaction: backend_reply, delay: 0.25}]
  backend_reply: {}

nodes:
  gateway:
    type: dummy                           # built-in capped node
    receives: [ingress_request]           # optional; edges/producers infer inputs
    resources: {instances: 1, cpu_per_instance: 1.0, ram_per_instance: 1.0}
    observation: {retention: 60.0, window: 1.0}
    params: {capacity_per_instance_per_tick: 40}  # 40 * resources.instances
  worker:
    type: dummy
    params: {capacity_per_tick: 20}

edges:                                    # edges are the routing source of truth
  - {from: gateway, to: worker, interaction: backend_request}
  - {from: worker, to: gateway, interaction: backend_reply}

producers:
  - id: client
    rate: 20.0                            # messages per simulated second
    batch_size: 5
    bytes_per_message: 256
    start: 0.0                            # defaults to manager.start
    stop: 30.0
    retry: {delay: 1.0, max_attempts: 2}
    targets:
      - {node: gateway, interaction: ingress_request}
```

Key rules:

- **Edges route caused work.** Interaction definitions never name receivers.
  When node `N` accepts an interaction, each caused interaction `C` is sent to
  the destinations of `N`'s outgoing edges whose interaction is `C`. Multiple
  edges between the same nodes are allowed.
- **Interaction graphs are acyclic.** Use a distinct interaction type per stage
  (e.g. `ingress_request` → `backend_request` → `backend_reply`).
- **Node inputs** are the union of incoming edges, producer targets and the
  optional `receives` list. A node registers those interactions and, through
  them, every caused interaction's task.
- **Node types** resolve through a registry. Built-ins:
  - `dummy` (`CappedNode`): per-tick admission cap. Use either
    `capacity_per_tick` or `capacity_per_instance_per_tick` (multiplied by
    `resources.instances`); omitted means accept everything. Traffic drives
    usage through optional `cpu_per_message` and `ram_bytes_per_message`
    (per-tick costs), `disk_bytes_per_message` (cumulative level) and the
    accepted message bytes against `network_per_instance`. Usage is expressed
    per instance, so message costs are divided by `resources.instances`.
  - `queue`, aliased as `kafka` (`QueueNode`): a buffered message queue.
    Required params are `write_interaction` and `read_interaction`, which must
    be caused by the write interaction. Write throughput is bounded by
    `write_capacity_per_tick` or `write_capacity_per_instance_per_tick`; read
    throughput by the matching `read_capacity_*` pair. Accepted writes
    accumulate in `backlog_messages`. Each tick the node drains up to its read
    capacity into `read_interaction` work sent along the node's outgoing edges
    for that interaction. Writes beyond the write capacity are rejected and
    follow the normal retry policy. If `retention` (simulated seconds) is set,
    messages that have waited that long are discarded before each tick's read,
    and the discarded count is recorded as `expired_messages`. Messages handed
    to a consumer leave the backlog even if the consumer rejects them;
    rejection is handled by the sender-side retry machinery.
    `cpu_per_message` charges both writes and reads; `ram_bytes_per_message`
    sizes the backlog in RAM; disk usage follows the backlog's actual byte
    size and network usage follows bytes written and read.

  A `type` is looked up in the registry; custom types:

  ```python
  from mqsim import Node, register_node_type

  @register_node_type("my_node")
  class MyNode(Node):
      def capacity_for(self, delivery, now):
          ...

  # or pass a mapping without changing the global registry
  experiment = load_experiment(path, node_types={"my_node": MyNode})
  ```

  Factories receive `node_id`, `interactions`, `settings`,
  `observation_retention`, `observation_window` plus `params` as keyword args
  and must return a `Node` with the configured id.
- **Producers** are nodes with no incoming interactions. They accumulate
  `rate * elapsed simulated time` messages, split each tick's volume into
  attempts of at most `batch_size`, and broadcast that volume to every target.
  The fractional accumulator keeps the total generated by a given timestamp
  independent of tick size. Rejections use the same task/retry machinery as
  other flows: only the rejected remainder is retried, then dropped at the
  attempt limit or expiry.
- Configuration is strict: unknown keys, undefined interactions/nodes, cycles,
  duplicate ids, reserved `params` keys and `receivers` on interactions are
  reported as `ConfigError` with the YAML location.

### Reference pipeline

`examples/pipeline/*.yaml` builds a client → backend (5 instances) → Kafka
(3 brokers) → consumer (2 instances) → Postgres flow, and
`examples/run_pipeline.py` runs it. The queue is configured as:

```yaml
kafka:
  type: kafka
  resources: {instances: 3}
  params:
    write_interaction: topic_write
    read_interaction: topic_read
    write_capacity_per_instance_per_tick: 400
    read_capacity_per_instance_per_tick: 50   # 150 messages/tick in total
```

With a 180 msg/s client and 1 s ticks, five ticks produce: backend accepts 720,
Kafka accepts 540 and drains 300 (backlog 240), the consumer accepts 300 and
Postgres accepts 150. The suite in `tests/test_pipeline.py` pins these values.

`examples/pipeline2/*.yaml` is the overloaded variant: Kafka accepts
180 messages/tick but serves only 60, with `retention: 10.0`. The backlog
plateaus at 1740 messages; from t=16 onwards Kafka expires 120 messages/tick
while the consumer and Postgres process 60/tick. `tests/test_pipeline.py`
pins those numbers too.

`examples/metric_plots.py` contains the shared plotting helper. Both
`examples/run_pipeline.py` and `examples/run_pipeline2.py` write one PNG per
node (`examples/plots/pipeline/` and `examples/plots/pipeline2/` respectively),
with a subplot per metric read from `manager.metrics` plus utilization panels
for CPU, RAM, disk and network sourced from `manager.usage`. Counter metrics are
summed per tick; level metrics listed in `GAUGE_METRICS` (currently
`backlog_messages`) use the last sample of the tick. Every executed tick
(`manager.tick_times`) appears on the x-axis, with zero filled in for ticks
where a metric recorded no sample.

## Interactions, caused work and tasks

```python
reply = Interaction("reply")
request = Interaction(
    "request",
    caused=(
        CausedInteraction(
            reply,
            delay=0.5,                  # due time of caused work
            messages_per_accepted=1,    # amount per accepted message
            receivers=("node_b",),
            profile=SendingProfile(bytes_per_message=128,
                                   retry=RetryPolicy(delay=1.0,
                                                     max_attempts=3,
                                                     expires_after=10.0)),
        ),
    ),
)
```

A node registers interactions at construction (`Node("a", [request])`) and gets
one long-lived `Task` per interaction, including interactions reachable through
`caused` links. Tasks persist for the node's lifetime.

`node.receive(delivery, now)` calls `capacity_for()` (the node's admission
formula), accepts a prefix of the batch, updates node state in `on_accept()`,
and adds caused work to the existing tasks using only the accepted portion:

```python
class MyNode(Node):
    def capacity_for(self, delivery, now) -> int:
        return min(delivery.batch.count, self.free_slots(now))

    def on_accept(self, delivery, accepted: Batch, now) -> None:
        self.usage.cpu_per_instance += 0.01 * accepted.count
```

`receive()` returns the accepted prefix length.

## Work, delivery attempts and results

These are separate types:

| Scheduled work | Delivery attempt | Receiver view | Result |
| --- | --- | --- | --- |
| `TaskEntry(due, specs)` and `TrafficSpec(receiver, count, profile, ...)` | `DeliveryAttempt` submitted to `Proxy` | `Delivery` passed to `receive()` | `AdmissionResult(attempt, accepted)` |

`Task` is a due-time ordered `deque`:

- `add_work(due, specs)` merges into the pending head when the due time matches,
  otherwise inserts in due-time order.
- `requeue(due, specs)` inserts a separate retry entry.
- `pop_due(now)` pops the head only when it is due; returns `None` for an empty
  deque or a future head.
- `task.state` is a shared dict for coordinated flows; subclasses can read and
  update it in `prepare_dispatch()` / `on_result()`.

The proxy groups attempts per receiver and orders them deterministically by
`(sender, task, submission sequence)`. It makes every admission decision before
relaying any result. A sender's `handle_admission_result()` requeues only the
rejected remainder, at `now + retry.delay`, while the attempt limit and expiry
allow. Accepted work is not retried.

## Resources and observations

`ResourceSettings` holds per-instance capacity (`instances`, `cpu_per_instance`,
`ram_per_instance`, `disk_per_instance`, `network_per_instance`).
`ResourceUsage` holds current per-instance use plus `messages`/`bytes` counters,
and offers `utilization(settings)` / `max_utilization(settings)`. Concrete
formulas are left to node implementations.

The built-in nodes use a simple traffic cost model, configured through node
`params`:

- `cpu_per_message` — per-tick CPU units per accepted (and, for queues, read)
  message;
- `ram_bytes_per_message` — per-tick RAM bytes for `dummy` nodes, or the
  backlog's RAM footprint for `queue` nodes;
- `disk_bytes_per_message` — cumulative stored bytes for `dummy` nodes
  (e.g. a database); queues derive disk usage from the backlog's actual bytes;
- network usage is the accepted/read message bytes per tick.

All usage is per instance (divided by `resources.instances`). CPU, RAM and
network are per-tick values reset in `prepare_tick()`; disk is a persistent
level. The manager records an end-of-tick usage snapshot for every node, and
`examples/metric_plots.py` adds `utilization_<resource>` panels whenever a
resource has nonzero capacity and saw traffic.

Each node owns an `ObservationStore`:

```python
node.observations.record_metric(now, "backlog", 12)
node.observations.log(now, "saturated", level="warning", queue="inbox")
node.observations.record_usage(now, node.usage)

node.observations.metric_samples("backlog", since=now - 10)
node.observations.latest_metrics()
node.observations.metric_windows("backlog")        # 1 s windows by default
node.observations.usage_samples()
node.observations.logs(level="warning")

node.observe()  # NodeObservation: metrics, logs, usage, utilization
```

History is retained for a configurable number of simulated seconds
(`observation_retention`); the manager prunes it against the simulated clock on
every tick. `metric_windows(name, size=1.0)` groups samples by simulated
timestamp, independent of tick duration.
