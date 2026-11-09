"""Load the example YAML experiment, run it and print a short summary."""

from pathlib import Path

from mqsim import load_experiment

config_dir = Path(__file__).parent / "example"
experiment = load_experiment(*sorted(config_dir.glob("*.yaml")))
experiment.run(20)

print(f"now={experiment.manager.now}")
for node_id, observation in sorted(experiment.observe().items()):
    print(node_id, dict(observation.metrics))
