#!/usr/bin/env python3
"""Случайная политика масштабирования: даёт примеры «состояние → действие → результат».

Каждые --period секунд с вероятностью --p меняет на ±1 число реплик случайно выбранного сервиса
в пределах [min, max]. Каждое действие записывается строкой в events.jsonl. Без разнообразных
действий нельзя оценить, что произошло бы при другом решении, а в открытых датасетах таких данных нет.

Не запускать вместе с HPA или KEDA из manifests/policies: они будут перебивать действия.

  python3 tools/random_policy.py --duration 1800 --events datasets/<run_id>/events.jsonl --seed 1
"""
import argparse
import json
import os
import random
import subprocess
import time

# Верхняя граница консьюмеров равна числу партиций топика: лишние реплики простаивали бы.
TARGETS = {
    "ingest": (1, 4),
    "filter": (1, 6),
    "enricher": (1, 6),
    "processor": (1, 6),
    "showcase": (1, 4),
}


def kubectl(*args):
    return subprocess.run(["kubectl", "-n", "platform", *args], check=True,
                          capture_output=True, text=True).stdout.strip()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--duration", type=float, required=True, help="длительность, с")
    p.add_argument("--period", type=float, default=60, help="период принятия решения, с")
    p.add_argument("--p", type=float, default=0.5, help="вероятность действия в каждом периоде")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--events", required=True, help="файл журнала действий")
    args = p.parse_args()

    rnd = random.Random(args.seed)
    os.makedirs(os.path.dirname(args.events) or ".", exist_ok=True)
    end = time.time() + args.duration
    with open(args.events, "a") as log:
        while time.time() < end:
            time.sleep(args.period)
            if rnd.random() > args.p:
                continue
            name = rnd.choice(sorted(TARGETS))
            low, high = TARGETS[name]
            current = int(kubectl("get", "deployment", name, "-o", "jsonpath={.spec.replicas}"))
            target = min(high, max(low, current + rnd.choice([-1, 1])))
            if target == current:
                continue
            kubectl("scale", "deployment", name, f"--replicas={target}")
            log.write(json.dumps({"ts": time.time(), "type": "action", "policy": "random", "action": "scale",
                                  "target": name, "from": current, "to": target}) + "\n")
            log.flush()


if __name__ == "__main__":
    main()
