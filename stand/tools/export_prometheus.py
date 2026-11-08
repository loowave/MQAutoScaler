#!/usr/bin/env python3
"""Выгружает признаки и цели запуска из Prometheus в Parquet.

Забирает все ряды mq:* за интервал запуска; их список и смысл описаны в docs/metrics.md.
Формат длинный: ts, metric, value и колонки меток (namespace, workload, service, topic, ...).

  kubectl -n monitoring port-forward svc/kps-prometheus 9090 &
  python3 tools/export_prometheus.py --start 2026-11-10T12:00:00Z --end 2026-11-10T12:30:00Z \\
      --out datasets/<run_id>

Зависимости: tools/requirements.txt.
"""
import argparse
import os
from datetime import datetime, timezone

import pandas as pd
import requests

SELECTOR = '{__name__=~"mq:.+"}'
CHUNK_S = 3600  # query_range отдаёт до 11 000 точек на ряд: час при шаге 5 с — это 720 точек
SERVICE_LABELS = ("prometheus", "job", "endpoint", "container", "prometheus_replica")


def parse_ts(value: str) -> float:
    moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def query_range(prom, start, end, step):
    response = requests.get(f"{prom}/api/v1/query_range",
                            params={"query": SELECTOR, "start": start, "end": end, "step": step},
                            timeout=300)
    response.raise_for_status()
    rows = []
    for series in response.json()["data"]["result"]:
        labels = dict(series["metric"])
        name = labels.pop("__name__")
        for ts, value in series["values"]:
            rows.append({**labels, "metric": name, "ts": float(ts), "value": float(value)})
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--prom", default="http://localhost:9090")
    p.add_argument("--start", required=True, help="начало интервала, ISO 8601, UTC")
    p.add_argument("--end", required=True, help="конец интервала, ISO 8601, UTC")
    p.add_argument("--step", type=float, default=5, help="шаг, с")
    p.add_argument("--out", required=True, help="каталог запуска")
    args = p.parse_args()

    start, end = parse_ts(args.start), parse_ts(args.end)
    rows = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + CHUNK_S, end)
        rows += query_range(args.prom, chunk_start, chunk_end, args.step)
        chunk_start = chunk_end + args.step

    if not rows:
        raise SystemExit("за указанный интервал рядов mq:* нет")
    frame = pd.DataFrame(rows)
    frame = frame.drop(columns=[c for c in SERVICE_LABELS if c in frame])
    os.makedirs(args.out, exist_ok=True)
    path = os.path.join(args.out, "metrics.parquet")
    frame.to_parquet(path, index=False)
    print(f"{path}: {len(frame)} строк, {frame['metric'].nunique()} рядов-метрик")


if __name__ == "__main__":
    main()
