#!/usr/bin/env python3
"""Строит профиль интенсивности для генератора нагрузки (manifests/load/replay.js).

Профиль — CSV с колонками t_offset_s и rps и постоянным шагом. Рядом записывается <out>.meta.json
с параметрами построения: их нужно сохранять вместе с результатами запуска.

Абсолютный уровень нагрузки задаётся при запуске через SCALE, поэтому профили в репозитории
описывают только форму и нормированы на пик --peak (по умолчанию 100).

Синтетические формы:
  make_profile.py synthetic --shape ramp -o ramp.csv
  форма: constant | step | ramp | spike | diurnal | input-stop

Реальные трассы (те же, что разобраны в 01_eda):
  # NAB: CSV с колонками timestamp и value
  make_profile.py series --input nyc_taxi.csv --start 2014-10-06 --end 2014-10-13 -o taxi.csv

  # Azure Functions 2021: app,func,end_timestamp,duration; поток считается по времени прихода
  make_profile.py azure2021 --input AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt \\
      --window-start-h 24 --window-hours 24 -o azure.csv

Трасса сжимается во времени до --duration секунд, коэффициент сжатия пишется в meta.json.
"""
import argparse
import csv
import json
import math
import os
import random
from collections import Counter
from datetime import datetime, timezone


COMMON_PARAMS = ["source", "duration", "step", "peak"]
SOURCE_PARAMS = {
    "synthetic": ["shape", "base_share", "noise", "seed"],
    "series": ["start", "end"],
    "azure2021": ["window_start_h", "window_hours"],
}


def parse_time(value: str) -> float:
    """Секунды: число как есть или дата ISO 8601, которую считаем заданной в UTC."""
    try:
        return float(value)
    except ValueError:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return moment.timestamp()


def resample(points, step):
    """Приводит ряд к сетке с шагом step.

    Если в ячейку попало несколько точек, берётся их среднее, чтобы всплески не терялись.
    В пустых ячейках значение интерполируется между соседними точками.
    """
    points = sorted(points)
    start, end = points[0][0], points[-1][0]
    n = max(1, int(math.ceil((end - start) / step)))
    totals, counts = [0.0] * n, [0] * n
    for t, v in points:
        i = min(n - 1, int((t - start) // step))
        totals[i] += v
        counts[i] += 1

    series, j = [], 0
    for i in range(n):
        t = start + i * step
        if counts[i]:
            value = totals[i] / counts[i]
        else:
            while j + 1 < len(points) and points[j + 1][0] <= t:
                j += 1
            (ta, va), (tb, vb) = points[j], points[min(j + 1, len(points) - 1)]
            value = va if tb == ta else va + (vb - va) * (t - ta) / (tb - ta)
        series.append((i * step, value))
    return series


def normalize(series, peak):
    top = max(v for _, v in series) or 1.0
    return [(t, v / top * peak) for t, v in series]


def compress(points, duration):
    """Сжимает ряд во времени так, чтобы он занял duration секунд."""
    points = sorted(points)
    span = points[-1][0] - points[0][0]
    if span <= 0:
        raise SystemExit("в выбранном окне меньше двух точек")
    factor = span / duration
    return [((t - points[0][0]) / factor, v) for t, v in points], factor


def synthetic(args):
    rnd = random.Random(args.seed)
    n = int(args.duration // args.step)
    peak, base = args.peak, args.peak * args.base_share
    series = []
    for i in range(n):
        x = i / max(1, n - 1)
        if args.shape == "constant":
            value = peak
        elif args.shape == "step":
            levels = [base, (base + peak) / 2, peak]
            value = levels[min(len(levels) - 1, int(x * len(levels)))]
        elif args.shape == "ramp":
            value = base + (peak - base) * x
        elif args.shape == "spike":
            value = peak if 0.45 <= x < 0.55 else base
        elif args.shape == "diurnal":
            value = base + (peak - base) * (1 - math.cos(2 * math.pi * x)) / 2
        elif args.shape == "input-stop":
            value = 0.0 if 0.45 <= x < 0.55 else (base + peak) / 2
        else:
            raise SystemExit(f"неизвестная форма: {args.shape}")
        series.append((i * args.step, max(0.0, value * (1 + rnd.gauss(0, args.noise)))))
    return normalize(series, peak), {}


def from_series(args):
    start = parse_time(args.start) if args.start else -math.inf
    end = parse_time(args.end) if args.end else math.inf
    points = []
    with open(args.input, newline="") as f:
        for row in csv.DictReader(f):
            t = parse_time(row[args.time_col])
            if start <= t < end:
                points.append((t, float(row[args.value_col])))
    points, factor = compress(points, args.duration)
    return normalize(resample(points, args.step), args.peak), {"time_compression": round(factor, 2)}


def from_azure2021(args):
    start = args.window_start_h * 3600
    end = start + args.window_hours * 3600
    per_minute = Counter()
    with open(args.input, newline="") as f:
        for row in csv.DictReader(f):
            arrival = float(row["end_timestamp"]) - float(row["duration"])
            if start <= arrival < end:
                per_minute[int(arrival // 60)] += 1
    if not per_minute:
        raise SystemExit("в выбранном окне нет вызовов")
    first, last = min(per_minute), max(per_minute)
    points = [(m * 60.0, float(per_minute[m])) for m in range(first, last + 1)]
    points, factor = compress(points, args.duration)
    return normalize(resample(points, args.step), args.peak), {"time_compression": round(factor, 2)}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source", choices=["synthetic", "series", "azure2021"])
    p.add_argument("-o", "--out", required=True)
    p.add_argument("--duration", type=float, default=1800, help="длительность профиля, с")
    p.add_argument("--step", type=float, default=10, help="шаг профиля, с")
    p.add_argument("--peak", type=float, default=100, help="значение в пике, RPS")
    p.add_argument("--shape", default="diurnal", help="synthetic: форма нагрузки")
    p.add_argument("--base-share", type=float, default=0.2, help="synthetic: фон как доля от пика")
    p.add_argument("--noise", type=float, default=0.03, help="synthetic: σ мультипликативного шума")
    p.add_argument("--seed", type=int, default=1, help="synthetic: зерно генератора шума")
    p.add_argument("--input", help="series, azure2021: исходный файл")
    p.add_argument("--time-col", default="timestamp", help="series: колонка времени")
    p.add_argument("--value-col", default="value", help="series: колонка значения")
    p.add_argument("--start", help="series: начало окна, включая")
    p.add_argument("--end", help="series: конец окна, не включая")
    p.add_argument("--window-start-h", type=float, default=0, help="azure2021: начало окна, ч от начала трассы")
    p.add_argument("--window-hours", type=float, default=24, help="azure2021: длина окна, ч")
    args = p.parse_args()

    build = {"synthetic": synthetic, "series": from_series, "azure2021": from_azure2021}[args.source]
    profile, extra = build(args)

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["t_offset_s", "rps"])
        for t, value in profile:
            writer.writerow([round(t, 3), round(value, 2)])

    mean = sum(v for _, v in profile) / len(profile)
    meta = {k: getattr(args, k) for k in COMMON_PARAMS + SOURCE_PARAMS[args.source]}
    if args.input:
        meta["input"] = os.path.basename(args.input)
    meta.update(extra, points=len(profile), duration_s=profile[-1][0] + args.step, mean_rps=round(mean, 2))
    with open(args.out + ".meta.json", "w") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"{args.out}: {len(profile)} точек, {meta['duration_s']:.0f} с, средняя интенсивность {mean:.1f} RPS")


if __name__ == "__main__":
    main()
