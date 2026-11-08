#!/usr/bin/env python3
"""Проверяет результат smoke-теста: метрики собираются, правила считаются, события не теряются.

Ожидает, что очередь уже разобрана. Выход 0 — все проверки пройдены.
"""
import json
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

PROMETHEUS = "http://127.0.0.1:19090"
RULES = Path(__file__).parent / "generated" / "rules.yaml"

# Ряды, которым нужен Kubernetes или node-exporter: их проверяют модульные тесты tests/rules.
NEEDS_CLUSTER = re.compile(r"^mq:(cpu_|memory_|network_|node_|replicas_|pods_|container_|oom_|biz_allocated)")
# Появляются только при сбоях или считаются по окну, в котором в конце теста может не быть потока.
OPTIONAL = {
    "mq:load_dropped_iterations:rate30s",
    "mq:redis_evictions:rate30s",
}


def query(expr):
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=20) as response:
        return json.load(response)["data"]["result"]


def scalar(expr):
    result = query(expr)
    return float(result[0]["value"][1]) if result else 0.0


def postgres(sql):
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "platform", "-Atc", sql],
        cwd=Path(__file__).parent, check=True, capture_output=True, text=True)
    return out.stdout.strip()


def rule_names():
    return re.findall(r"record: (mq:\S+)", RULES.read_text())


def main():
    failures = []

    def check(name, ok, details=""):
        print(f"{'OK  ' if ok else 'FAIL'} {name}" + (f": {details}" if details else ""))
        if not ok:
            failures.append(name)

    down = [t["metric"]["service"] for t in query("up == 0")]
    check("все цели сбора доступны", not down, ", ".join(down))

    missing = []
    for name in rule_names():
        if NEEDS_CLUSTER.match(name) or name in OPTIONAL:
            continue
        if not query(f"last_over_time({name}[10m])"):
            missing.append(name)
    check("recording rules дают данные", not missing, ", ".join(missing))

    routes = {t["metric"]["route"] for t in query("http_requests_total")}
    check("HTTP-маршруты распознаются", "unmatched" not in routes and len(routes) == 2, ", ".join(sorted(routes)))

    received = scalar("sum(platform_events_received_total)")
    sent = scalar('sum(k6_http_reqs_total{name="publish"})')
    failed = scalar('sum(k6_http_reqs_total{expected_response="false"})')
    dropped = scalar("sum(k6_dropped_iterations_total)")
    check("платформа приняла всё, что отправил генератор", received == sent and received > 0,
          f"отправлено {sent:.0f}, принято {received:.0f}")
    check("клиент не видел ошибок", failed == 0, f"{failed:.0f}")
    check("генератору хватило виртуальных пользователей", dropped == 0, f"{dropped:.0f}")

    lag = scalar("sum(kafka_consumergroup_lag)")
    check("очередь разобрана", lag == 0, f"lag {lag:.0f}")

    filtered = scalar("sum(platform_events_filtered_total)")
    processed = scalar("sum(platform_events_processed_total)")
    lost = scalar("sum(platform_events_lost_total)")
    evicted = scalar("sum(consumer_evicted_messages_total)")
    rows = float(postgres("SELECT count(*) FROM events"))
    stats = float(postgres("SELECT coalesce(sum(processed), 0) FROM tenant_stats"))
    check("баланс событий сходится", received == filtered + rows + lost + evicted,
          f"принято {received:.0f} = отфильтровано {filtered:.0f} + в базе {rows:.0f} "
          f"+ потеряно {lost:.0f} + вытеснено {evicted:.0f}")
    check("счётчик обработанных совпадает с базой", processed == rows, f"{processed:.0f} и {rows:.0f}")
    check("агрегаты витрины совпадают с событиями", stats == rows, f"{stats:.0f} и {rows:.0f}")

    ratio = received and processed / (received - filtered)
    check("доля дошедших до витрины равна единице", ratio == 1, f"{ratio:.4f}")

    if failures:
        print(f"\nНе пройдено проверок: {len(failures)}")
        sys.exit(1)
    print("\nВсе проверки пройдены")


if __name__ == "__main__":
    main()
