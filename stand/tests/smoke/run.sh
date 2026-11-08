#!/usr/bin/env bash
# Smoke-тест без Kubernetes: поднимает платформу в Docker Compose, подаёт нагрузку выше ёмкости,
# ждёт разбора очереди и проверяет метрики и баланс событий. В конце удаляет контейнеры и тома.
# Занимает около 5 минут, нужно примерно 6 CPU и 6 GB свободной памяти.
set -euo pipefail

cd "$(dirname "$0")"
STAND_DIR="$(cd ../.. && pwd)"
export SCALE="${SCALE:-2}"

cleanup() { docker compose --profile load down -v --timeout 5 >/dev/null 2>&1 || true; }
trap cleanup EXIT

mkdir -p generated
python3 extract_rules.py "$STAND_DIR/manifests/monitoring/rules.yaml" generated/rules.yaml
python3 "$STAND_DIR/tools/make_profile.py" synthetic --shape ramp --duration 180 --noise 0 -o generated/ramp.csv
export PROFILE_CSV=./generated/ramp.csv

docker build -q -t mqautoscaler/platform:0.1.0 "$STAND_DIR/app" >/dev/null
docker compose up -d --wait-timeout 180 --quiet-pull 2>/dev/null
echo "Платформа запущена, прогрев 30 с"
sleep 30

echo "Нагрузка: профиль ramp, пик $((SCALE * 100)) RPS"
docker compose --profile load run --rm --no-deps k6 >/dev/null

echo "Ожидание разбора очереди"
for _ in $(seq 1 120); do
  lag="$(curl -s http://127.0.0.1:19090/api/v1/query --data-urlencode 'query=sum(kafka_consumergroup_lag)' \
    | python3 -c 'import json, sys; r = json.load(sys.stdin)["data"]["result"]; print(int(float(r[0]["value"][1])) if r else -1)')"
  [ "$lag" = "0" ] && break
  sleep 5
done
sleep 20  # последние значения счётчиков должны попасть в Prometheus

python3 verify.py
