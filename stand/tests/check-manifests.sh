#!/usr/bin/env bash
# Проверки без кластера: сборка оверлеев, схемы Kubernetes, значения чартов, recording rules,
# скрипты и полнота документации. Нужны kubectl, helm, docker и python3 с pyyaml.
# С кластером не взаимодействует: helm запускается с пустым kubeconfig.
set -euo pipefail

cd "$(dirname "$0")/.."
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

PROMETHEUS_IMAGE=quay.io/prometheus/prometheus:v3.16.0
CHART_VERSION="$(sed -n 's/^CHART_VERSION := //p' Makefile)"
CHART="$(sed -n 's/^CHART := //p' Makefile)"

echo "== Сборка манифестов"
for dir in manifests/overlays/* manifests/monitoring manifests/load; do
  kubectl kustomize "$dir" > "$WORK/$(basename "$dir").yaml"
  echo "ok $dir"
done

echo "== Схемы Kubernetes"
for file in "$WORK"/*.yaml manifests/namespaces.yaml manifests/storage/*.yaml manifests/policies/*.yaml; do
  cat "$file"
  echo '---'
done > "$WORK/all.txt"
docker run --rm -i ghcr.io/yannh/kubeconform:v0.7.0 -summary -strict -kubernetes-version 1.34.0 -output text \
  -schema-location default \
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
  - < "$WORK/all.txt"

echo "== Значения чарта kube-prometheus-stack"
KUBECONFIG=/dev/null HELM_PLUGINS="$WORK/plugins" helm template kps "$CHART" --version "$CHART_VERSION" -n monitoring \
  -f manifests/monitoring/kube-prometheus-values.yaml --kube-version 1.34.0 2>/dev/null > "$WORK/kps.txt"
grep -q 'name: kps-prometheus$' "$WORK/kps.txt"
echo "ok, сервис kps-prometheus на месте"

echo "== Recording rules"
mkdir -p tests/rules/generated
python3 tests/smoke/extract_rules.py manifests/monitoring/rules.yaml tests/rules/generated/rules.yaml
docker run --rm -v "$PWD/tests/rules:/work:ro" -w /work --entrypoint promtool "$PROMETHEUS_IMAGE" check rules generated/rules.yaml
docker run --rm -v "$PWD/tests/rules:/work:ro" -w /work --entrypoint promtool "$PROMETHEUS_IMAGE" test rules rules_test.yaml

echo "== Скрипты"
docker run --rm -v "$PWD:/work:ro" -w /work koalaman/shellcheck:stable scripts/*.sh tests/*.sh tests/smoke/*.sh
echo ok

echo "== Метрики и документация"
python3 tests/check_docs.py
