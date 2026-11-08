#!/usr/bin/env bash
# Создаёт случайные пароли стенда. Манифесты их не содержат, потому что репозиторий публичный.
# Повторный запуск существующие секреты не меняет.
set -euo pipefail

secret_value() {
  kubectl -n "$1" get secret "$2" -o "jsonpath={.data.$3}" 2>/dev/null | base64 -d
}

PG_PASSWORD="$(secret_value data postgres password || true)"
PG_PASSWORD="${PG_PASSWORD:-$(openssl rand -hex 16)}"

for ns in data platform; do
  kubectl -n "$ns" create secret generic postgres \
    --from-literal="password=$PG_PASSWORD" --dry-run=client -o yaml | kubectl apply -f -
done

if ! kubectl -n monitoring get secret grafana-admin >/dev/null 2>&1; then
  kubectl -n monitoring create secret generic grafana-admin \
    --from-literal=admin-user=admin --from-literal="admin-password=$(openssl rand -hex 16)"
fi
