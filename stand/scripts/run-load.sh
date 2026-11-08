#!/usr/bin/env bash
# Запускает генератор нагрузки.
#   scripts/run-load.sh <профиль.csv> <run_id> [SCALE]
# SCALE задаёт уровень нагрузки: профили нормированы на пик 100, поэтому SCALE=3 даёт пик 300 RPS.
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Использование: $0 <профиль.csv> <run_id> [SCALE]" >&2
  exit 2
fi
PROFILE="$1" RUN_ID="$2" SCALE="${3:-1}"
STAND_DIR="$(cd "$(dirname "$0")/.." && pwd)"

kubectl -n load delete job k6-replay --ignore-not-found --wait=true
kubectl -n load create configmap k6-profile --from-file="profile.csv=$PROFILE" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl kustomize "$STAND_DIR/manifests/load" \
  | sed -e "s/__RUN_ID__/\"$RUN_ID\"/" -e "s/__SCALE__/\"$SCALE\"/" \
  | kubectl apply -f -

echo "Запуск $RUN_ID: профиль $PROFILE, SCALE=$SCALE. Журнал: kubectl -n load logs -f job/k6-replay"
