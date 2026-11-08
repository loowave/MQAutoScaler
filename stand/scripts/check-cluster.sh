#!/usr/bin/env bash
# Защита от применения манифестов не в тот кластер: в текущем контексте kubectl должны быть
# ноды стенда со всеми четырьмя ролями. Роли могут совмещаться на одной ноде.
# Вызывается перед каждым изменением через Makefile.
set -euo pipefail

CONTEXT="$(kubectl config current-context 2>/dev/null || echo 'не задан')"
for role in infra app data load; do
  if [ -z "$(kubectl get nodes -l "mqautoscaler/$role=true" -o name 2>/dev/null)" ]; then
    echo "Контекст kubectl '$CONTEXT': нет ноды с меткой mqautoscaler/$role=true." >&2
    echo "Это не кластер стенда либо он ещё не размечен. Изменения не применены." >&2
    exit 1
  fi
done
echo "Кластер стенда: контекст '$CONTEXT'"
