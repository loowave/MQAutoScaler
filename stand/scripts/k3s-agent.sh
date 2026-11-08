#!/usr/bin/env bash
# Подключает машину к кластеру k3s. Запускать на самой машине от root:
#   scripts/k3s-agent.sh <роли через запятую> https://<infra>:6443 <токен>
# Роли: infra, app, data, load. Например: scripts/k3s-agent.sh app,data https://10.0.0.1:6443 <токен>
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Использование: $0 <роли через запятую> https://<infra>:6443 <токен>" >&2
  exit 2
fi
ROLES="$1" SERVER="$2" TOKEN="$3"
K3S_VERSION="${K3S_VERSION:-v1.34.13+k3s1}"

labels=()
IFS=',' read -ra role_list <<< "$ROLES"
for role in "${role_list[@]}"; do
  case "$role" in
    infra|app|data|load) labels+=(--node-label "mqautoscaler/$role=true") ;;
    *) echo "Неизвестная роль: $role" >&2; exit 2 ;;
  esac
done

curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$K3S_VERSION" K3S_URL="$SERVER" K3S_TOKEN="$TOKEN" sh -s - agent \
  "${labels[@]}" \
  --kubelet-arg=system-reserved=cpu=500m,memory=1Gi
