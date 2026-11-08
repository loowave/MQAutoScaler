#!/usr/bin/env bash
# Устанавливает control plane k3s на машину infra. Запускать на самой машине от root.
#   scripts/k3s-server.sh [роли через запятую, по умолчанию infra]
# В конце печатает адрес и токен, которые нужны для scripts/k3s-agent.sh.
set -euo pipefail

ROLES="${1:-infra}"
K3S_VERSION="${K3S_VERSION:-v1.34.13+k3s1}"

labels=()
IFS=',' read -ra role_list <<< "$ROLES"
for role in "${role_list[@]}"; do
  case "$role" in
    infra|app|data|load) labels+=(--node-label "mqautoscaler/$role=true") ;;
    *) echo "Неизвестная роль: $role" >&2; exit 2 ;;
  esac
done

curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$K3S_VERSION" sh -s - server \
  "${labels[@]}" \
  --disable traefik \
  --disable servicelb \
  --kubelet-arg=system-reserved=cpu=500m,memory=1Gi \
  --write-kubeconfig-mode 600

echo
echo "Сервер: https://$(hostname -I | awk '{print $1}'):6443"
echo "Токен:  $(cat /var/lib/rancher/k3s/server/node-token)"
echo "Kubeconfig: /etc/rancher/k3s/k3s.yaml"
