#!/usr/bin/env bash
# Собирает образ сервисов и загружает его в containerd ноды app по ssh.
# Манифесты используют imagePullPolicy: Never, поэтому образ должен быть импортирован заранее.
#   scripts/import-image.sh <пользователь@нода-app>
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Использование: $0 <пользователь@нода-app>" >&2
  exit 2
fi
HOST="$1"
IMAGE="${IMAGE:-mqautoscaler/platform:0.1.0}"
STAND_DIR="$(cd "$(dirname "$0")/.." && pwd)"

docker build -t "$IMAGE" "$STAND_DIR/app"
docker save "$IMAGE" | ssh "$HOST" sudo k3s ctr images import -
