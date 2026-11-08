# Управление стендом. Каждая команда, меняющая кластер, сначала проверяет,
# что текущий контекст kubectl указывает на кластер стенда (scripts/check-cluster.sh).

SHELL := /bin/bash
.DEFAULT_GOAL := help

OVERLAY ?= baseline
CHART := oci://ghcr.io/prometheus-community/charts/kube-prometheus-stack
CHART_VERSION := 93.1.0
CHAOS_MESH_VERSION := 2.8.8
KEDA_VERSION := 2.21.0
CHAOS_SOCKET := /run/k3s/containerd/containerd.sock

.PHONY: help check base monitoring platform wait load reset chaos-mesh keda render lint smoke

help: ## Показать список команд
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

check: ## Проверить, что kubectl смотрит на кластер стенда
	@scripts/check-cluster.sh

base: check ## Создать namespace, StorageClass и пароли
	kubectl apply -f manifests/namespaces.yaml -f manifests/storage/storageclass.yaml
	scripts/create-secrets.sh

monitoring: check ## Установить Prometheus и Grafana, применить правила и дашборд
	helm upgrade --install kps $(CHART) --version $(CHART_VERSION) -n monitoring \
	  -f manifests/monitoring/kube-prometheus-values.yaml --wait --timeout 10m
	kubectl apply -k manifests/monitoring

platform: check ## Развернуть брокер, базы и сервисы: make platform OVERLAY=tight-kafka
	kubectl apply -k manifests/overlays/$(OVERLAY)
	$(MAKE) wait

wait: ## Дождаться готовности брокера, баз и сервисов
	kubectl -n data wait --for=condition=complete job/kafka-topics --timeout=300s
	kubectl -n data wait --for=condition=ready pod -l app.kubernetes.io/component --timeout=300s
	kubectl -n platform wait --for=condition=ready pod -l app.kubernetes.io/component --timeout=300s

load: check ## Запустить нагрузку: make load PROFILE=ramp RUN_ID=run-001 SCALE=3
	@test -n "$(PROFILE)" && test -n "$(RUN_ID)" || { echo "Нужны PROFILE и RUN_ID"; exit 2; }
	scripts/run-load.sh manifests/load/profiles/$(PROFILE).csv $(RUN_ID) $(or $(SCALE),1)

reset: check ## Удалить брокер, базы и сервисы вместе с данными
	-kubectl delete -f manifests/policies/ --ignore-not-found
	kubectl delete -k manifests/overlays/$(OVERLAY) --ignore-not-found --wait=true
	kubectl -n data delete pvc --all --wait=true

chaos-mesh: check ## Установить Chaos Mesh для внесения сбоев
	helm upgrade --install chaos-mesh chaos-mesh --repo https://charts.chaos-mesh.org \
	  --version $(CHAOS_MESH_VERSION) -n chaos-mesh --create-namespace --wait --timeout 10m \
	  --set chaosDaemon.runtime=containerd --set chaosDaemon.socketPath=$(CHAOS_SOCKET) \
	  --set-string controllerManager.nodeSelector.mqautoscaler/infra=true \
	  --set dashboard.create=false

keda: check ## Установить KEDA для базовой политики масштабирования по lag
	helm upgrade --install keda keda --repo https://kedacore.github.io/charts \
	  --version $(KEDA_VERSION) -n keda --create-namespace --wait --timeout 10m \
	  --set-string nodeSelector.mqautoscaler/infra=true

render: ## Собрать манифесты без применения
	@kubectl kustomize manifests/overlays/$(OVERLAY)

lint: ## Проверить манифесты, правила и скрипты без кластера
	tests/check-manifests.sh

smoke: ## Поднять платформу в Docker Compose и проверить её под нагрузкой
	tests/smoke/run.sh
