# Развёртывание на машинах

Что и сколько нужно — в [resources.md](resources.md). Команды `make` выполняются из каталога `stand/` на своей машине,
команды установки k3s — на самих машинах.

На своей машине нужны `kubectl`, `helm` версии 3.12 или новее, `docker`, `make`, `ssh` и `python3`.

## 1. Подготовить машины

На машине `data` заранее смонтировать отдельный SSD в `/var/lib/rancher/k3s/storage`: там будут данные брокера и баз.

```bash
sudo mkfs.ext4 /dev/<диск>
sudo mkdir -p /var/lib/rancher/k3s/storage
echo '/dev/<диск> /var/lib/rancher/k3s/storage ext4 defaults,noatime 0 2' | sudo tee -a /etc/fstab
sudo mount -a
```

На всех машинах отключить swap: `sudo swapoff -a`, и убрать его из `/etc/fstab`. Иначе нехватка памяти будет выглядеть как замедление диска.

## 2. Установить k3s

Скопировать каталог `stand/scripts` на машины или склонировать репозиторий, затем:

```bash
# на infra: скрипт напечатает адрес сервера и токен
sudo scripts/k3s-server.sh infra

# на остальных машинах
sudo scripts/k3s-agent.sh app  https://<infra>:6443 <токен>
sudo scripts/k3s-agent.sh data https://<infra>:6443 <токен>
sudo scripts/k3s-agent.sh load https://<infra>:6443 <токен>
```

Роли можно совмещать, например `sudo scripts/k3s-agent.sh app,data ...` для компактного варианта.

Версия k3s зафиксирована в скриптах, версии чартов — в `Makefile`, версии образов — в манифестах. Так запуски остаются сравнимыми между собой.

## 3. Настроить доступ с своей машины

Скопировать `/etc/rancher/k3s/k3s.yaml` с `infra` на свою машину и заменить `127.0.0.1` на адрес `infra`.
Использовать **отдельный файл**, не смешивая с рабочими кластерами:

```bash
export KUBECONFIG=~/.kube/mqautoscaler.yaml
make check
```

`make check` убеждается, что в кластере есть ноды всех ролей. Все команды `make`, меняющие кластер, выполняют эту проверку первыми
и не применят манифесты к чужому кластеру.

## 4. Базовые объекты и мониторинг

```bash
make base         # namespace, StorageClass, случайные пароли в Secret
make monitoring   # Prometheus, Grafana, правила mq:* и дашборд
```

Пароли создаются при установке, в репозитории их нет.

## 5. Платформа

Образ сервисов собирается локально и загружается на ноду `app`. В манифестах `imagePullPolicy: Never`, поэтому без этого шага поды не запустятся.

```bash
scripts/import-image.sh <пользователь>@<app>
make platform
```

Команда дождётся готовности брокера, баз и сервисов. Проверка, что события доходят до витрины:

```bash
kubectl -n platform port-forward svc/ingest 8080 &
curl -X POST localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"id": "check-1", "tenant": 1, "type": "business"}'
kubectl -n platform port-forward svc/showcase 8081:8080 &
sleep 5 && curl localhost:8081/showcase/1
```

В ответе витрины должно быть `check-1`. Кеш витрины живёт 5 секунд.

## 6. Grafana и Prometheus

```bash
kubectl -n monitoring port-forward svc/kps-grafana 3000:80 &
kubectl -n monitoring get secret grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d; echo
```

Grafana откроется на `localhost:3000`, логин `admin`. Основной дашборд называется **MQAutoScaler**.

```bash
kubectl -n monitoring port-forward svc/kps-prometheus 9090 &
```

В Prometheus на странице Status → Target health все цели `mqautoscaler` должны быть в состоянии UP.

## 7. Первый замер

```bash
make load PROFILE=ramp RUN_ID=capacity-baseline SCALE=4
kubectl -n load logs -f job/k6-replay
```

Смотреть на панель «Загрузка ρ» на дашборде: поток, при котором ρ доходит до 1, и есть ёмкость. От него выбираются `SCALE` для остальных профилей.
Дальше — [experiments.md](experiments.md).

## Что проверить после первого запуска

Часть проверок можно выполнить только в настоящем кластере:

- Ряды ресурсов и нод из разделов 6–8 в [metrics.md](metrics.md) содержат данные.
- `mq:node_pressure_*` не пустые. Если пустые, в ядре выключен PSI: нужно добавить `psi=1` в параметры загрузки.
- `kubectl get pods -A -o wide`: мониторинг работает на `infra`, база и брокер на `data`, сервисы на `app`.
- Брокер записывает данные на том: в журнале пода `kafka` нет ошибок доступа к `/var/lib/kafka/data`. Контейнер работает от uid 1000, а local-path создаёт каталоги с правами 0777.
- Сбои Chaos Mesh применяются: `kubectl describe` объекта показывает, что он внедрён. `IOChaos` чувствителен к среде, его стоит проверить отдельно.
- `kafka_broker_request_handler_idle_ratio` близко к 2 в простое: брокер и контроллер работают в одном процессе, и счётчик суммирует оба пула. Если близко к 1, в правиле `mq:kafka_request_handler_idle:ratio` не нужно вычитать единицу.

## Сброс и удаление

```bash
make reset OVERLAY=baseline                # удалить брокер, базы и сервисы вместе с данными
helm -n monitoring uninstall kps           # удалить мониторинг
sudo /usr/local/bin/k3s-uninstall.sh       # на infra: удалить k3s
sudo /usr/local/bin/k3s-agent-uninstall.sh # на остальных машинах
```
