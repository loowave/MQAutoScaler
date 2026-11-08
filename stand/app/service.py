"""Сервисы событийной платформы стенда.

Одна кодовая база, роль задаётся переменной ROLE:

  ingest     HTTP-вход: принимает события и пишет их в топик events-raw
  filter     модуль фильтрации: отбрасывает лишние типы и дубли (Redis), пишет в events-filtered
  enricher   первая задача воркфлоу: профиль арендатора (Redis → MongoDB), пишет в events-enriched
  processor  вторая задача воркфлоу: внешний вызов и запись результата в PostgreSQL
  showcase   витрина: HTTP-чтение результатов из PostgreSQL через кеш Redis

Метрики Prometheus отдаются на порту 9100. Метку сервиса код не пишет: её добавляет
Prometheus из имени Kubernetes Service (label service).
"""
import asyncio
import json
import os
import random
import signal
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import redis.asyncio as aioredis
import uvicorn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from aiokafka.errors import KafkaError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, Histogram, disable_created_metrics, start_http_server
from pydantic import BaseModel
from pymongo import AsyncMongoClient
from pymongo.errors import BulkWriteError

ROLE = os.environ["ROLE"]

# Поведение компонента: ресурсоёмкость и размеры пулов.
WORK_MS = float(os.getenv("WORK_MS", "2"))                  # CPU-время на запрос или сообщение
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))             # сообщений одной реплики за раз
PG_POOL = int(os.getenv("PG_POOL", "10"))
PG_ACQUIRE_TIMEOUT_S = float(os.getenv("PG_ACQUIRE_TIMEOUT_S", "5"))
PRODUCE_TIMEOUT_S = float(os.getenv("PRODUCE_TIMEOUT_S", "5"))
CACHE_TTL_S = int(os.getenv("CACHE_TTL_S", "30"))
DEDUP_TTL_S = int(os.getenv("DEDUP_TTL_S", "300"))
DROP_TYPES = set(filter(None, os.getenv("DROP_TYPES", "debug").split(",")))
N_TENANTS = int(os.getenv("N_TENANTS", "10000"))
EXTERNAL_LATENCY_MS = float(os.getenv("EXTERNAL_LATENCY_MS", "50"))  # среднее, экспоненциальное
EXTERNAL_FAIL_RATE = float(os.getenv("EXTERNAL_FAIL_RATE", "0.01"))
SLA_SECONDS = float(os.getenv("SLA_SECONDS", "30"))
HANDLER_RETRIES = int(os.getenv("HANDLER_RETRIES", "3"))

IN_TOPIC = os.getenv("IN_TOPIC", "")
OUT_TOPIC = os.getenv("OUT_TOPIC", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongodb:27017")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
PG_DSN = os.getenv("PG_DSN", "")

disable_created_metrics()

FAST_BUCKETS = (.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10)
SLOW_BUCKETS = (.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800)

# --- Технические метрики ------------------------------------------------------
HTTP_REQUESTS = Counter("http_requests_total", "Входящие HTTP-запросы", ["route", "method", "code"])
HTTP_LATENCY = Histogram("http_request_duration_seconds", "Время обработки HTTP-запроса",
                         ["route"], buckets=FAST_BUCKETS)
HTTP_INFLIGHT = Gauge("http_requests_in_flight", "HTTP-запросы в обработке")

CONSUMED = Counter("consumer_messages_total", "Сообщения, разобранные консьюмером", ["topic", "result"])
PROCESSING = Histogram("consumer_processing_seconds", "Время обработки одного сообщения",
                       ["topic"], buckets=FAST_BUCKETS)
QUEUE_WAIT = Histogram("consumer_wait_seconds", "Время ожидания сообщения в брокере",
                       ["topic"], buckets=SLOW_BUCKETS)
BUSY = Counter("consumer_busy_seconds_total", "Время, занятое обработкой, без ожидания новых сообщений",
               ["topic"])
EVICTED = Counter("consumer_evicted_messages_total", "Сообщения, вытесненные из брокера до чтения",
                  ["topic"])
REBALANCES = Counter("consumer_rebalances_total", "Перераспределения партиций", ["topic"])
PARTITIONS = Gauge("consumer_assigned_partitions", "Партиции, назначенные реплике", ["topic"])

PRODUCED = Counter("producer_messages_total", "Сообщения, отправленные в брокер", ["topic", "result"])
PRODUCE_LATENCY = Histogram("producer_send_seconds", "Подтверждение записи брокером",
                            ["topic"], buckets=FAST_BUCKETS)

DB_LATENCY = Histogram("db_operation_duration_seconds", "Время операции с хранилищем",
                       ["db", "op"], buckets=FAST_BUCKETS)
DB_POOL_WAIT = Histogram("db_pool_acquire_seconds", "Ожидание соединения из пула",
                         ["db"], buckets=FAST_BUCKETS)
CACHE_REQUESTS = Counter("cache_requests_total", "Обращения к кешу", ["result"])
EXTERNAL_LATENCY = Histogram("external_call_duration_seconds", "Время внешнего вызова",
                             buckets=FAST_BUCKETS)

# --- Бизнес-метрики -----------------------------------------------------------
EVENTS_RECEIVED = Counter("platform_events_received_total", "События, принятые платформой")
EVENTS_REJECTED = Counter("platform_events_rejected_total", "События, не принятые из-за сбоя", ["reason"])
EVENTS_FILTERED = Counter("platform_events_filtered_total", "События, отброшенные фильтром", ["reason"])
EVENTS_PROCESSED = Counter("platform_events_processed_total", "События, дошедшие до витрины", ["status"])
EVENTS_LOST = Counter("platform_events_lost_total", "События, потерянные после исчерпания повторов",
                      ["stage"])
EVENTS_SLA = Counter("platform_events_sla_total", "Выполнение SLA по времени обработки", ["result"])
EVENT_E2E = Histogram("platform_event_end_to_end_seconds", "От приёма события до появления на витрине",
                      buckets=SLOW_BUCKETS)
SHOWCASE_READS = Counter("platform_showcase_reads_total", "Чтения витрины", ["result"])
SHOWCASE_FRESHNESS = Gauge("platform_showcase_freshness_seconds", "Возраст самого свежего события на витрине")


def init_series() -> None:
    """Создаёт ряды с нулями заранее: rate() видит первый рост, а не появление нового ряда."""
    if ROLE in ("filter", "enricher", "processor"):
        BUSY.labels(IN_TOPIC)
        EVICTED.labels(IN_TOPIC)
        REBALANCES.labels(IN_TOPIC)
        EVENTS_LOST.labels(ROLE)
        for result in ("ok", "error"):
            CONSUMED.labels(IN_TOPIC, result)
    if OUT_TOPIC:
        for result in ("ok", "error"):
            PRODUCED.labels(OUT_TOPIC, result)
    if ROLE == "ingest":
        EVENTS_REJECTED.labels("broker")
    if ROLE == "filter":
        for reason in ("rule", "duplicate"):
            EVENTS_FILTERED.labels(reason)
    if ROLE == "processor":
        for status in ("ok", "failed"):
            EVENTS_PROCESSED.labels(status)
        for result in ("met", "breached"):
            EVENTS_SLA.labels(result)
    if ROLE == "showcase":
        for result in ("ok", "error"):
            SHOWCASE_READS.labels(result)
    if ROLE in ("enricher", "showcase"):
        for result in ("hit", "miss"):
            CACHE_REQUESTS.labels(result)


class Clients:
    """Подключения процесса к зависимостям."""
    mongo = None
    redis = None
    pg: Optional[asyncpg.Pool] = None
    producer: Optional[AIOKafkaProducer] = None


def burn_cpu(ms: float) -> None:
    """Тратит именно CPU-время: при throttling растёт задержка, а объём работы не меняется."""
    end = time.thread_time() + ms / 1000
    while time.thread_time() < end:
        pass


async def retry(connect, attempts: int = 60, delay: float = 2.0):
    """Ждёт зависимость: при старте стенда порядок готовности подов не гарантирован."""
    for attempt in range(attempts):
        try:
            return await connect()
        except Exception:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(delay)


@asynccontextmanager
async def timed(db: str, op: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        DB_LATENCY.labels(db, op).observe(time.perf_counter() - start)


@asynccontextmanager
async def pg_connection():
    start = time.perf_counter()
    async with Clients.pg.acquire(timeout=PG_ACQUIRE_TIMEOUT_S) as conn:
        DB_POOL_WAIT.labels("postgres").observe(time.perf_counter() - start)
        yield conn


async def connect_pg() -> None:
    Clients.pg = await retry(lambda: asyncpg.create_pool(PG_DSN, min_size=2, max_size=PG_POOL))


async def connect_producer() -> None:
    Clients.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, acks="all", linger_ms=5)
    await retry(Clients.producer.start)


async def publish(topic: str, key: str, event: dict) -> None:
    start = time.perf_counter()
    try:
        await asyncio.wait_for(
            Clients.producer.send_and_wait(topic, json.dumps(event).encode(), key=key.encode()),
            PRODUCE_TIMEOUT_S)
    except Exception:
        PRODUCED.labels(topic, "error").inc()
        raise
    PRODUCE_LATENCY.labels(topic).observe(time.perf_counter() - start)
    PRODUCED.labels(topic, "ok").inc()


async def cached_json(key: str, ttl: int, load):
    """Чтение через кеш: при промахе данные берутся из load() и кладутся в Redis."""
    async with timed("redis", "get"):
        raw = await Clients.redis.get(key)
    if raw is not None:
        CACHE_REQUESTS.labels("hit").inc()
        return json.loads(raw)
    CACHE_REQUESTS.labels("miss").inc()
    value = await load()
    if value is not None:
        async with timed("redis", "set"):
            await Clients.redis.set(key, json.dumps(value), ex=ttl)
    return value


# --- filter -------------------------------------------------------------------
async def handle_filter(event: dict, delivery: str) -> None:
    burn_cpu(WORK_MS)
    if event["type"] in DROP_TYPES:
        EVENTS_FILTERED.labels("rule").inc()
        return
    key = f"dedup:{event['id']}"
    async with timed("redis", "set_nx"):
        first_seen = await Clients.redis.set(key, delivery, nx=True, ex=DEDUP_TTL_S)
    if not first_seen:
        async with timed("redis", "get"):
            origin = await Clients.redis.get(key)
        # Повторная доставка того же сообщения после сбоя — не дубль: его нужно передать дальше.
        if origin is not None and origin.decode() != delivery:
            EVENTS_FILTERED.labels("duplicate").inc()
            return
    await publish(OUT_TOPIC, event["id"], event)


# --- enricher -----------------------------------------------------------------
async def seed_tenants() -> None:
    tenants = Clients.mongo.platform.tenants
    if await tenants.estimated_document_count() >= N_TENANTS:
        return
    rnd = random.Random(42)
    docs = [{"_id": i, "segment": rnd.choice(["retail", "business", "partner"]), "limit": rnd.randint(100, 10000)}
            for i in range(N_TENANTS)]
    try:
        await tenants.insert_many(docs, ordered=False)
    except BulkWriteError:
        pass  # соседняя реплика уже записала часть документов


async def load_tenant(tenant: int):
    async with timed("mongodb", "find_one"):
        return await Clients.mongo.platform.tenants.find_one({"_id": tenant})


async def handle_enrich(event: dict, _delivery: str) -> None:
    burn_cpu(WORK_MS)
    tenant = event["tenant"]
    profile = await cached_json(f"tenant:{tenant}", CACHE_TTL_S, lambda: load_tenant(tenant))
    event["segment"] = profile["segment"] if profile else "unknown"
    await publish(OUT_TOPIC, event["id"], event)


# --- processor ----------------------------------------------------------------
async def handle_process(event: dict, _delivery: str) -> None:
    burn_cpu(WORK_MS)
    start = time.perf_counter()
    await asyncio.sleep(random.expovariate(1000 / EXTERNAL_LATENCY_MS))
    EXTERNAL_LATENCY.observe(time.perf_counter() - start)
    status = "failed" if random.random() < EXTERNAL_FAIL_RATE else "ok"

    received_at = datetime.fromtimestamp(event["received_ts"], timezone.utc)
    async with pg_connection() as conn:
        async with timed("postgres", "write_result"), conn.transaction():
            inserted = await conn.fetchrow(
                """INSERT INTO events (id, tenant, type, segment, status, received_at)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   ON CONFLICT (id) DO NOTHING RETURNING id""",
                event["id"], event["tenant"], event["type"], event["segment"], status, received_at)
            if inserted is None:
                return  # повторная доставка после падения реплики: результат уже записан
            await conn.execute(
                """INSERT INTO tenant_stats AS s (tenant, processed, failed, last_event_at)
                   VALUES ($1, 1, $2, now())
                   ON CONFLICT (tenant) DO UPDATE
                   SET processed = s.processed + 1, failed = s.failed + $2, last_event_at = now()""",
                event["tenant"], int(status == "failed"))

    elapsed = time.time() - event["received_ts"]
    EVENTS_PROCESSED.labels(status).inc()
    EVENT_E2E.observe(elapsed)
    EVENTS_SLA.labels("met" if elapsed <= SLA_SECONDS else "breached").inc()


# --- цикл консьюмера ----------------------------------------------------------
class RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer: AIOKafkaConsumer, offsets: dict):
        self.consumer = consumer
        self.offsets = offsets

    async def on_partitions_revoked(self, revoked):
        self.offsets.clear()

    async def on_partitions_assigned(self, assigned):
        REBALANCES.labels(IN_TOPIC).inc()
        PARTITIONS.labels(IN_TOPIC).set(len(assigned))
        for partition in assigned:
            committed = await self.consumer.committed(partition)
            if committed is not None:  # разрыв виден и после перезапуска реплики
                self.offsets[partition.partition] = committed - 1


async def handle_with_retries(handler, message) -> None:
    """Сбой зависимости не теряет событие сразу: сообщение повторяется с паузой."""
    start = time.perf_counter()
    QUEUE_WAIT.labels(IN_TOPIC).observe(max(0.0, time.time() - message.timestamp / 1000))
    event = json.loads(message.value)
    for attempt in range(HANDLER_RETRIES):
        try:
            await handler(event, f"{message.partition}:{message.offset}")
            CONSUMED.labels(IN_TOPIC, "ok").inc()
            break
        except Exception:
            if attempt == HANDLER_RETRIES - 1:
                CONSUMED.labels(IN_TOPIC, "error").inc()
                EVENTS_LOST.labels(ROLE).inc()
            else:
                await asyncio.sleep(0.2 * 2 ** attempt)
    PROCESSING.labels(IN_TOPIC).observe(time.perf_counter() - start)


def count_evicted(offsets: dict, messages: list) -> None:
    """Смещения в топике идут подряд: разрыв значит, что сообщения удалены до чтения."""
    for message in messages:
        previous = offsets.get(message.partition)
        if previous is not None and message.offset > previous + 1:
            EVICTED.labels(IN_TOPIC).inc(message.offset - previous - 1)
        offsets[message.partition] = message.offset


async def run_consumer(handler, stop: asyncio.Event) -> None:
    offsets: dict = {}
    consumer = AIOKafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, group_id=ROLE,
                                enable_auto_commit=False, auto_offset_reset="earliest",
                                metadata_max_age_ms=10000)
    await retry(consumer.start)
    consumer.subscribe([IN_TOPIC], listener=RebalanceListener(consumer, offsets))
    try:
        while not stop.is_set():
            batches = await consumer.getmany(timeout_ms=500, max_records=BATCH_SIZE)
            messages = [message for part in batches.values() for message in part]
            if not messages:
                continue
            start = time.perf_counter()
            count_evicted(offsets, messages)
            await asyncio.gather(*(handle_with_retries(handler, m) for m in messages))
            try:
                # Фиксируем только обработанное: позиция непрочитанной партиции могла сдвинуться
                # из-за вытеснения, и тогда следующая реплика не увидела бы разрыв.
                await consumer.commit({partition: part[-1].offset + 1 for partition, part in batches.items() if part})
            except KafkaError:
                pass  # партиции ушли другой реплике: она повторит сообщения, а запись результата идемпотентна
            BUSY.labels(IN_TOPIC).inc(time.perf_counter() - start)
    finally:
        await consumer.stop()  # уходит из группы сразу, не дожидаясь session timeout


async def consumer_main() -> None:
    handlers = {"filter": handle_filter, "enricher": handle_enrich, "processor": handle_process}
    if ROLE in ("filter", "enricher"):
        Clients.redis = aioredis.from_url(REDIS_URL, max_connections=BATCH_SIZE * 2)
    if ROLE == "enricher":
        Clients.mongo = AsyncMongoClient(MONGO_URL)
        await retry(seed_tenants)
    if ROLE == "processor":
        await connect_pg()
    else:
        await connect_producer()

    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    try:
        await run_consumer(handlers[ROLE], stop)
    finally:
        if Clients.producer:
            await Clients.producer.stop()
        if Clients.pg:
            await Clients.pg.close()


# --- HTTP-сервисы -------------------------------------------------------------
class EventIn(BaseModel):
    id: str
    tenant: int
    type: str
    payload: str = ""


async def refresh_freshness() -> None:
    while True:
        try:
            async with pg_connection() as conn:
                age = await conn.fetchval(
                    "SELECT EXTRACT(EPOCH FROM now() - max(last_event_at))::float8 FROM tenant_stats")
            if age is not None:
                SHOWCASE_FRESHNESS.set(age)
        except Exception:
            pass  # витрина недоступна: это видно по ошибкам чтения, gauge остаётся прежним
        await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    freshness = None
    if ROLE == "ingest":
        await connect_producer()
    else:
        Clients.redis = aioredis.from_url(REDIS_URL, max_connections=200)
        await connect_pg()
        freshness = asyncio.create_task(refresh_freshness())
    yield
    if freshness:
        freshness.cancel()
    if Clients.producer:
        await Clients.producer.stop()
    if Clients.redis:
        await Clients.redis.aclose()
    if Clients.pg:
        await Clients.pg.close()


app = FastAPI(lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


if ROLE == "ingest":
    @app.post("/events", status_code=202)
    async def receive_event(event: EventIn):
        burn_cpu(WORK_MS)
        body = event.model_dump()
        body["received_ts"] = time.time()
        try:
            await publish(OUT_TOPIC, event.id, body)
        except Exception:
            EVENTS_REJECTED.labels("broker").inc()
            return JSONResponse({"error": "broker unavailable"}, status_code=503)
        EVENTS_RECEIVED.inc()
        return {"id": event.id}

if ROLE == "showcase":
    async def load_showcase(tenant: int):
        async with pg_connection() as conn:
            async with timed("postgres", "read_stats"):
                stats = await conn.fetchrow(
                    "SELECT processed, failed FROM tenant_stats WHERE tenant = $1", tenant)
            async with timed("postgres", "read_recent"):
                recent = await conn.fetch(
                    """SELECT id, type, status FROM events WHERE tenant = $1
                       ORDER BY processed_at DESC LIMIT 20""", tenant)
        return {"tenant": tenant,
                "processed": stats["processed"] if stats else 0,
                "failed": stats["failed"] if stats else 0,
                "recent": [dict(row) for row in recent]}

    @app.get("/showcase/{tenant}")
    async def read_showcase(tenant: int):
        burn_cpu(WORK_MS)
        try:
            data = await cached_json(f"showcase:{tenant}", CACHE_TTL_S, lambda: load_showcase(tenant))
        except Exception:
            SHOWCASE_READS.labels("error").inc()
            return JSONResponse({"error": "storage unavailable"}, status_code=503)
        SHOWCASE_READS.labels("ok").inc()
        return data


@app.middleware("http")
async def http_metrics(request: Request, call_next):
    if request.url.path == "/healthz":
        return await call_next(request)
    start = time.perf_counter()
    HTTP_INFLIGHT.inc()
    code = 500
    try:
        response = await call_next(request)
        code = response.status_code
        return response
    finally:
        HTTP_INFLIGHT.dec()
        route = request.scope.get("route")
        path = route.path if route else "unmatched"
        HTTP_REQUESTS.labels(path, request.method, str(code)).inc()
        HTTP_LATENCY.labels(path).observe(time.perf_counter() - start)


if __name__ == "__main__":
    init_series()
    start_http_server(9100)
    if ROLE in ("ingest", "showcase"):
        uvicorn.run(app, host="0.0.0.0", port=8080, log_level="warning", access_log=False)
    else:
        asyncio.run(consumer_main())
