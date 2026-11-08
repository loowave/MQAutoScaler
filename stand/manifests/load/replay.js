// Генератор нагрузки: воспроизводит профиль интенсивности в режиме open-loop.
//
// Запросы отправляются по расписанию, даже если система тормозит. Иначе при деградации генератор
// сам снизил бы нагрузку и скрыл узкое место. Если виртуальных пользователей не хватает, недоданная
// нагрузка видна в k6_dropped_iterations_total.
//
// Сценарии: events публикует события во вход платформы, reads читает витрину.
// Параметры задаются через env:
//   PROFILE      путь к CSV с колонками t_offset_s,rps (см. tools/make_profile.py)
//   SCALE        множитель интенсивности: профиль описывает форму, SCALE задаёт уровень
//   READ_SHARE   интенсивность чтений как доля от потока событий
//   DEBUG_SHARE  доля событий, которые отбрасывает правило фильтра
//   DUP_SHARE    доля повторно отправленных событий
import http from 'k6/http';

const PROFILE = __ENV.PROFILE || '/profile/profile.csv';
const INGEST_URL = __ENV.INGEST_URL || 'http://ingest.platform.svc.cluster.local:8080';
const SHOWCASE_URL = __ENV.SHOWCASE_URL || 'http://showcase.platform.svc.cluster.local:8080';
const SCALE = Number(__ENV.SCALE || 1);
const READ_SHARE = Number(__ENV.READ_SHARE || 0.2);
const DEBUG_SHARE = Number(__ENV.DEBUG_SHARE || 0.1);
const DUP_SHARE = Number(__ENV.DUP_SHARE || 0.01);
const N_TENANTS = Number(__ENV.N_TENANTS || 10000);
const MAX_VUS = Number(__ENV.MAX_VUS || 2000);

const rows = open(PROFILE).trim().split('\n').slice(1).map((line) => line.split(',').map(Number));
const step = rows.length > 1 ? rows[1][0] - rows[0][0] : 10;

function scenario(exec, share) {
  const rate = (rps) => Math.max(0, Math.round(rps * SCALE * share));
  return {
    executor: 'ramping-arrival-rate',
    exec,
    timeUnit: '1s',
    startRate: rate(rows[0][1]),
    preAllocatedVUs: 50,
    maxVUs: MAX_VUS,
    stages: rows.map(([, rps]) => ({ target: rate(rps), duration: `${step}s` })),
  };
}

export const options = {
  discardResponseBodies: true,
  scenarios: {
    events: scenario('publish', 1),
    reads: scenario('read', READ_SHARE),
  },
};

// Арендаторы неравномерны: небольшая часть создаёт большую часть потока. От этого зависят
// доля попаданий в кеш и конкуренция за блокировки строк в PostgreSQL.
function skewedTenant() {
  return Math.min(N_TENANTS - 1, Math.floor(Math.pow(Math.random(), 3) * N_TENANTS));
}

// Размер сообщения: в основном около 512 байт, изредка до нескольких килобайт.
function payload() {
  const size = Math.min(8192, Math.round(Math.exp(6.2 + 0.6 * (Math.random() + Math.random() - 1) * 2)));
  return 'x'.repeat(size);
}

const headers = { 'Content-Type': 'application/json' };
let lastId = null;

export function publish() {
  const duplicate = lastId !== null && Math.random() < DUP_SHARE;
  const id = duplicate ? lastId : `${__VU}-${__ITER}-${Date.now()}`;
  lastId = id;
  const body = JSON.stringify({
    id,
    tenant: skewedTenant(),
    type: Math.random() < DEBUG_SHARE ? 'debug' : 'business',
    payload: payload(),
  });
  http.post(`${INGEST_URL}/events`, body, { headers, tags: { name: 'publish' }, timeout: '10s' });
}

export function read() {
  http.get(`${SHOWCASE_URL}/showcase/${skewedTenant()}`, { tags: { name: 'showcase' }, timeout: '10s' });
}
