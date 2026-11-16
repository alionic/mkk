# Асинхронные платежи

Python 3.12 / FastAPI / PostgreSQL / FastStream + RabbitMQ. API сохраняет платёж и
Outbox **одной транзакцией**, отвечает `202`; worker эмулирует шлюз и доставляет
подписанный webhook. Это **эмулятор**, не интеграция с банком: задержка 2–5 секунд,
90% `succeeded`, 10% `failed`, результат стабилен по `payment_id`.

```text
client → API → PostgreSQL (payment + Outbox, одна транзакция) → 202
                     Outbox relay → payments exchange → payments.new quorum
                     worker → gateway emulator → terminal commit → signed webhook
                              → delivered | due retry Outbox | exhausted + DLQ Outbox
```

## Запуск

Нужны Docker Engine + Compose v2 с `service_completed_successfully`, либо Python
3.12 и [uv](https://docs.astral.sh/uv/). Проверенная сборка: Python **3.12.16**,
uv **0.13.8**, PostgreSQL **17.7-bookworm**, RabbitMQ **4.2.1-management**.

```bash
# Только если собственного .env ещё нет; существующий файл не перезаписывать!
test -e .env || (umask 077; cp .env.example .env)
chmod 600 .env
# Заполните четыре независимых секрета и домены. Генератор одного секрета:
python3 -c 'import secrets; print(secrets.token_hex(32))'
docker compose --env-file .env -p payments config --quiet
docker compose --env-file .env -p payments build
docker compose --env-file .env -p payments up -d --wait --wait-timeout 240
docker compose --env-file .env -p payments ps
```

По умолчанию `ENVIRONMENT=production`, `ALLOW_UNSAFE_WEBHOOKS=false`. Обязателен
`WEBHOOK_ALLOWED_DOMAINS` — **JSON-массив**, не CSV:

```dotenv
WEBHOOK_ALLOWED_DOMAINS='["example.com", "hooks.partner.example"]'
```

`example.com` разрешает сам домен, `hooks.example.com` и `a.b.example.com`, но не
`badexample.com` и не `example.com.attacker.net`. IP literals запрещены всегда.
В production разрешён только публичный **HTTPS:443** без credentials, fragment,
redirects и environment proxy. Проверяется весь DNS-ответ перед каждой попыткой;
соединение использует проверенные IP с исходным TLS SNI/проверкой сертификата.

`migrate` запускает `alembic upgrade head` один раз перед API/consumer. API зависит
от готовой БД и успешной миграции, **не от RabbitMQ**; consumer — от RabbitMQ и
миграции. Health API проверяет БД; worker-health — свежий (≤30s), атомарный,
привязанный к процессу heartbeat с живыми consumer/relay, не просто `kill -0`.
При рестарте старый heartbeat не считается готовностью. Healthcheck сам по себе
не перезапускает unhealthy container: нужен alert/orchestrator; exit worker
перезапускается политикой `unless-stopped`.

API опубликован **127.0.0.1:8000** (`API_PORT` меняет host port, `0` — случайный).
Postgres, AMQP, management UI и receiver **не имеют host ports**. Named volumes
сохраняют данные. `stop_grace_period=180s` оставляет worker 90s drain и отдельные
cleanup budgets; не уменьшайте его до обычных Docker 10s. Runtime image non-root,
без uv/dev dependencies/tests/docs/.env/.git/agent state; секреты только runtime.

### Явно небезопасная локальная демонстрация (не production)

В **отдельном** приватном env-файле оставьте свежие секреты, но задайте:

```dotenv
ENVIRONMENT=development
ALLOW_UNSAFE_WEBHOOKS=true
WEBHOOK_ALLOWED_DOMAINS='["receiver"]'
WEBHOOK_FAIL_FIRST=0
API_PORT=8000
```

```bash
docker compose --env-file .env.dev -p payments-dev --profile dev config --quiet
docker compose --env-file .env.dev -p payments-dev --profile dev build
docker compose --env-file .env.dev -p payments-dev --profile dev up -d --wait --wait-timeout 240
```

Webhook URL: **`http://receiver:8081/webhook`**, то есть service DNS, не literal IP
и не localhost (localhost внутри worker — сам worker). Receiver opt-in profile
`dev`, без публикации порта; Compose явно задаёт `WEBHOOK_RECEIVER_HOST=0.0.0.0`.
При самостоятельном `uv run python examples/webhook_receiver.py` он по умолчанию
слушает только **127.0.0.1:8081**; нужны `ENVIRONMENT=development` и `WEBHOOK_SECRET`.
Receiver main запрещает production даже с bind override. Production + unsafe
отклоняется Settings, включение `dev` profile само защиту не снимает. Даже в dev
не отключаются allowlist, запрет literal IP, TLS certificate/hostname checks.
`WEBHOOK_FAIL_FIRST=3` эмулирует три ошибки HTTP для новых событий после старта.
Дедупликация receiver in-memory (4096 IDs), после рестарта сбрасывается.

## HTTP API

**Все** HTTP endpoints, включая `/health/live` и `/health/ready`, требуют
`X-API-Key`. Swagger/OpenAPI по HTTP отключены; offline export ниже. Секрет не
кладите в историю shell; `API_KEY` в примерах — уже установленная переменная.

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" -H 'Idempotency-Key: order-2026-001' \
  -H 'Content-Type: application/json' \
  --data '{"amount":"123.45","currency":"RUB","description":"Заказ 1","metadata":{"order_id":"1"},"webhook_url":"https://hooks.example.com/payments"}'
# Для dev demo замените только webhook_url на http://receiver:8081/webhook
curl -sS http://127.0.0.1:8000/api/v1/payments/PAYMENT_UUID \
  -H "X-API-Key: $API_KEY"
curl -sS http://127.0.0.1:8000/health/ready -H "X-API-Key: $API_KEY"
```

POST возвращает `202` с `payment_id/status/created_at`. Повтор того же ключа и
того же нормализованного тела — `202` с тем же ID; другое тело — `409`.
`Idempotency-Key`: 1–128 символов. Статусы платежа только `pending`, `succeeded`,
`failed`; сбой webhook **не меняет** результат оплаты. GET отдельно показывает
`webhook_status` (`pending`, `delivered`, `exhausted`) и время доставки.
`404` — нет платежа, `401` — неверный ключ, `413` — >32 KiB, `422` — невалидный
запрос, `408` — истёк общий budget чтения body, `503` — БД недоступна.
Broker outage сам по себе не мешает POST `202`.

После auth и до JSON parsing всё тело читается за **10s total** по умолчанию
(`HTTP_BODY_TIMEOUT_SECONDS`, конечное число >0). Это один deadline на весь
receive-loop, включая ожидание первого chunk и EOF, а не idle/keep-alive timeout;
trickle chunks его не продлевают. При `408` чтение прекращается без обращения к БД,
HTTP/1.x connection закрывается. Лимит 32 KiB действует и без Content-Length.

Ограничения: положительный NUMERIC(18,2), максимум 16 целых цифр и 2 дробных;
`RUB/USD/EUR`; description ≤1024 символов; metadata — object ≤8 KiB;
webhook URL ≤2048 символов; всё тело ≤32 KiB. Не храните чувствительные данные
в description/metadata/URL. U+0000 (NUL) запрещён в description, metadata
(рекурсивно в ключах/строках, включая списки), webhook URL и Idempotency-Key:
это sanitized `422`, а не retryable `503`; символ не удаляется молча.
Ответ GET содержит эти поля: доступ к API ограничьте.

## Webhook, подпись и повторная доставка

Тело содержит `event_id`, `payment_id`, `status`, строковый amount (2 знака),
currency, description, metadata, created_at, processed_at (UTC ISO8601).
`Idempotency-Key` webhook равен стабильному `event_id`; generation и номер
попытки в body не передаются. Body/event_id сохраняются при retry/replay.

Заголовки: `X-Webhook-Timestamp` — целые Unix seconds,
`X-Webhook-Signature: v1=<hex>`. Проверяйте HMAC-SHA256 над
`timestamp.encode('ascii') + b'.' + EXACT_RAW_BODY`, **до JSON parsing**:

```python
import hashlib
import hmac
import re
import time


def valid(secret: bytes, timestamp: str, signature: str, raw: bytes) -> bool:
    if not re.fullmatch(r"[0-9]{1,12}", timestamp):
        return False
    if abs(time.time() - int(timestamp)) > 300:  # replay window ±5 минут
        return False
    if not re.fullmatch(r"v1=[0-9a-f]{64}", signature):
        return False
    digest = hmac.new(secret, timestamp.encode("ascii") + b"." + raw, hashlib.sha256)
    return hmac.compare_digest(signature, "v1=" + digest.hexdigest())
```

Как в example receiver, отклоняйте дубли timestamp/signature headers, ограничьте
размер body, сопоставляйте event_id с единственным Idempotency-Key. Синхронизируйте
часы. Timestamp не заменяет **durable dedupe**: атомарно запишите event_id вместе
с бизнес-эффектом, повтору верните 2xx; храните dedupe дольше всего окна retries и
операторских replay. Не пересериализуйте JSON для HMAC. Не логируйте body/secret.

**Всего три попытки**, включая первую; задержки по умолчанию **2 и 4 секунды**,
durable в `Outbox.available_at`, не sleep внутри handler. Только завершённый
bounded ответ 2xx — success; 3xx/4xx/5xx, timeout, DNS/TLS/URL ошибки — failure.
Total timeout ≤10s, connect ≤3s; response ≤64 KiB включительно. При ровно 64 KiB
transport ждёт EOF под тем же total deadline, читая не более одного дополнительного
byte для обнаружения превышения; cap+1 — failure, отсутствие EOF — timeout.
HTTP success перед неудачным DB commit может повториться: гарантия
**at-least-once**, не exactly-once.

## Конфигурация

| Переменная | Назначение / default |
|---|---|
| POSTGRES_PASSWORD | Обязательный отдельный секрет Compose, URL-safe hex |
| RABBITMQ_PASSWORD | Обязательный отдельный секрет Compose, URL-safe hex |
| API_KEY | Обязательный секрет входного API |
| WEBHOOK_SECRET | Обязательный независимый HMAC секрет |
| WEBHOOK_ALLOWED_DOMAINS | Обязательный непустой JSON list доменов |
| ENVIRONMENT | production (development/test только явно) |
| ALLOW_UNSAFE_WEBHOOKS | false; HTTP/private DNS только явно в dev/test |
| RETRY_BASE_SECONDS | 2; первая/вторая задержки base и 2×base |
| HTTP_BODY_TIMEOUT_SECONDS | 10; конечный >0 total budget чтения authenticated body до JSON |
| API_PORT | 8000 на loopback; 0 для случайного порта |
| WEBHOOK_FAIL_FIRST | 0; только demo receiver |
| WEBHOOK_RECEIVER_HOST | standalone receiver: 127.0.0.1; Compose dev: 0.0.0.0 |
| DATABASE_URL | Без Compose: postgresql+asyncpg://… |
| RABBITMQ_URL | Без Compose: amqp://… или amqps://… |

Compose сам собирает URLs из passwords/service DNS; специальные URL-символы в
паролях надо percent-encode либо использовать hex. Не публикуйте вывод обычного
`docker compose config`, inspect env или connection URLs: там секреты. Для проверки
используйте `config --quiet`. `.env.example` без реальных секретов, `.env` ignored;
любые иные env-файлы держите вне version control и с mode0600.

## Очереди, Outbox и операторский runbook

- Durable direct exchange `payments` → quorum `payments.new`; durable direct
  `payments.dlx` → quorum `payments.dlq`. `single-active-consumer`, prefetch=1,
  persistent messages, publisher confirms, mandatory routing.
- Relay берёт due Outbox через `FOR UPDATE SKIP LOCKED`. Нет confirm / unroutable /
  broker outage — published_at не ставится, bounded exponential backoff; после
  восстановления публикация повторяется. Crash после confirm перед commit может
  дать дубликат. Published Outbox **автоматически не удаляется**.
- Worker ack только после commit; БД/инфраструктурная ошибка — nack/backoff,
  malformed envelope — reject; crash/cancel оставляет сообщение на redelivery.
  Устаревшие generation/attempt игнорируются без повторного бизнес-эффекта.
- Третья ошибка webhook атомарно сохраняет `exhausted` и диагностический Outbox
  для `payments.dlq`; broker outage задержит физический DLQ publish, не потеряет его.
- Native DLX использует at-least-once и reject-publish; при недоступном DLX target
  сообщение может удерживаться main quorum queue до восстановления. Следите за
  disk/memory alarms и retained/unacked backlog, не удаляйте очередь для «лечения».

### Exhausted webhook: штатный replay

Сначала проверьте GET, исправьте причину доставки и согласуйте повтор с receiver.

```bash
docker compose --env-file .env -p payments exec -T consumer \
  python -m payments.cli replay-webhook PAYMENT_UUID
docker compose --env-file .env -p payments exec -T consumer \
  python -m payments.cli worker-health
docker compose --env-file .env -p payments run --rm --no-deps api \
  python -m payments.cli export-schema > openapi.json
```

Replay принимает **только terminal payment + exhausted webhook**; под row lock
создаёт новую generation/attempt1 и свежий main envelope в Outbox, очищает safe
last_error. Повтор конкурентного replay отклоняется. Статус оплаты и event_id не
меняются. CLI возвращает 0/1, invalid command 2. Старое DLQ сообщение остаётся
для аудита; оно не является инструкцией на безусловный повтор.

### Pending payment / infrastructure broker-DLQ — другой случай

Native `delivery-limit=3` означает **initial delivery + до 3 redeliveries**,
а не три business webhook attempts. Крэши/infra nacks могут исчерпать этот лимит,
когда платёж ещё pending. `replay-webhook` намеренно такое **не принимает**.

1. Остановите автоматические массовые redrive, выясните причину по safe ID,
   состоянию БД (`status`, generation, expected_attempt, webhook_status), Outbox
   и Rabbit `x-death`/delivery count. Различайте rejected invalid envelope,
   delivery-limit infrastructure case и business exhausted diagnostics.
2. Просматривайте **адресно и с requeue**, не purge/get-no-ack. Не печатайте raw
   extended DLQ payload в общий лог: он может содержать пользовательские данные.
3. Для infra case после исправления причины уполномоченный оператор сверяет
   **актуальные** payment_id, webhook_event_id, generation, expected_attempt с БД
   под row lock. Повтор — свежий строго валидный `PaymentEvent` через durable
   Outbox, destination/event_type `payments.new`, после проверки отсутствия
   существующего такого `(payment_id,generation,event_type,attempt)` Outbox
   (`event_type=payments.new`, destination также `payments.new`).
   Если он уже опубликован, нужен адресный аудируемый redrive того же проверенного
   envelope с confirms/mandatory, а не обход unique constraint/массовый SQL reset.
   Готового generic infra-redrive CLI **нет**: это ручная incident procedure с
   review, snapshot и проверкой каждого ID, не обещание автоматического recovery.
4. После подтверждённой доставки отдельно архивируйте/удаляйте только выбранную
   DLQ запись по утверждённой retention policy. Не меняйте результат gateway.

**Нельзя слепо requeue весь DLQ в main:** extended business DLQ payload не проходит
строгий envelope contract. Нельзя bulk purge, удалять volumes или публиковать
диагностику как PaymentEvent. Перед любым удалением — ownership, backup и явное
подтверждение; этот проект не содержит cleanup-команд, стирающих данные.

## Миграции, локальная разработка и тесты

```bash
uv sync --frozen
# Нужна только DATABASE_URL, без API/broker secrets:
uv run alembic upgrade head
uv run python -m payments.api
uv run python -m payments.worker
uv run python -m payments.cli export-schema > openapi.json
uv run pytest tests/unit -q
# Настоящие отдельные test PG/Rabbit, admin права CREATE DATABASE / manage vhosts.
# Экспортируйте TEST_DATABASE_URL, TEST_RABBITMQ_URL (с admin credentials),
# TEST_RABBITMQ_MANAGEMENT_URL (см. tests/integration/conftest.py).
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy src tests migrations examples
uv lock --check
```

Не направляйте fixtures в production: они создают/удаляют **собственные случайные**
test databases/vhosts. Без test env integration пропускаются; это не доказательство
работы PG/Rabbit. Полный native DLX outage тест ждёт **180 секунд** — не исключайте
его из итоговой проверки, дайте всему прогону ≥15 минут. Известен minor upstream
`aiormq 6.9.4 ResourceWarning` при abrupt outage и `-W error` (transport closing /
reader EOF); обычная suite проходит. Warning не подавлен и third-party не patched.
Для обновлений повторите реальные outage/shutdown/slow DLX tests.
Standalone Python entrypoints читают экспортированное окружение, **не загружают
`.env` автоматически**. Перед ними задайте DATABASE_URL/RABBITMQ_URL и остальные
Settings из таблицы; формат Compose passwords сам по себе не заменяет эти URLs.

Для обновления приложения: backup → совместимая миграция → one-shot `migrate` →
API/consumer rollout. При уже завершённом migrate контейнере явно выполните
`docker compose ... run --rm migrate` на новой image **до** rollout; `depends_on`
не является полноценным migration orchestrator/rollback. Не выполняйте downgrade
на боевых данных без отдельного плана восстановления. SIGTERM worker прекращает
intake, bounded drain, cancel+join sends, затем закрывает transport/client/DB;
deadlines cooperative, не защита от блокирующего event loop стороннего кода.

## Production: обязательные внешние меры и ограничения

Этот Compose — воспроизводимый **single-host baseline**, не готовый HA кластер.

- **HA:** один Postgres и один Rabbit node, quorum queue сама по себе не даёт
  отказоустойчивости узла. Нужны replicated Rabbit cluster, HA Postgres/failover,
  распределение по failure domains; проверьте confirms, quorum и disaster recovery.
- **Backups:** регулярный PostgreSQL backup + WAL/PITR, restore drills; Rabbit
  definitions/config и выбранная стратегия восстановления durable backlog.
  Docker volume не backup; snapshot без координации не гарантирует consistency.
- **Secrets:** secret manager, least-privilege DB/broker identities, отдельная
  migration role, rotation/revocation API/HMAC. Env baseline виден Docker admin;
  production adapter может выдавать runtime env из secret manager. Не bake в image.
- **TLS:** termination/reverse proxy перед API, auth/rate limiting/body limits;
  private network или TLS к БД и AMQPS к broker. Compose plaintext внутри bridge —
  не гарантия шифрования. Management не выставлять в публичную сеть.
- **Egress:** firewall/proxy allowlist, блокировка metadata/private networks на
  уровне сети плюс DNS policy; application SSRF checks — defense in depth.
- **Retention:** payments/metadata, idempotency records, Outbox и DLQ растут;
  задайте сроки, аудит/GDPR policy, архивирование и batch cleanup published rows
  только после требуемого окна восстановления. Ничего не purgится автоматически.
- **Monitoring:** API latency/error/DB readiness; heartbeat age/consumer/relay;
  age/count oldest due unpublished Outbox, publish failures, queue ready/unacked,
  DLQ depth/age, exhausted count, last_error codes, Rabbit disk/memory/connection
  alarms, DB locks/pool/connections, backup/restore age. JSON logs fail-closed,
  без headers/URLs/body/DSN/tracebacks; unsafe uvicorn access log выключен. Safe
  log_redacted lifecycle messages ожидаемы. Нужны внешние metrics/alerts, здесь
  нет готового Prometheus stack, SLA dashboards или автоматического remediation.
- **Throughput:** SAC + prefetch1 намеренно сериализуют обработку; gateway latency
  ограничивает пропускную способность. Несколько consumer replicas — standby,
  не горизонтальное ускорение. Partitioning — отдельное изменение ordering/data
  contract. API replicas допустимы, relay использует SKIP LOCKED.
- **Real gateway:** внешнее списание должно поддерживать **собственную durable
  idempotency по payment_id**, lookup/reconciliation и безопасный retry после
  timeout/crash. Стабильный эмулятор не решает атомарность банка и нашей БД.
  At-least-once delivery и receiver dedupe обязательны даже после manual replay.
