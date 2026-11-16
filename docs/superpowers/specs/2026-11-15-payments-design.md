# Сервис асинхронной обработки платежей

## Назначение и границы

Реализовать эксплуатационно пригодный микросервис по ТЗ: FastAPI, Pydantic v2,
асинхронный SQLAlchemy 2, PostgreSQL, RabbitMQ/FastStream, Alembic и Docker Compose.
Сейчас репозиторий содержит только `.gitignore`.

Пользователь подтвердил реальное использование, а не только демонстрацию.
Платёжный шлюз при этом остаётся **эмулятором**, как требует исходное ТЗ.
Настоящее списание, PCI DSS, хранение карточных данных, возвраты, мультиарендность
и высокодоступный кластер инфраструктуры не входят в работу. Compose на одном
хосте не является HA-развёртыванием; это ограничение явно описывается в README.

Подтверждённое уточнение: `webhook_url` передаётся в теле каждого запроса.
В настройках задаются разрешённые домены webhook. Каждая запись разрешает сам
домен и его поддомены любой глубины (уточнение пользователя). Общего URL в
настройках нет.

Ниже приведены инженерные решения исполнителя; они требуют утверждения дизайна.

## Контракт HTTP API

Все HTTP endpoints, включая healthcheck, защищены статическим `X-API-Key`.
Секрет загружается из окружения, сравнивается без утечки по времени и не логируется.
Отсутствующий/неверный ключ возвращает одинаковый `401`.
Документация OpenAPI отключена по умолчанию; экспорт схемы доступен как команда.

### POST /api/v1/payments

Обязательные заголовки: `X-API-Key`, `Idempotency-Key` (1–128 символов).
Поля тела: `amount`, `currency`, `description`, `metadata`, `webhook_url`.

- `amount`: положительный decimal, NUMERIC(18, 2), не более двух знаков после запятой;
  в ответах строка, без преобразования через float.
- `currency`: строго RUB, USD либо EUR.
- `description`: строка до 1024 символов, по умолчанию пустая.
- `metadata`: JSON object, по умолчанию `{}`, не более 8 KiB в каноническом JSON.
- `webhook_url`: абсолютный HTTPS URL до 2048 символов, порт только 443,
  без credentials и fragment, с доменом из allowlist.
- Неизвестные поля запрещены. Размер всего тела ограничен 32 KiB до JSON parsing.

Ответ `202`: `payment_id`, `status`, `created_at`.
Повтор того же ключа и того же нормализованного тела возвращает тот же платёж и
`202`; тот же ключ с другим телом — `409`. Неверное тело/отсутствующий
Idempotency-Key — `422`. Хэш канонического тела включает webhook URL и metadata,
нормализованную сумму и валюту; порядок ключей JSON не влияет на сравнение.
Уникальность в PostgreSQL, а не проверка «SELECT, затем INSERT», разрешает гонки.

### GET /api/v1/payments/{payment_id}

Возвращает поля платежа, `payment_id`, `amount`, `currency`, `description`,
`metadata`, `status`, `webhook_url`, `created_at`, `processed_at`, а также отдельные
`webhook_status` и `webhook_delivered_at`. Статусы оплаты только pending,
succeeded, failed. Неуспешный webhook не меняет результат оплаты на failed.
Несуществующий UUID — `404`, некорректный UUID — `422`.
Внутренние exception text, хэши и секреты не возвращаются.

## Компоненты

Один Python-пакет с явными границами: HTTP API, прикладные сервисы, SQLAlchemy
модели/транзакции, брокер/Outbox, шлюз и безопасный webhook transport, настройки.
Без формальных repository-интерфейсов для каждой таблицы и иных лишних слоёв.

Два долгоживущих процесса:

1. `api`: HTTP и PostgreSQL. При создании платежа не обращается к RabbitMQ.
2. `consumer`: ровно один обработчик сообщений; в этом же процессе отдельная
   фоновая задача Outbox relay. Обработчик проводит платёж, сохраняет результат,
   отправляет webhook и планирует retry/DLQ.

Очередь использует single-active-consumer и prefetch=1: даже при случайном запуске
двух экземпляров активен один. Это осознанный предел пропускной способности по ТЗ,
а не обещание масштабирования до высокой нагрузки. Relay допускает несколько
экземпляров благодаря `FOR UPDATE SKIP LOCKED`.

## Данные и миграция

Таблица `payments`: UUID PK; amount NUMERIC(18,2); currency; description;
metadata JSONB; status; idempotency_key UNIQUE; request_hash; webhook_url;
created_at/processed_at TIMESTAMPTZ; стабильный webhook event UUID;
webhook_status (pending/delivered/exhausted); webhook_delivered_at;
номер ожидаемой попытки, UUID поколения обработки и последний безопасный код ошибки.
CHECK constraints обеспечивают положительную сумму, допустимые статусы/валюты
и согласованность terminal status с processed_at.

Таблица `outbox`: UUID PK; payment_id FK; event_type/schema_version;
JSONB payload; destination; available_at; created_at; published_at;
publish_attempts; last_error. Уникальность логического события
`(payment_id, generation, event_type, attempt)` предотвращает повторное планирование.
Частичный индекс по `(available_at, created_at)` для неопубликованных записей.
FK и ограничения добавляются Alembic-миграцией; `create_all()` в runtime нет.
Даты UTC, UUID создаются в приложении.

## Создание и transactional Outbox

Payment и первоначальное событие `payments.new` создаются одной DB-транзакцией.
При конфликте уникального ключа приложение читает существующий платёж и
сопоставляет request_hash, корректно работая при конкурентных запросах.

Relay выбирает готовые записи с `FOR UPDATE SKIP LOCKED`, публикует persistent
сообщение с publisher confirms и mandatory routing и только после подтверждения
фиксирует published_at. Сетевые ожидания имеют timeout; объём партии ограничен.
Отсутствие маршрута считается ошибкой, не успешной публикацией.
При ошибке увеличивает publish_attempts и переносит available_at с ограниченным
экспоненциальным backoff. Ошибки публикации не имеют окончательного лимита:
недоступность брокера не должна уничтожать задание.

Сбой после broker confirm, но до DB commit, может породить дубль — это ожидаемо.
Гарантия at-least-once, не exactly-once. Опубликованные строки не удаляются
автоматически; README содержит рекомендации по retention/архивированию.

## Брокер, обработка и повторная доставка

Durable direct exchange `payments`, durable quorum queue `payments.new`,
durable dead-letter exchange `payments.dlx`, quorum queue `payments.dlq`.
Topology объявляется до публикации. Сообщение содержит schema_version, event_id,
payment_id, generation, attempt; не содержит секретов или доверенного webhook URL.
Получатель читает актуальный платёж из PostgreSQL.

В обработчике explicit ack: подтверждение только после зафиксированного
результата, durable retry либо durable записи в DLQ-Outbox.

Алгоритм обработки:

1. Проверить envelope. Невалидное/неподдерживаемое сообщение — reject в DLX,
   без бесконечного requeue. Отсутствующий payment — также DLQ с диагностикой.
2. Прочитать и заблокировать payment. Устаревшее generation/attempt, уже доставленный
   webhook или уже exhausted payment — безопасный no-op/ack. Блокировка
   защищает изменения, single-active-consumer сериализует выполнение.
3. Если статус pending, вызвать эмулятор: задержка 2–5 секунд, распределение
   90% succeeded / 10% failed. Результат воспроизводим по payment_id, чтобы
   crash/replay не менял решение шлюза. Зафиксировать status/processed_at
   **до** HTTP webhook. DB-транзакция на этом шаге ограничена временем шлюза.
4. Отправить webhook для terminal payment. Уже завершённый платёж не проводить
   повторно при retry или дублирующемся сообщении.
5. При 2xx сохранить delivered и timestamp, затем ack. При любой другой
   HTTP-ошибке/timeout запланировать следующую попытку в той же транзакции,
   которая обновляет ожидаемый attempt. Retry — новая Outbox запись с
   available_at; consumer не спит с unacked сообщением на время backoff.
6. Всего **три попытки**, включая первоначальную; задержки по умолчанию 2 и 4
   секунды (база настраивается). Business decline — не техническая ошибка:
   платёж failed сразу, webhook отправляется по тем же правилам.
7. После третьей ошибки отметить webhook exhausted и атомарно создать Outbox
   событие для `payments.dlx` → `payments.dlq`, затем ack. Публикация в DLQ
   защищена тем же Outbox, что и первичная публикация.

Временная недоступность БД не подтверждается и не считается бизнес-попыткой:
используются bounded backoff и broker redelivery. Для crash/poison loops отдельно
задаётся явный quorum delivery-limit; превышение уводит сообщение в DLQ.
Для broker dead-lettering используется at-least-once стратегия quorum queues
с reject-publish overflow. Конкретные поддерживаемые аргументы проверяются
интеграционным тестом на выбранной закреплённой версии RabbitMQ.

Операторский replay из DLQ выполняется только осознанно: README описывает
диагностику, состояние payment и сброс delivery state/создание Outbox в одной
транзакции через отдельную CLI-команду с явным payment_id. Replay создаёт новое
поколение обработки; старые сообщения не могут начать новую цепочку попыток.
Стабильный webhook event_id при replay сохраняется. Обычная повторная
публикация старого exhausted сообщения не должна возобновлять webhook.

## Webhook и безопасность

`WEBHOOK_ALLOWED_DOMAINS` — JSON-массив базовых DNS-имён в окружении.
Регистр/IDNA нормализуются. Каждая запись разрешает сам домен и поддомены любой
глубины: `host == domain` либо `host.endswith("." + domain)` после нормализации.
`example.com` разрешает `a.b.example.com`, но не `evilexample.com` и не
`example.com.attacker.net`. Wildcard-синтаксис не нужен и не принимается.
Allowlist обязательна и не может быть пустой. IP literals запрещены.
При создании проверяется URL и allowlist; непосредственно при соединении
проверяются allowlist и **все** результаты DNS-resolution.

Loopback, private, link-local, multicast, unspecified, reserved, IPv4-mapped
private IPv6 и прочие непубличные адреса блокируются. DNS-проверка выполняется
в resolver сетевого connector: сокет соединяется именно с проверенным адресом,
без повторного независимого DNS lookup; TLS SNI/проверка сертификата сохраняют
исходный домен. Это необходимо против DNS rebinding/TOCTOU.
Реализация на aiohttp с публичным AbstractResolver/TCPConnector API,
без environment proxy, redirects, DNS cache и отключения TLS verification.
Ограничены connect/total timeouts, response read и concurrency.
Изменение allowlist блокирует дальнейшие отправки на удалённый домен.

Локальная демонстрация допускает HTTP/private IP **только** через явный dev-only
флаг, запрещённый при ENVIRONMENT=production; allowlist остаётся обязательной.
Необязательный compose profile предоставляет локальный webhook receiver.

JSON webhook: event_id, payment_id, status, amount (строка), currency,
description, metadata, created_at, processed_at. Event_id и тело стабильны между
попытками. `Idempotency-Key` исходящего запроса равен event_id.
`X-Webhook-Timestamp` и `X-Webhook-Signature: v1=<hex>`:
HMAC-SHA256 общего WEBHOOK_SECRET по `timestamp + "." + raw_body`.
Подпись вычисляется по ровно тем bytes, которые отправляются.
Timestamp меняется при retry, event_id — нет. README содержит проверку подписи,
окно против replay и требование дедупликации у получателя.
Сейчас один доверенный интеграционный контур/общий секрет; отдельные секреты
клиентов и авторизация между tenants потребуют отдельного расширения API.

## Конфигурация и эксплуатация

Python 3.12+, uv с lockfile, pydantic-settings; секреты только из окружения,
`.env.example` без настоящих credentials. Конкретные версии зависимостей
закрепляются после проверки совместимости FastStream/RabbitMQ.

Compose: postgres, rabbitmq, api, consumer; дополнительно одноразовый migrate
service и необязательный dev webhook receiver. Healthchecks и зависимости
исключают гонку миграций при старте. DB/broker не публикуются наружу по умолчанию;
API локально слушает 127.0.0.1. Persistent volumes, non-root app container,
multi-stage build, restart policy и корректный SIGTERM/shutdown.
Для production обязательны внешнее TLS termination, secrets management,
резервное копирование PostgreSQL, мониторинг и сетевые egress-ограничения.
README явно отделяет готовую реализацию гарантий от настройки production среды.

Структурированные JSON-логи: event_id, payment_id, attempt, безопасная категория
ошибки, длительность. Не логировать API key, секрет подписи, metadata, полный URL
с query string и тела webhook. Health live/readiness; API readiness зависит от
БД, но не RabbitMQ, поскольку при его отказе платежи сохраняются в Outbox.
README: команды старта/тестов/миграций, curl, настройки allowlist, схема потока,
retry/DLQ/replay, отказные сценарии и ограничения single consumer.

## Проверки при реализации

- Unit: валидация денег/JSON/URL/settings, канонический хэш, подпись, DNS/SSRF,
  стабильность gateway outcome, backoff и сообщения об ошибках.
- PostgreSQL integration: миграция upgrade/downgrade, ограничения, атомарность
  payment+Outbox, конкурентная идемпотентность, конфликт ключа, skip-locked relay,
  publish failure/confirm-before-commit и дубли consumer.
- API: auth на всех routes, create/get/404/409/422/413, недоступный RabbitMQ.
- Consumer: успех/decline, webhook timeout/non-2xx, три попытки, durable retry,
  финальный DLQ, отсутствие повторного проведения и стабильный webhook event_id.
- RabbitMQ integration: реальное routing/mandatory confirms, durable topology,
  reject в DLQ, перезапуск consumer, quorum dead-letter arguments.
- Compose smoke: build, migrate, POST→terminal state→webhook, retry→DLQ,
  повтор запроса и восстановление после broker outage, если Docker доступен.
- Ruff, strict type checking и pytest. Недоступные в окружении реальные
  проверки отмечаются явно; mocks не выдаются за проверку инфраструктуры.
