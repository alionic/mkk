# Результат реализации

Все семь этапов плана выполнены в ветке `feat/async-payments-service`.
На каждом этапе работали отдельные исполнитель и ревьюер; дополнительно
проведены итоговое сквозное ревью, исправление найденных дефектов с RED→GREEN
регрессиями и независимое повторное ревью исправлений.
На этапе реализации коммиты, push и merge не выполнялись. Последующая публикация
ветки и draft PR выполняется по отдельному запросу пользователя; merge не запрошен.

## Реализовано

- FastAPI POST/GET платежей, точный Decimal, Pydantic v2, общий X-API-Key.
- Идемпотентность с конкурентной защитой PostgreSQL и конфликтом тела `409`.
- Асинхронный SQLAlchemy, таблицы payments/outbox и Alembic migration.
- Атомарное создание Payment+Outbox, publisher confirms, mandatory routing,
  SKIP LOCKED, повтор публикации без потери при недоступном брокере.
- Один активный FastStream consumer, эмулятор шлюза, terminal commit до webhook.
- Три суммарные попытки webhook с durable задержками 2/4 секунды, затем DLQ через
  тот же transactional Outbox; отдельный crash-delivery limit брокера.
- `webhook_url` в теле запроса; WEBHOOK_ALLOWED_DOMAINS разрешает базовый домен
  и его поддомены, с проверкой DNS/IP непосредственно в connector.
- Подпись HMAC, стабильные event_id/тело, bounded HTTP/DNS/response и запрет
  redirects/proxy. Входной body имеет byte cap и total timeout.
- CLI replay-webhook с новым generation и прежним событием webhook,
  offline OpenAPI export, JSON-логи, process-bound worker healthcheck.
- Non-root Docker image, Compose с postgres/rabbitmq/api/consumer/migrate,
  optional dev receiver, healthchecks и persistent volumes.

Инструкции запуска, env-настройки и curl — в [README](../README.md).

## Итоговые проверки

Независимый финальный прогон контроллера на исправленном коде:

| Проверка | Результат |
|---|---|
| Полный pytest с настоящими PostgreSQL/RabbitMQ, включая DLX outage | **468 passed**, 453.83 s, без skips/deselections |
| `ruff check .` | PASS |
| `ruff format --check .` | PASS |
| `mypy src tests migrations examples` | PASS, 48 source files |
| `uv lock --check` | PASS |
| `docker compose ... config --quiet` | PASS |
| `docker compose ... build` | PASS |
| `git diff --check` | PASS |

Реальный Compose smoke после исправлений проверил оплату и подписанное
уведомление, одинаковый ключ/тело и конфликт тела, три HTTP-ошибки до настоящей
DLQ, CLI replay, приём API при остановленном брокере и восстановление из Outbox.
Raw TCP проверки развёрнутого API подтвердили `408` и закрытие соединения при
stalled/trickle body через настроенные 10 секунд; NUL-входы дают `422`.

Итоговое повторное ревью: все Important исправлены, новых Critical/Important нет.
Полные локальные логи и отчёты сохранены в ignored
`.superpowers/sdd/2026-11-15-payments/`, секреты в публичный отчёт не включены.

## Принятые границы и их последствия

- Первоначальное ревью выполнялось по filesystem snapshots до запроса публикации.
  Перед commit/PR проверяется соответствие итогового diff проверенному коду.
- Локальные ignored evidence сохранены, а не удалены: без коммитов это единственный
  подробный журнал выполнения. Цена — дополнительное локальное место на диске.
- Шлюз остаётся эмулятором. Для реальных списаний нужны настоящий идемпотентный
  адаптер, согласование неопределённых исходов и отдельная compliance-проработка;
  PCI/карточные данные/возвраты в реализацию не входят.
- Compose — single-host baseline и один интеграционный контур, не HA и не
  multitenancy. Отказоустойчивый кластер и tenant isolation требуют отдельной работы.
- Production TLS, secrets manager, egress restrictions, backup/restore и alerting
  настраиваются оператором в целевой среде. Без этих мер безопасная и
  восстанавливаемая production-эксплуатация не обеспечена одним репозиторием.
- Для exhausted webhook есть CLI replay, для generic infrastructure-DLQ — ручной
  runbook. До отдельного инструмента такой incident требует действий оператора.

## Отложенные неблокирующие замечания

1. Часть старых negative raw-parser unit tests ожидает общий ValueError, а не
   конкретный Pydantic ValidationError. HTTP `422` и новые NUL cases покрыты отдельно.
2. В uniqueness fixture нового поколения колонка generation и payload.generation
   не синхронизированы. Production replay строит корректный envelope и отдельно
   проверен integration/E2E-тестами; замечание относится к тестовому примеру.
3. У aiormq 6.9.4 воспроизводится ResourceWarning при дополнительном `-W error`
   abrupt-disconnect сценарии. Подтверждённой потери платежей/утечки FD не найдено;
   обычный итоговый полный прогон чистый. Warning не подавлен; нужен отдельный
   dependency follow-up с reproducer/FD-проверкой и совместимым обновлением.

Выделенная тестовая инфраструктура и демонстрационный Compose оставлены
работающими; данные и volumes не удалялись. Это dev-стенд, не production deployment.
