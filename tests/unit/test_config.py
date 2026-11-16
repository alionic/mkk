from typing import Any

import pytest

from payments.config import Settings


def settings(**overrides: Any) -> Settings:
    return Settings(
        **(
            {
                "database_url": "postgresql+asyncpg://pay:password@localhost/payments",
                "rabbitmq_url": "amqp://pay:password@localhost/",
                "api_key": "test-api-key",
                "webhook_secret": "test-webhook-secret",
                "webhook_allowed_domains": ["Example.COM.", "ПРИМЕР.РФ"],
                "environment": "production",
            }
            | overrides
        )
    )


@pytest.mark.parametrize(
    "change",
    [
        {"allow_unsafe_webhooks": True},
        {"api_key": ""},
        {"api_key": "   "},
        {"webhook_secret": ""},
        {"webhook_secret": "\t"},
        {"webhook_allowed_domains": []},
    ],
)
def test_production_rejects_unsafe_settings(change: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        settings(**change)


def test_settings_normalizes_allowlist_and_protects_secrets() -> None:
    configured = settings()
    assert configured.webhook_allowed_domains == ["example.com", "xn--e1afmkfd.xn--p1ai"]
    assert configured.retry_base_seconds == 2
    assert configured.api_key.get_secret_value() == "test-api-key"
    assert configured.webhook_secret.get_secret_value() == "test-webhook-secret"
    for secret in ("test-api-key", "test-webhook-secret", "password"):
        assert secret not in repr(configured)
    assert configured.allow_unsafe_webhooks is False


def test_settings_loads_real_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "DATABASE_URL": "postgresql+asyncpg://pay:password@localhost/payments",
        "RABBITMQ_URL": "amqp://pay:password@localhost/",
        "API_KEY": "env-key",
        "WEBHOOK_SECRET": "env-secret",
        "WEBHOOK_ALLOWED_DOMAINS": '["EXAMPLE.COM."]',
        "ENVIRONMENT": "production",
        "ALLOW_UNSAFE_WEBHOOKS": "false",
        "RETRY_BASE_SECONDS": "3",
    }.items():
        monkeypatch.setenv(name, value)
    configured = Settings()
    assert configured.api_key.get_secret_value() == "env-key"
    assert configured.webhook_allowed_domains == ["example.com"]
    assert configured.retry_base_seconds == 3


def test_development_unsafe_flag_requires_explicit_opt_in() -> None:
    assert settings(environment="development", allow_unsafe_webhooks=True).allow_unsafe_webhooks
    assert not settings(environment="development").allow_unsafe_webhooks


@pytest.mark.parametrize(
    "domain", ["*.example.com", "https://example.com", "127.0.0.1", "bad..com"]
)
def test_settings_rejects_invalid_allowlist_entries(domain: str) -> None:
    with pytest.raises(ValueError):
        settings(webhook_allowed_domains=[domain])


@pytest.mark.parametrize(
    "change",
    [
        {"retry_base_seconds": 0},
        {"retry_base_seconds": -1},
        {"retry_base_seconds": float("inf")},
        {"database_url": "http://example.com"},
        {"rabbitmq_url": "https://example.com"},
        {"environment": "prodution"},
        {"unknown": 1},
    ],
)
def test_settings_fail_fast_on_invalid_configuration(change: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        settings(**change)


def test_settings_errors_do_not_expose_secrets_or_url_query() -> None:
    with pytest.raises(ValueError) as error:
        settings(api_key="DO_NOT_LOG", database_url="https://user:pass@host/?token=DO_NOT_LOG")
    assert "DO_NOT_LOG" not in str(error.value)
    assert "user:pass" not in str(error.value)


@pytest.mark.parametrize("budget", [0, -1, float("inf"), float("-inf"), float("nan")])
def test_body_read_budget_must_be_finite_positive(budget: float) -> None:
    with pytest.raises(ValueError):
        settings(http_body_timeout_seconds=budget)


def test_body_read_budget_is_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_BODY_TIMEOUT_SECONDS", "0.25")
    assert settings().http_body_timeout_seconds == 0.25
