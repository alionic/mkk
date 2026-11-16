from typing import Any

import pytest

from payments.config import Settings
from payments.security import normalize_domain, validate_webhook_url


def settings(**overrides: Any) -> Settings:
    return Settings(
        **(
            {
                "database_url": "postgresql+asyncpg://pay:password@localhost/payments",
                "rabbitmq_url": "amqp://pay:password@localhost/",
                "api_key": "test-key",
                "webhook_secret": "test-secret",
                "webhook_allowed_domains": ["example.com", "пример.рф"],
                "environment": "production",
            }
            | overrides
        )
    )


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://example.com/hook", "https://example.com/hook"),
        ("https://a.b.example.com/hook", "https://a.b.example.com/hook"),
        ("HTTPS://EXAMPLE.COM.:443/hook?q=%2F", "https://example.com/hook?q=%2F"),
        ("https://EXAMPLE.COM。/hook", "https://example.com/hook"),
        ("https://ПРИМЕР.РФ./hook", "https://xn--e1afmkfd.xn--p1ai/hook"),
        ("https://a.пример.рф/hook", "https://a.xn--e1afmkfd.xn--p1ai/hook"),
    ],
)
def test_domain_policy_allows_base_and_subdomains(url: str, expected: str) -> None:
    assert validate_webhook_url(url, settings()) == expected


@pytest.mark.parametrize(
    "url",
    [
        "https://evilexample.com/hook",
        "https://example.com.attacker.net/hook",
        "https://example.com@attacker.net/hook",
        "https://user:password@example.com/hook",
        "https://@example.com/hook",
        "https://example.com:444/hook",
        "https://example.com:0/hook",
        "https://example.com:invalid/hook",
        "https://127.0.0.1/hook",
        "https://[::1]/hook",
        "https://[2606:4700:4700::1111]/hook",
        "http://example.com/hook",
        "ftp://example.com/hook",
        "//example.com/hook",
        "https://example.com/hook#fragment",
        "https://example.com/hook#",
        "https://example.com\\@evil.com/hook",
        " https://example.com/hook",
        "https://example.com/\nhook",
        "https://example.com/%0d%0aX:a",
        "https://%65xample.com/hook",
        "https://example.com../hook",
        "https://example.com:/hook",
        "https://2130706433/hook",
        "https://0x7f000001/hook",
        "https://0x7f.0x0.0x0.0x1/hook",
    ],
)
def test_domain_policy_rejects_unsafe_or_unlisted_urls(url: str) -> None:
    with pytest.raises(ValueError):
        validate_webhook_url(url, settings())


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("EXAMPLE.COM.", "example.com"),
        ("EXAMPLE.COM。", "example.com"),
        ("ПРИМЕР.РФ", "xn--e1afmkfd.xn--p1ai"),
        ("a.b.Example.com", "a.b.example.com"),
        ("bücher.example", "xn--bcher-kva.example"),
    ],
)
def test_normalize_domain(raw: str, expected: str) -> None:
    assert normalize_domain(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        " example.com",
        "example.com ",
        "example.com..",
        "*.example.com",
        "a..example.com",
        "https://example.com",
        "example.com:443",
        "127.0.0.1",
        "0x7f.0x0.0x0.0x1",
        "0x7f.0.0.0x1",
        "::1",
        "[::1]",
        "a_b.example.com",
        "-bad.example.com",
        "bad-.example.com",
        "a" * 64 + ".com",
    ],
)
def test_normalize_domain_rejects_non_dns_inputs(raw: str) -> None:
    with pytest.raises(ValueError):
        normalize_domain(raw)


def test_dev_http_does_not_disable_allowlist_or_url_structure_checks() -> None:
    configured = settings(environment="development", allow_unsafe_webhooks=True)
    assert validate_webhook_url("http://example.com:8080/hook", configured) == (
        "http://example.com:8080/hook"
    )
    for url in ("http://attacker.net", "http://user@example.com", "http://example.com/#x"):
        with pytest.raises(ValueError):
            validate_webhook_url(url, configured)


def test_policy_errors_never_echo_url_or_query() -> None:
    with pytest.raises(ValueError) as error:
        validate_webhook_url("https://user:password@attacker.net/?secret=DO_NOT_LOG", settings())
    assert "DO_NOT_LOG" not in str(error.value)
    assert "password" not in str(error.value)


def test_policy_enforces_url_length() -> None:
    with pytest.raises(ValueError):
        validate_webhook_url("https://example.com/" + "x" * 2049, settings())
