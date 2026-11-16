import asyncio
from pathlib import Path

import yaml

from payments.cli import export_schema

ROOT = Path(__file__).resolve().parents[2]


def test_deployment_contract() -> None:
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    services = compose["services"]
    assert {"postgres", "rabbitmq", "api", "consumer", "migrate", "receiver"} <= services.keys()
    for name in ("postgres", "rabbitmq", "api", "consumer"):
        assert services[name]["restart"] == "unless-stopped"
        assert services[name]["healthcheck"]["test"]
    for name in ("postgres", "rabbitmq"):
        assert "ports" not in services[name]
        assert services[name]["volumes"]
        assert ":latest" not in services[name]["image"]
    assert services["postgres"]["image"] == "postgres:17.7-bookworm"
    assert services["rabbitmq"]["image"] == "rabbitmq:4.2.1-management"
    assert services["rabbitmq"]["healthcheck"]["test"][:4] == [
        "CMD",
        "gosu",
        "rabbitmq",
        "rabbitmq-diagnostics",
    ]
    assert services["api"]["depends_on"] == {
        "postgres": {"condition": "service_healthy"},
        "migrate": {"condition": "service_completed_successfully"},
    }
    assert services["consumer"]["depends_on"] == {
        "rabbitmq": {"condition": "service_healthy"},
        "migrate": {"condition": "service_completed_successfully"},
    }
    assert services["migrate"]["restart"] == "no"
    assert services["receiver"]["profiles"] == ["dev"]
    assert "ports" not in services["receiver"]
    assert "127.0.0.1:" in services["api"]["ports"][0]
    for name in ("api", "consumer"):
        assert services[name]["stop_grace_period"] == "180s"
        env = services[name]["environment"]
        assert env["ENVIRONMENT"] == "${ENVIRONMENT:-production}"
        assert env["ALLOW_UNSAFE_WEBHOOKS"] == "${ALLOW_UNSAFE_WEBHOOKS:-false}"
        assert env["WEBHOOK_ALLOWED_DOMAINS"] == "${WEBHOOK_ALLOWED_DOMAINS:?set domain allowlist}"
    assert "worker-health" in " ".join(services["consumer"]["healthcheck"]["test"])
    assert set(compose["volumes"]) == {"postgres-data", "rabbitmq-data"}


def test_image_contract_and_context_allowlist() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert "python:3.12.16-slim-bookworm" in dockerfile
    assert "ghcr.io/astral-sh/uv:0.13.8" in dockerfile
    assert " AS build" in dockerfile and " AS runtime" in dockerfile
    assert "--frozen" in dockerfile and "--no-dev" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert "COPY . " not in dockerfile
    assert "greenlet.__file__" in dockerfile and "shutil.rmtree" in dockerfile
    ignored = (ROOT / ".dockerignore").read_text().splitlines()
    assert "**" in ignored  # Opt-in build context, not an ever-growing denylist.
    assert "!src/**" in ignored and "!uv.lock" in ignored
    for path in ("tests", "docs", ".env", ".git", ".superpowers"):
        assert f"!{path}" not in ignored


def test_exported_http_contract() -> None:
    schema = asyncio.run(export_schema())
    paths = schema["paths"]
    assert isinstance(paths, dict)
    assert {"/api/v1/payments", "/api/v1/payments/{payment_id}"} <= paths.keys()
    post = paths["/api/v1/payments"]["post"]
    header = next(p for p in post["parameters"] if p["name"] == "Idempotency-Key")
    assert header["required"] and header["schema"]["maxLength"] == 128
    assert post["security"] == [{"APIKeyHeader": []}]
    components = schema["components"]
    assert isinstance(components, dict)
    assert components["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"
