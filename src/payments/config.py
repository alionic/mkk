"""Environment-backed configuration, validated explicitly at process startup."""

from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from payments.security import normalize_domain


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="forbid", hide_input_in_errors=True, case_sensitive=False
    )

    database_url: str = Field(repr=False)
    rabbitmq_url: str = Field(repr=False)
    api_key: SecretStr
    webhook_secret: SecretStr
    webhook_allowed_domains: list[str] = Field(min_length=1)
    environment: Literal["development", "test", "production"] = "production"
    allow_unsafe_webhooks: bool = False
    retry_base_seconds: float = Field(default=2, gt=0, allow_inf_nan=False)
    http_body_timeout_seconds: float = Field(default=10, gt=0, allow_inf_nan=False)

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        try:
            parsed = urlsplit(value)
            valid = (
                parsed.scheme == "postgresql+asyncpg" and bool(parsed.hostname) and parsed.port != 0
            )
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("database URL must use postgresql+asyncpg with a host")
        return value

    @field_validator("rabbitmq_url")
    @classmethod
    def validate_rabbitmq_url(cls, value: str) -> str:
        try:
            parsed = urlsplit(value)
            valid = (
                parsed.scheme in {"amqp", "amqps"} and bool(parsed.hostname) and parsed.port != 0
            )
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("RabbitMQ URL must use amqp or amqps with a host")
        return value

    @field_validator("api_key", "webhook_secret")
    @classmethod
    def reject_empty_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("secret must not be empty")
        return value

    @field_validator("webhook_allowed_domains")
    @classmethod
    def normalize_allowed_domains(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(normalize_domain(domain) for domain in value))

    @model_validator(mode="after")
    def reject_unsafe_production(self) -> Self:
        if self.environment == "production" and self.allow_unsafe_webhooks:
            raise ValueError("unsafe webhooks are forbidden in production")
        return self
