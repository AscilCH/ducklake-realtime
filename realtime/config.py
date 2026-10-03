import os
from dataclasses import dataclass, field


class ConfigError(RuntimeError):
    pass


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"missing environment variable: {name}")
    return value


def _optional_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from None
    if value <= 0:
        raise ConfigError(f"{name} must be positive, got {value}")
    return value


@dataclass(frozen=True)
class Settings:
    postgres_user: str
    postgres_password: str = field(repr=False)
    postgres_db: str
    postgres_host: str
    postgres_port: int
    data_path: str
    nats_url: str
    stream: str
    subject_prefix: str
    base_consumer: str
    cdc_table: str | None
    listen_timeout_ms: int
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            postgres_user=_required("POSTGRES_USER"),
            postgres_password=_required("POSTGRES_PASSWORD"),
            postgres_db=_required("POSTGRES_DB"),
            postgres_host=_required("POSTGRES_HOST"),
            postgres_port=int(_required("POSTGRES_PORT")),
            data_path=_required("DUCKLAKE_DATA_PATH"),
            nats_url=_required("NATS_URL"),
            stream=_required("JETSTREAM_NAME"),
            subject_prefix=_required("JETSTREAM_SUBJECT_PREFIX"),
            base_consumer=_required("CDC_CONSUMER"),
            cdc_table=os.environ.get("CDC_TABLE") or None,
            listen_timeout_ms=_optional_int("CDC_LISTEN_TIMEOUT_MS", 1000),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        )


def _required_int(name: str) -> int:
    raw = _required(name)
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from None


@dataclass(frozen=True)
class ClientSettings:
    """Settings of a log client. It reads the log only, so it holds no database credentials."""

    nats_url: str
    stream: str
    subject_prefix: str
    table_id: int
    state_file: str
    log_level: str
    web_host: str
    web_port: int

    @classmethod
    def from_env(cls) -> "ClientSettings":
        return cls(
            nats_url=_required("NATS_URL"),
            stream=_required("JETSTREAM_NAME"),
            subject_prefix=_required("JETSTREAM_SUBJECT_PREFIX"),
            table_id=_required_int("CLIENT_TABLE_ID"),
            state_file=_required("CLIENT_STATE_FILE"),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            web_host=os.environ.get("WEB_HOST", "127.0.0.1"),
            web_port=_optional_int("WEB_PORT", 8000),
        )