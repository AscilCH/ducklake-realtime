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
            listen_timeout_ms=_optional_int("CDC_LISTEN_TIMEOUT_MS", 1000),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        )