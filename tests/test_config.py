import pytest

from realtime.config import ConfigError, Settings

REQUIRED = {
    "POSTGRES_USER": "u",
    "POSTGRES_PASSWORD": "p",
    "POSTGRES_DB": "d",
    "POSTGRES_HOST": "h",
    "POSTGRES_PORT": "5433",
    "DUCKLAKE_DATA_PATH": "data/lake/",
    "NATS_URL": "nats://localhost:4222",
    "JETSTREAM_NAME": "CHANGES",
    "JETSTREAM_SUBJECT_PREFIX": "changes",
    "CDC_CONSUMER": "orders_sink",
}


@pytest.fixture
def env(monkeypatch):
    for k in list(REQUIRED) + ["CDC_LISTEN_TIMEOUT_MS", "LOG_LEVEL"]:
        monkeypatch.delenv(k, raising=False)
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    return monkeypatch


def test_defaults(env):
    s = Settings.from_env()
    assert s.listen_timeout_ms == 1000
    assert s.log_level == "INFO"


def test_missing_required_raises(env):
    env.delenv("NATS_URL")
    with pytest.raises(ConfigError, match="NATS_URL"):
        Settings.from_env()


def test_timeout_override(env):
    env.setenv("CDC_LISTEN_TIMEOUT_MS", "250")
    assert Settings.from_env().listen_timeout_ms == 250


def test_timeout_invalid(env):
    env.setenv("CDC_LISTEN_TIMEOUT_MS", "abc")
    with pytest.raises(ConfigError):
        Settings.from_env()


def test_password_not_in_repr(env):
    assert "p'" not in repr(Settings.from_env())