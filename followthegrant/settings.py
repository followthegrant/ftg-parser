import os
from pathlib import Path

from banal.bools import as_bool


def get_env(name, default=None):
    value = os.environ.get(name)
    if value is not None:
        return str(value)
    if default is not None:
        return str(default)


DEBUG = as_bool(get_env("DEBUG"))
LOG_LEVEL = get_env("LOG_LEVEL", "warning")
DATA_ROOT = Path(get_env("DATA_ROOT", os.path.join(os.getcwd(), "data")))
RABBITMQ_URL = get_env(
    "RABBITMQ_URL",
    "amqp://guest:guest@localhost:5672/%2F?heartbeat=3600&blocked_connection_timeout=3600",
)
EXCHANGE = "ftg.exchange"
