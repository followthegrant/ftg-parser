import os
from servicelayer import env


DEBUG = env.to_bool("DEBUG")
LOG_LEVEL = env.get("LOG_LEVEL", "warning")
DATA_ROOT = env.get("DATA_ROOT", os.path.join(os.getcwd(), "data"))
RABBITMQ_URL = env.get(
    "RABBITMQ_URL",
    "amqp://guest:guest@localhost:5672/%2F?heartbeat=3600&blocked_connection_timeout=3600",
)
PREFETCH_COUNT = env.to_int("RABBITMQ_PREFETCH_COUNT", 10000)
