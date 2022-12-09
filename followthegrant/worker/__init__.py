from .base import BaseBatchWorker, BaseWorker
from .tasks import QUEUES  # noqa
from .tasks import (
    AUTHOR_TRIPLES,
    DELETE_SOURCE,
    MAP_FTM,
    PARSE,
    STORE_JSON,
    WRITE_AUTHOR_TRIPLES,
    WRITE_FTM,
)


class Worker(BaseWorker):
    queues = [PARSE, MAP_FTM, DELETE_SOURCE, STORE_JSON, AUTHOR_TRIPLES]


class BatchWorker(BaseBatchWorker):
    queues = [WRITE_FTM, WRITE_AUTHOR_TRIPLES]
