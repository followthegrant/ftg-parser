from .base import BaseBatchWorker, BaseWorker
from .tasks import QUEUES  # noqa
from .tasks import AUTHOR_TRIPLES, DELETE_SOURCE, PARSE, WRITE_AUTHOR_TRIPLES, WRITE_FTM


class Worker(BaseWorker):
    queues = [PARSE, DELETE_SOURCE, AUTHOR_TRIPLES]


class BatchWorker(BaseBatchWorker):
    queues = [WRITE_FTM, WRITE_AUTHOR_TRIPLES]
