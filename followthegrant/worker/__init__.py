from .base import BaseBatchWorker, BaseWorker
from .tasks import PARSE, WRITE_AUTHOR_TRIPLES, WRITE_FTM


class Worker(BaseWorker):
    queues = [PARSE]


class BatchWorker(BaseBatchWorker):
    queues = [WRITE_FTM, WRITE_AUTHOR_TRIPLES]
