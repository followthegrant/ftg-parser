from .tasks import DELETE_SOURCE, PARSE, QUEUES, STORE_JSON  # noqa
from .worker import BaseWorker


class Worker(BaseWorker):
    queues = QUEUES
