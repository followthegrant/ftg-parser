import json
import time
from functools import lru_cache

import pika
from servicelayer.cache import get_redis
from servicelayer.jobs import Dataset
from structlog import get_logger

from ftg import settings
from ftg.util import cached_property

from .consumer import ReconnectingPikaConsumer
from .tasks import TaskAggregator, get_stage

log = get_logger(__name__)


KV = get_redis()
EXCHANGE = "ftg.processing"


def basic_publish(queue, payload, channel, should_log=False):
    stage = get_stage(queue, dataset=payload["dataset"], job_id=payload["job_id"])
    stage.queue()
    if should_log:
        log.info(f'[{payload["dataset"]}] {payload["fpath"]} -> {queue.upper()}')
    payload = json.dumps(payload, default=lambda x: str(x))
    props = pika.BasicProperties(content_type="application/json", delivery_mode=1)
    try:
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=queue,
            body=payload,
            mandatory=True,
            properties=props,
        )
    except pika.exceptions.UnroutableError:
        log.error("Message could not be confirmed", msg=payload)


class Consumer(ReconnectingPikaConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._aggregators = {}
        self.last_heartbeat = time.time()

    def on_message(self, channel, method, properties, payload):
        """receive a message"""
        log.debug("Recieving message", tag=method.delivery_tag)
        queue = method.routing_key
        payload = json.loads(payload)
        stage = get_stage(queue, dataset=payload["dataset"], job_id=payload["job_id"])
        aggregator = self._get_aggregator(stage)
        aggregator.add(method.delivery_tag, payload)

    def on_heartbeat(self, channel, method, properties, payload):
        """receive a heartbeat to run periodic tasks"""
        self.flush()

    def dispatch(self, queue, payload):
        basic_publish(queue, payload, self.channel)

    def flush(self):
        """flush all task aggregators"""
        if self.is_active:
            log.info("Flushing all aggregators...")
            aggregators = self._aggregators.values()
            for aggregator in aggregators:
                aggregator.flush()

    @lru_cache(maxsize=1024)
    def _get_aggregator(self, stage):
        """get task aggregator per job and stage"""
        key = f"{stage.job.id}-{stage.stage}"
        if key not in self._aggregators:
            self._aggregators[key] = TaskAggregator(self, stage)
        return self._aggregators[key]

    @property
    def is_active(self):
        if self.connection.is_closed:
            return False
        if self.channel is None:
            return False
        return self._consumer._consuming


class BaseWorker:
    queues = []

    def __init__(self, heartbeat=0):
        self.consumer = None
        self.heartbeat = heartbeat

    def start(self):
        self.consumer = Consumer(
            settings.RABBITMQ_URL,
            exchange=EXCHANGE,
            queues=list(self.queues.keys()),
            prefetch_count=10000,
            heartbeat=self.heartbeat,
        )
        self.consumer.run()

    def flush(self):
        """flush all aggregators in consumer"""
        if self.consumer is not None:
            self.consumer.flush()

    def dispatch(self, queue, payload):
        basic_publish(queue, payload, self.publish_channel, should_log=True)

    @cached_property
    def publish_channel(self):
        """a base channel for dispatching without asynchrous consumer running"""
        connection = pika.BlockingConnection(pika.URLParameters(settings.RABBITMQ_URL))
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE, exchange_type="direct", durable=True
        )
        for queue in self.queues:
            channel.queue_declare(
                queue=queue, durable=True, exclusive=False, auto_delete=False
            )
            channel.queue_bind(exchange=EXCHANGE, queue=queue, routing_key=queue)
        return channel

    @classmethod
    def get_status(cls):
        return Dataset.get_active_dataset_status(KV)
