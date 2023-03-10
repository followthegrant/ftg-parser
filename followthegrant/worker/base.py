from functools import cached_property

import pika
from structlog import get_logger

from followthegrant import settings

from .consumer import BatchConsumer, Consumer, basic_publish

log = get_logger(__name__)


class BaseWorker:
    consumer_class = Consumer
    queues = []
    consumer_kwargs = {"prefetch_count": 5}

    def __init__(self, queues=None):
        self.queues = queues or self.queues

    def run(self):
        self.consumer = self.consumer_class(
            settings.RABBITMQ_URL,
            exchange=settings.EXCHANGE,
            queues=self.queues,
            **self.consumer_kwargs,
        )
        self.consumer.run()

    def shutdown(self):
        self.publish_channel.cancel()
        if self._connection is not None:
            self._connection.close()

    def dispatch(self, queue, payload):
        basic_publish(queue, payload, self.publish_channel, should_log=True)

    @cached_property
    def publish_channel(self):
        """a base channel for dispatching without asynchrous consumer running"""
        connection = pika.BlockingConnection(pika.URLParameters(settings.RABBITMQ_URL))
        channel = connection.channel()
        channel.exchange_declare(
            exchange=settings.EXCHANGE, exchange_type="direct", durable=True
        )
        for queue in self.queues:
            channel.queue_declare(
                queue=queue, durable=True, exclusive=False, auto_delete=False
            )
            channel.queue_bind(
                exchange=settings.EXCHANGE, queue=queue, routing_key=queue
            )
        self._connection = connection
        return channel


class BaseBatchWorker(BaseWorker):
    consumer_class = BatchConsumer

    def __init__(self, queues=None, heartbeat=5, batch_size=10_000):
        super().__init__(queues)
        self.batch_size = batch_size
        self.consumer_kwargs = {
            "heartbeat": heartbeat,
            "prefetch_count": len(self.queues) * self.batch_size + len(self.queues),
        }

    def flush(self):
        """flush all aggregators in consumer"""
        if self.consumer is not None:
            self.consumer.flush()
