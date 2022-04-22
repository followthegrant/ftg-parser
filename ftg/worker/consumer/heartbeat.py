import logging
import time
import uuid
import pika

from .base import PikaConsumer, ReconnectingPikaConsumer

log = logging.getLogger(__name__)


class HeartbeatPikaConsumer(PikaConsumer):
    """Based on the example consumer but with an additional heartbeat
    queue that reminds the consumer to run periodic tasks.
    """

    def __init__(
        self,
        amqp_url,
        exchange,
        queues,
        on_message_cb,
        prefetch_count=1,
        heartbeat=0,
        on_heartbeat_cb=None,
    ):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self.on_heartbeat_cb = on_heartbeat_cb
        self.heartbeat = heartbeat
        self.heartbeat_queue = None
        self.last_heartbeat = time.time()
        self.queues = self.preconfigure_queues(queues)
        super().__init__(amqp_url, exchange, queues, on_message_cb, prefetch_count)

    def preconfigure_queues(self, queues):
        """configure basic kwargs for queues and an exclusive heartbeat
        queue if required"""
        configured_queues = super().preconfigure_queues(queues)
        if self.heartbeat > 0:
            self.heartbeat_queue = f"heartbeat-{uuid.uuid4()}"
            configured_queues[self.heartbeat_queue] = {
                "durable": False,
                "exclusive": True,
                "auto_delete": True,
            }
        return configured_queues

    def start_consuming(self):
        log.info("Issuing consumer related RPC commands")
        self.add_on_cancel_callback()
        for queue in self._binded_queues:
            if queue == self.heartbeat_queue:
                consumer_tag = self._channel.basic_consume(
                    queue, self.on_heartbeat_cb_safe, auto_ack=True
                )
            else:
                consumer_tag = self._channel.basic_consume(queue, self.on_message_cb)
            self._consumer_tags.append(consumer_tag)
        self.was_consuming = True
        self._consuming = True
        if self.heartbeat > 0:
            self.last_heartbeat = time.time()
            self.schedule_heartbeat()

    def schedule_heartbeat(self):
        if self.heartbeat > 0 and self._connection.is_open:
            self._connection.ioloop.call_later(self.heartbeat, self.send_heartbeat)

    def send_heartbeat(self):
        if self.heartbeat > 0 and self._consuming:
            self._channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.heartbeat_queue,
                body=str(time.time()),
                mandatory=False,
                properties=pika.BasicProperties(
                    delivery_mode=1, content_type="text/plain"
                ),
            )
        self.schedule_heartbeat()

    def on_heartbeat_cb_safe(self, *args, **kwargs):
        """Ignore missed heartbeats"""
        log.info("<3")
        if time.time() - self.last_heartbeat >= self.heartbeat:
            self.on_heartbeat_cb(*args, **kwargs)
            self.last_heartbeat = time.time()


class ReconnectingHeartbeatPikaConsumer(ReconnectingPikaConsumer):
    consumer_class = HeartbeatPikaConsumer

    def __init__(self, *args, **kwargs):
        super().__init__(*args, on_heartbeat_cb=self.on_heartbeat, **kwargs)

    def on_heartbeat(self, channel, method, properties, payload):
        """receive a heartbeat to run periodic tasks"""
        raise NotImplementedError
