import json
from collections import Counter
from functools import lru_cache

import pika
from structlog import get_logger

from followthegrant import settings

from ..tasks import QUEUES, TaskAggregator, get_stage
from .base import ReconnectingPikaConsumer
from .heartbeat import ReconnectingHeartbeatPikaConsumer

log = get_logger(__name__)


def basic_publish(queue, payload, channel, should_log=False):
    props = pika.BasicProperties(content_type="application/json", delivery_mode=2)
    if should_log:
        log.info(f'[{payload["dataset"]}] {payload["fpath"]} -> {queue.upper()}')
    payload = json.dumps(payload, default=lambda x: str(x))
    msg_size = len(payload.encode("utf-8"))
    if msg_size > 500000000:
        log.error(
            f"Message too large ({msg_size})",
            dataset=payload["dataset"],
            fpath=payload["fpath"],
        )
    else:
        try:
            channel.basic_publish(
                exchange=settings.EXCHANGE,
                routing_key=queue,
                body=payload,
                mandatory=True,
                properties=props,
            )
        except pika.exceptions.UnroutableError:
            log.error("Message could not be confirmed", msg=payload)


class _FTGConsumer:
    QUEUES = QUEUES
    MAX_RETRIES = 3

    def dispatch(self, queue, payload):
        basic_publish(queue, payload, self.channel)

    def retry_task(self, queue, payload):
        retries = payload.get("retries", 0)
        if retries <= self.MAX_RETRIES:
            payload["retries"] = retries + 1
            self.dispatch(queue, payload)
        else:
            e = f"Max retries ({self.MAX_RETRIES}) exceeded."
            self.handle_error(e, payload, queue)

    def get_next_queues(self, payload, next_queues):
        """make sure to dispatch only to active queues"""
        return set(payload.get("allowed_queues", self.QUEUES.keys())) & set(next_queues)

    def handle_result(self, res, next_queues):
        if res is not None:
            for payload in res:
                for queue in next_queues:
                    self.dispatch(queue, payload)

    def handle_error(self, e, payload, queue):
        msg = f'[{payload["dataset"]}] {queue.upper()} : {e} : {payload["fpath"]}'
        log.error(msg)
        if settings.DEBUG:
            log.exception(msg, payload=payload, exception=e)

    def get_stage(self, queue, payload):
        return get_stage(payload["dataset"], payload["job_id"], queue)


class Consumer(ReconnectingPikaConsumer, _FTGConsumer):
    handled_tasks = 0
    done = Counter()
    errors = Counter()

    def on_message(self, channel, method, properties, payload):
        """receive a message and handle a task"""
        log.debug("Recieving message", tag=method.delivery_tag)
        queue = method.routing_key
        payload = json.loads(payload)
        stage = self.get_stage(queue, payload)
        func, *next_queues = self.QUEUES[queue]
        next_queues = self.get_next_queues(payload, next_queues)

        res = func(payload)

        try:
            self.handle_result(res, next_queues)
            self.ack(method.delivery_tag)
            self.done[stage] += 1
        except Exception as e:
            self.handle_error(e, payload, queue)
            self.nack(method.delivery_tag, requeue=False)
            self.errors[stage] += 1
        self.handled_tasks += 1

        if self.handled_tasks % 100 == 0:
            log.info(f"Handled {self.handled_tasks} tasks.")
            for stage, tasks in self.done.items():
                # stage.mark_done(tasks)  # FIXME
                self.done[stage] = 0
            for stage, tasks in self.errors.items():
                # stage.mark_error(tasks)  # FIXME
                self.errors[stage] = 0


class BatchConsumer(ReconnectingHeartbeatPikaConsumer, _FTGConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._aggregators = {}

    def on_message(self, channel, method, properties, payload):
        """receive a message"""
        log.debug("Recieving message", tag=method.delivery_tag)
        queue = method.routing_key
        payload = json.loads(payload)
        stage = self.get_stage(queue, payload)
        aggregator = self._get_aggregator(stage)
        aggregator.add(method.delivery_tag, payload)

    def on_heartbeat(self, channel, method, properties, payload):
        """receive a heartbeat to run periodic tasks"""
        self.flush()

    def flush(self):
        """flush all task aggregators"""
        if self.is_active:
            aggregators = self._aggregators.values()
            for aggregator in aggregators:
                if aggregator.should_flush():
                    log.info(
                        f"Flushing {aggregator.stage}: {len(aggregator.tasks)} tasks"
                    )
                aggregator.flush()

    @lru_cache(maxsize=1024)
    def _get_aggregator(self, stage):
        """get task aggregator per job and stage"""
        if stage.key not in self._aggregators:
            self._aggregators[stage.key] = TaskAggregator(self, stage)
        return self._aggregators[stage.key]
