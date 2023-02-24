import os
import time
from functools import cached_property, lru_cache

from followthemoney.util import make_entity_id
from structlog import get_logger

from followthegrant import settings
from followthegrant.dedupe import explode_triples
from followthegrant.parse import parse
from followthegrant.store import get_dataset, get_store
from followthegrant.transform import make_proxies
from followthegrant.util import get_path

log = get_logger(__name__)


class Stage:
    """
    a stage is a wrapper to organize tasks based on a queue name, dataset name and job id
    """

    def __init__(self, dataset: str, job_id: str, queue: str):
        self.dataset = dataset
        self.job_id = job_id
        self.queue = queue

    def __str__(self):
        return f"[{self.dataset}] {self.queue.upper()}"

    @cached_property
    def key(self):
        """unique identifier"""
        return make_entity_id(self.dataset, self.job_id, self.queue)


@lru_cache(maxsize=1024)
def get_stage(dataset: str, job_id: str, queue: str) -> Stage:
    return Stage(dataset, job_id, queue)


PARSE = "parse"
DELETE_SOURCE = "delete-source"
WRITE_FTM = "write-ftm"
AUTHOR_TRIPLES = "author-triples"
WRITE_AUTHOR_TRIPLES = "write-author-triples"


def op_parse(payload):
    parser = payload["parser"]
    dataset = payload["dataset"]
    fpath = get_path(payload["fpath"])
    # some entities are a lot of json data, so we emit them 1 by 1 to avoid
    # rabbitmq message size limit
    for proxy in parse(fpath, parser, dataset):
        yield {**payload, **{"data": [proxy.to_dict()]}}


def op_delete_source(payload):
    try:
        os.remove(get_path(payload["fpath"]))
    except (OSError, FileNotFoundError):
        pass


def op_write_ftm(dataset, entities):
    dataset = get_dataset(dataset)
    bulk = dataset.bulk()
    for entity in entities:
        bulk.put(entity)
    bulk.flush()
    # yield from entities


def op_author_triples(payload):
    data = make_proxies(payload["data"])
    triples = set()
    for triple in explode_triples(data):
        triples.add(triple)
    yield {**payload, **{"data": list(triples)}}


def op_write_author_triples(dataset, rows):
    rows = (r + [dataset] for r in rows)
    store = get_store()
    store.write_author_triples(rows)


QUEUES = {
    # stage: (func, batch_size, *next_stages)
    PARSE: (op_parse, 1, WRITE_FTM, DELETE_SOURCE),
    DELETE_SOURCE: (op_delete_source, 1),
    WRITE_FTM: (op_write_ftm, 10_000, AUTHOR_TRIPLES),
    AUTHOR_TRIPLES: (op_author_triples, 10_000, WRITE_AUTHOR_TRIPLES),
    WRITE_AUTHOR_TRIPLES: (op_write_author_triples, 10_000),
}


class TaskAggregator:
    """handle a collection of identical tasks"""

    def __init__(self, consumer, stage: Stage):
        self.is_flushing = False
        self.consumer = consumer
        self.stage = stage
        self.tasks = []
        func, batch_size, *next_queues = QUEUES[stage.queue]
        self.func = func
        self.batch_size = batch_size
        self.next_queues = next_queues
        self.is_writer = self.stage.queue.startswith("write")
        self.last_activity = time.time()

    def add(self, tag, payload):
        self.tasks.append((tag, payload))
        self.last_activity = time.time()
        self.flush()

    def flush(self):
        if self.should_flush():
            self.is_flushing = True
            log.info(f"{self.stage} : running {len(self.tasks)} tasks...")
            done = 0
            errors = 0
            to_write = []
            # to_dispatch = []

            for delivery_tag, payload in self.tasks:
                next_queues = set(payload.get("allowed_queues", QUEUES.keys())) & set(
                    self.next_queues
                )
                try:
                    if self.is_writer:
                        for item in payload["data"]:
                            to_write.append(item)
                    else:
                        res = self.func(payload)
                        if res is not None:
                            for payload in res:
                                for queue in next_queues:
                                    # to_dispatch.append((queue, payload))
                                    self.consumer.dispatch(queue, payload)
                    done += 1
                    self.consumer.ack(delivery_tag)
                except Exception as e:
                    msg = f'{self.stage} : {e} : {payload["fpath"]}'
                    log.error(msg)
                    if settings.DEBUG:
                        log.exception(msg, payload=payload, exception=e)
                    errors += 1
                    self.consumer.nack(delivery_tag, requeue=False)

            if self.is_writer and len(to_write):
                try:
                    self.func(self.stage.dataset, to_write)
                except Exception as e:
                    self.handle_error(e)
                    done = 0
                    errors = 0

            # if len(to_dispatch):
            #     for queue, payload in to_dispatch:
            #         self.consumer.dispatch(queue, payload)

            if done:
                log.info(f"{self.stage} : {done} tasks successful.")
            if errors:
                log.warning(f"{self.stage} : {errors} tasks failed.")

            # reset basket
            self.tasks = []
            self.is_flushing = False
            self.last_activity = time.time()

    def handle_error(self, e):
        """re-queue all the tasks in current batch"""
        e = str(e)[:1000]  # don't pollute logs too much
        log.warning(f"{self.stage} : Aggregated tasks failed ({e}). Will retry...")
        for _, task in self.tasks:
            self.consumer.retry_task(self.stage.queue, task)

    def should_flush(self):
        """make sure we can and should flush"""
        if self.is_flushing:
            return False
        if not self.tasks:
            return False
        if not self.consumer.is_active:
            return False
        if len(self.tasks) >= self.batch_size:
            return True
        if time.time() - self.last_activity > 5:
            return True
        return False
