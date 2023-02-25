import os
import time
from functools import cache

from followthemoney.util import make_entity_id
from structlog import get_logger

from followthegrant.dedupe import explode_triples
from followthegrant.parse import parse
from followthegrant.store import get_dataset, get_store
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
        self.key = make_entity_id(self.dataset, self.job_id, self.queue)

    def __str__(self):
        return f"[{self.dataset}] {self.queue.upper()}"


@cache
def get_stage(dataset: str, job_id: str, queue: str) -> Stage:
    return Stage(dataset, job_id, queue)


PARSE = "parse"
WRITE_FTM = "write-ftm"
WRITE_AUTHOR_TRIPLES = "write-author-triples"


def op_parse(payload):
    parser = payload["parser"]
    dataset = payload["dataset"]
    fpath = get_path(payload["fpath"])
    # some entities are a lot of json data, so we emit them 1 by 1 to avoid
    # rabbitmq message size limit
    for proxy in parse(fpath, parser, dataset):
        yield WRITE_FTM, {**payload, **{"data": [proxy.to_dict()]}}
        triples = set()
        for triple in explode_triples(proxy):
            triples.add(triple)
        if triples:
            yield WRITE_AUTHOR_TRIPLES, {**payload, **{"data": list(triples)}}
    # if configured, delete source file to free up disk space
    if payload.get("delete_source"):
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


def op_write_author_triples(dataset, rows):
    rows = (r + [dataset] for r in rows)
    store = get_store()
    store.write_triples(rows)


QUEUES = {
    # stage: (func, batch_size, *next_stages)
    PARSE: (op_parse, 1, WRITE_FTM, WRITE_AUTHOR_TRIPLES),
    WRITE_FTM: (op_write_ftm, 10_000),
    WRITE_AUTHOR_TRIPLES: (op_write_author_triples, 10_000),
}


class BulkWriter:
    """collect tasks to bulk write"""

    def __init__(self, consumer, stage: Stage):
        self.is_flushing = False
        self.consumer = consumer
        self.stage = stage
        self.tasks = []
        func, batch_size, *next_queues = QUEUES[stage.queue]
        self.func = func
        self.batch_size = batch_size
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

            for delivery_tag, payload in self.tasks:
                for item in payload["data"]:
                    to_write.append(item)
                self.consumer.ack(delivery_tag)
                done += 1

            if len(to_write):
                try:
                    self.func(self.stage.dataset, to_write)
                except Exception as e:
                    self.handle_error(e)
                    done = 0
                    errors += 1

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
