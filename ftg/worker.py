import glob
import json
import multiprocessing
import threading
import os
from datetime import datetime

import pika
from ftmstore import get_dataset
from servicelayer.cache import get_redis
from servicelayer.jobs import Job, Stage, Dataset
from structlog import get_logger

from . import parse as parsers
from . import db, ftm, schema
from .dedupe.authors import explode_triples
from .util import cached_property


log = get_logger(__name__)

CRAWL = "crawl"
PARSE = "parse"
DELETE_SOURCE = "delete-source"
STORE_JSON = "store-json"
MAP_FTM = "map-ftm"
WRITE_FTM = "write-ftm"
AUTHOR_TRIPLES = "author-triples"
WRITE_AUTHOR_TRIPLES = "write-author-triples"

KV = get_redis()


def op_crawl(payload):
    pattern = payload.pop("fpath")
    for fp in glob.glob(pattern):
        yield {**payload, **{"fpath": fp}}


def op_parse(payload):
    parser = payload["parser"]
    fpath = payload["fpath"]
    parser = getattr(parsers, parser)
    for data in parser(fpath):
        yield {**payload, **{"data": data.dict()}}


def op_delete_source(payload):
    os.remove(payload["fpath"])


def op_store_json(payload):
    fp = os.path.join(payload["store_json"], payload["data"]["id"] + ".json")
    with open(fp, "w") as f:
        json.dump(payload["data"], f, default=lambda x: str(x), sort_keys=True)


def op_map_ftm(payload):
    data = schema.ArticleFullOutput(**payload.pop("data"))
    yield {**payload, **{"data": [e.to_dict() for e in ftm.make_entities(data)]}}


def op_write_ftm(payload):
    dataset = get_dataset(payload["dataset"])
    bulk = dataset.bulk()
    for entity in payload["data"]:
        bulk.put(entity)
    bulk.flush()


def op_author_triples(payload):
    data = schema.ArticleFullOutput(**payload.pop("data"))
    triples = set()
    for triple in explode_triples(data):
        triples.add(triple)
    yield {**payload, **{"data": list(triples)}}


def op_write_author_triples(payload):
    dataset = payload["dataset"]
    rows = [r + [dataset] for r in payload.pop("data")]
    if len(rows):
        db.insert_many("author_triples", rows)


QUEUES = {
    # stage: (func, *dispatch)
    CRAWL: (op_crawl, PARSE),
    PARSE: (op_parse, DELETE_SOURCE, MAP_FTM, AUTHOR_TRIPLES, STORE_JSON),
    DELETE_SOURCE: (op_delete_source, ),
    STORE_JSON: (op_store_json, ),
    MAP_FTM: (op_map_ftm, WRITE_FTM),
    AUTHOR_TRIPLES: (op_author_triples, WRITE_AUTHOR_TRIPLES),
    WRITE_FTM: (op_write_ftm, ),
    WRITE_AUTHOR_TRIPLES: (op_write_author_triples, )
}


def get_stage(queue, payload):
    dataset = payload["dataset"]
    job_id = payload["job_id"]
    job = Job(KV, dataset, job_id)
    return Stage(job, queue)


class Worker:
    X = "ftg.processing"

    def __init__(self, **options):
        self._threads = []
        self.num_threads = options.pop("threads", None) or multiprocessing.cpu_count()

    def dispatch(self, queue, payload, channel=None):
        if "job_id" not in payload:
            payload["job_id"] = f'{payload["dataset"]}-{datetime.now().isoformat()}'
        stage = get_stage(queue, payload)
        stage.queue()
        channel = channel or self.channel
        log.info(f'[{payload["dataset"]}] {payload["fpath"]} -> {queue.upper()}',
                 thread=threading.current_thread().name)
        payload = json.dumps(payload, default=lambda x: str(x))
        channel.basic_publish(
            exchange=self.X,
            routing_key=queue,
            body=payload
        )

    def handle(self, channel, method, properties, payload):
        queue = method.routing_key
        payload = json.loads(payload)
        stage = get_stage(queue, payload)
        stage._check_out()
        allowed_queues = payload.get("allowed_queues", list(QUEUES.keys()))
        log.info(f'[{payload["dataset"]}] {queue.upper()} : {payload["fpath"]}',
                 thread=threading.current_thread().name)
        func, *next_queues = QUEUES[queue]
        try:
            res = func(payload)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            stage.mark_done()
            if res is not None:
                for data in res:
                    for queue in next_queues:
                        if queue in allowed_queues:
                            self.dispatch(queue, data, channel)
        except Exception as e:
            log.error(f'[{payload["dataset"]}] {queue.upper()} < {payload["fpath"]}',
                      thread=threading.current_thread().name, exception=str(e))
            stage.mark_error()

    def start(self):
        def _start(channel):
            channel.start_consuming()

        if len(self._threads):
            for t in self._threads:
                t.exit()
            self._threads = []

        if self.num_threads > 1:
            for ix in range(self.num_threads):
                channel = self._create_channel()
                t = threading.Thread(name=f"{self.X}-{ix + 1}", target=_start, args=(channel,))
                log.info("Starting thread...", thread=t.name)
                t.start()
                self._threads.append(t)

            for t in self._threads:
                t.join()
        else:
            channel = self._create_channel()
            channel.start_consuming()

    def stop(self):
        for t in self._threads:
            t.join(10)
        self._threads = []

    @cached_property
    def channel(self):
        return self._create_channel()

    def _create_connection(self):
        return pika.BlockingConnection(pika.ConnectionParameters("localhost"))

    def _create_channel(self, connection=None):
        if connection is None:
            connection = self._create_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=self.X, exchange_type="direct")
        for queue in QUEUES:
            channel.queue_declare(queue=queue, durable=True)
            channel.queue_bind(exchange=self.X, queue=queue, routing_key=queue)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=queue, on_message_callback=self.handle)
        return channel

    @classmethod
    def get_status(cls):
        return Dataset.get_active_dataset_status(KV)
