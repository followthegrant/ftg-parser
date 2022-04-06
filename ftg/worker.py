import glob
import json
import logging
import multiprocessing
import os

import pika
from ftmstore import get_dataset

from . import parse as parsers
from . import db, ftm, schema
from .dedupe.authors import explode_triples


log = logging.getLogger(__name__)

CRAWL = "crawl"
PARSE = "parse"
DELETE_SOURCE = "delete-source"
STORE_JSON = "store-json"
MAP_FTM = "map-ftm"
WRITE_FTM = "write-ftm"
AUTHOR_TRIPLES = "author-triples"
WRITE_AUTHOR_TRIPLES = "write-author-triples"


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
    if payload.pop("delete_source", False):
        os.remove(payload["fpath"])


def op_store_json(payload):
    if payload.get("store_json") is not None:
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


STAGES = {
    # stage: (func, *dispatch)
    CRAWL: (op_crawl, PARSE),
    PARSE: (op_parse, DELETE_SOURCE, MAP_FTM, AUTHOR_TRIPLES),
    DELETE_SOURCE: (op_delete_source, ),
    STORE_JSON: (op_store_json, ),
    MAP_FTM: (op_map_ftm, WRITE_FTM),
    AUTHOR_TRIPLES: (op_author_triples, WRITE_AUTHOR_TRIPLES),
    WRITE_FTM: (op_write_ftm, ),
    WRITE_AUTHOR_TRIPLES: (op_write_author_triples, )
}


class Worker:
    QUEUE = "ftg"

    def __init__(self, **options):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=self.QUEUE, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.QUEUE, on_message_callback=self.handle)
        self.channel = channel
        self.options = options
        self.num_threads = options.pop("threads", None) or multiprocessing.cpu_count()

    def dispatch(self, stage, payload):
        payload["stage"] = stage
        log.debug(f'[{payload["dataset"]}] {payload["fpath"]} -> {stage.upper()}')
        payload = json.dumps(payload, default=lambda x: str(x))
        self.channel.basic_publish(
            exchange="",
            routing_key=self.QUEUE,
            body=payload
        )

    def handle(self, channel, method, properties, payload):
        payload = json.loads(payload)
        stage = payload.pop("stage")
        log.info(f'[{payload["dataset"]}] {stage.upper()} < {payload["fpath"]}')
        func, *next_stages = STAGES[stage]
        try:
            res = func(payload)
            if res is not None:
                for data in res:
                    for stage in next_stages:
                        self.dispatch(stage, data)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            log.error(f'[{payload["dataset"]}] {stage.upper()} < {payload["fpath"]}')
            log.error("cannot handle: ", str(e))

    def consume(self):
        self.channel.start_consuming()
        # for ix in range(self.num_threads):
        #     p = multiprocessing.Process(target=self.channel.start_consuming)
        #     p.start()
        #     p.join()
        # print("threads started", ix + 1)
