from functools import cache
from typing import Iterable, Iterator, Optional

from ftm_columnstore import get_dataset as _get_dataset
from ftm_columnstore.driver import ClickhouseDriver, get_driver, table_exists
from ftm_columnstore.query import Query
from ftm_columnstore.store import Store as FtmCStore

from .db import insert_many

TRIPLES = """
CREATE TABLE {table}
(
    `entity_id` String,
    `prop` LowCardinality(String),
    `value` String,
    `dataset` LowCardinality(String),
    PROJECTION {table}_px (SELECT * ORDER BY prop),
    PROJECTION {table}_dx (SELECT * ORDER BY dataset)
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (entity_id, prop, value)
"""


class Store:
    def __init__(
        self, driver: Optional[ClickhouseDriver] = None, prefix: Optional[str] = "ftg"
    ):
        self.driver = driver or get_driver()
        self.prefix = prefix
        self.triples_table = f"{prefix}_triples"
        self.init()

    def init(self, recreate: Optional[bool] = False):
        self.driver.init(exists_ok=True, recreate=recreate)
        self.create_table(self.triples_table, TRIPLES, recreate)

    def create_table(self, table: str, query: str, recreate: Optional[bool] = False):
        query = query.format(table=table)
        if recreate:
            self.drop_table(table)
        try:
            self.driver.execute(query)
        except Exception as e:
            if not table_exists(e, table):
                raise e

    def drop_table(self, table: str):
        self.driver.execute(f"DROP TABLE IF EXISTS {table}")

    def get_dataset(self, dataset: str):
        ds = _get_dataset(dataset, driver=self.driver)
        return ds.store

    def write_triples(self, rows: Iterator[Iterable[str]]) -> int:
        columns = ["entity_id", "prop", "value", "dataset"]
        insert_many(self.triples_table, columns, rows, driver=self.driver)

    def iterate_triple_packs(self, dataset: Optional[str] = None) -> Iterator[str]:
        """yield triples for deduping by phonetic chunks"""
        q = (
            Query(self.triples_table)
            .select(
                "fingerprint_id, count(*) as count, groupArray(author_id), groupArray(value_id)"
            )
            .group_by("fingerprint_id")
            .having(count__gt=1)
        )
        if dataset is not None:
            q = q.where(dataset=dataset)
        for fp, _, author_ids, value_ids in q:
            triples = set()
            for author_id, value_id in zip(author_ids, value_ids):
                triples.add((fp, author_id, value_id))
            yield triples


@cache
def get_store(driver: Optional[ClickhouseDriver] = None) -> Store:
    return Store(driver=driver)


@cache
def get_dataset(name: str, driver: Optional[ClickhouseDriver] = None) -> FtmCStore:
    store = get_store(driver)
    return store.get_dataset(name)
