from functools import lru_cache
from typing import Iterable, Iterator, Optional

from ftm_columnstore import get_dataset as _get_dataset
from ftm_columnstore.driver import ClickhouseDriver, get_driver, table_exists
from ftm_columnstore.query import Query

from .db import insert_many


AUTHOR_TRIPLES = """
CREATE TABLE {table}
(
    `fingerprint_id` FixedString(40),
    `author_id` FixedString(40),
    `value_id` FixedString(40),
    `prop_type` LowCardinality(String),
    `dataset` LowCardinality(String),
    PROJECTION {table}_px
    (
        SELECT *
        ORDER BY
            fingerprint_id,
            author_id,
            dataset
    ),
    PROJECTION {table}_tpx
    (
        SELECT *
        ORDER BY
            prop_type
    )
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (dataset, fingerprint_id, author_id)
ORDER BY (dataset, fingerprint_id, author_id, value_id)
"""


class Store:
    def __init__(
        self, driver: Optional[ClickhouseDriver] = None, prefix: Optional[str] = "ftg"
    ):
        self.driver = driver or get_driver()
        self.prefix = prefix
        self.author_triples_table = f"{prefix}_author_triples"

    def init(self, recreate: Optional[bool] = False):
        self.driver.init(exists_ok=True, recreate=recreate)
        self.create_table(self.author_triples_table, AUTHOR_TRIPLES, recreate)

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
        return _get_dataset(dataset, driver=self.driver)

    def write_author_triples(self, rows: Iterator[Iterable[str]]) -> int:
        columns = ["fingerprint_id", "author_id", "value_id", "prop_type", "dataset"]
        insert_many(self.author_triples_table, columns, rows, driver=self.driver)

    def iterate_author_triple_packs(
        self, dataset: Optional[str] = None
    ) -> Iterator[str]:
        """yield triples for deduping by fingerprint chunks"""
        q = (
            Query("ftg_author_triples")
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


@lru_cache(maxsize=128)
def get_store(driver: Optional[ClickhouseDriver] = None) -> Store:
    return Store(driver=driver)
