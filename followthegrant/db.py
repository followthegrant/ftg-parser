from typing import Any, Iterable, Iterator, Optional

import pandas as pd
from ftm_columnstore.driver import ClickhouseDriver, get_driver

from .logging import get_logger

log = get_logger(__name__)


class BulkWriter:
    """write bulk data to clickhouse table"""

    def __init__(
        self,
        table: str,
        columns: Iterable[str],
        driver: Optional[ClickhouseDriver] = None,
        chunk_size: Optional[int] = 100_000,
    ):
        self.driver = driver or get_driver()
        self.table = table
        self.columns = columns
        self.chunk_size = chunk_size
        self.rows = []

    def put(self, row):
        self.rows.append(row)
        if len(self.rows) >= self.chunk_size:
            self.flush()

    def flush(self):
        if self.rows:
            df = pd.DataFrame(self.rows, columns=self.columns)
            self.driver.insert(df, self.table)
            log.info(f"Bulk inserted {len(self.rows)} rows into `{self.table}`")
        self.rows = []


def insert_many(
    table: str,
    columns: Iterable[str],
    rows: Iterator[Iterable[Any]],
    driver: Optional[ClickhouseDriver] = None,
    chunk_size: Optional[int] = 100_000,
) -> None:
    bulk = BulkWriter(table, columns, driver, chunk_size)
    for row in rows:
        bulk.put(row)
    bulk.flush()
