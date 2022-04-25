from functools import lru_cache
from typing import Iterator, Optional

import dataset
from ftmstore.settings import DATABASE_URI

from .logging import get_logger


log = get_logger(__name__)


@lru_cache(maxsize=128)
def get_connection() -> dataset.database.Database:
    return dataset.connect(DATABASE_URI)


class BulkWriter:
    TMPL = "INSERT INTO {table} VALUES {values} ON CONFLICT {on_conflict};"

    def __init__(
        self,
        table: str,
        conn: Optional[dataset.database.Database] = None,
        chunk_size: Optional[int] = 10000,
        on_conflict: Optional[str] = "do nothing",
    ):
        self.conn = conn or get_connection()
        self.table = table
        self.chunk_size = chunk_size
        self.on_conflict = on_conflict
        self.rows = []

    def add(self, row):
        self.rows.append(row)
        if len(self.rows) >= self.chunk_size:
            self.flush()

    def flush(self):
        values = ",\n".join([self._get_row(row) for row in self.rows])
        query = self.TMPL.format(
            table=self.table, values=values, on_conflict=self.on_conflict
        )
        with self.conn as tx:
            tx.query(query)
        log.info(f"Bulk inserted {len(self.rows)} rows into `{self.table}`")
        self.rows = []

    def _get_row(self, row):
        row = ",".join([f"'{v}'" for v in row])
        return f"({row})"


def insert_many(
    table: str,
    rows: Iterator[tuple],
    on_conflict: Optional[str] = "do nothing",
    conn: Optional[dataset.database.Database] = None,
    chunk_size: Optional[int] = 10000,
) -> None:
    """
    # FIXME
    this is a bit unstable for the query syntax
    """

    bulk = BulkWriter(table, conn, chunk_size, on_conflict)
    for row in rows:
        bulk.add(row)
    bulk.flush()
