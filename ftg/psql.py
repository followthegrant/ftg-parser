from functools import lru_cache
from typing import Iterator, Optional

import dataset
from ftmstore.settings import DATABASE_URI


@lru_cache(maxsize=128)
def get_connection() -> dataset.database.Database:
    return dataset.connect(DATABASE_URI)


def insert_many(
    table: str, rows: Iterator[tuple], on_conflict: Optional[str] = "do nothing"
) -> None:
    """
    # FIXME
    this is a bit unstable for the query syntax
    """
    tmpl = "insert into {table} values ({values}) on conflict {on_conflict};"
    q = "\n".join(
        [
            tmpl.format(
                table=table,
                values=",".join([f"'{v}'" for v in row]),
                on_conflict=on_conflict,
            )
            for row in rows
        ]
    )
    with get_connection() as tx:
        tx.query(q)
