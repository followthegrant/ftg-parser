from collections import defaultdict
from functools import lru_cache
from itertools import combinations
from typing import Iterable, Iterator, Optional

import fingerprints
from dataset.table import Table
from followthemoney.util import make_entity_id

from ..db import get_connection
from ..schema import ArticleFullOutput


@lru_cache(maxsize=1024000)  # 1GB
def _get_fingerprint(name: str) -> str:
    return make_entity_id(fingerprints.generate(name))


def explode_triples(article: ArticleFullOutput) -> Iterable[tuple[str, str, str]]:
    """
    generate author triples for institutions and co-authors:

    fingerprint,author_id,coauthor_id
    fingerprint,author_id,institution_id
    ...
    """
    for author in article.authors:
        f = _get_fingerprint(author.name)
        if f:
            for institution in author.institutions:
                yield f, author.id, institution.id
            for coauthor in article.authors:
                if author.id != coauthor.id:
                    yield f, author.id, coauthor.id


def dedupe_triples(
    triples: Iterable[tuple[str, str, str]]
) -> Iterable[tuple[str, str]]:
    """
    dedupe authors based on triples
    """

    authors = defaultdict(lambda: defaultdict(set))
    for f, author_id, value_id in triples:
        authors[f][author_id].add(value_id)

    # work per fingerprint chunks
    for fingerprint, authors in authors.items():
        # generate matching pairs
        res = set()
        for a1, a2 in combinations(authors.items(), 2):
            if a1[1] & a2[1]:  # they share some institutions or co-authors
                res.add((a1[0], a2[0]))

        # then dedupe via id pairs
        # (if a = b and b = c then also a = c)
        merged = defaultdict(set)
        for a1, a2 in combinations(sorted(res), 2):
            a1s, a2s = set(a1), set(a2)
            if a1s & a2s:
                base, *rest = sorted(a1s | a2s)
                for basebase, values in merged.items():
                    if base in values:
                        base = basebase
                        break
                merged[base].update(rest)
                # remove from result
                res.discard(a1)
                res.discard(a2)

        for base, values in merged.items():
            for v in sorted(values):
                yield base, v

        # some leftovers
        for x in res:
            yield tuple(sorted(x))


def dedupe_db(
    table: str, source: Optional[str] = None, conn=None
) -> Iterator[tuple[str, str, str]]:
    if conn is None:
        conn = get_connection()
    with conn as tx:
        table = tx[table]
        triples = set()
        current_f = None
        if source is not None:
            rows = table.find(source=source, order_by="fingerprint")
        else:
            rows = table.find(order_by="fingerprint")
        for row in rows:
            if row["fingerprint"] != current_f:
                # first flush triples
                yield from dedupe_triples(triples)
                triples = set()
                current_f = row["fingerprint"]
            triples.add((row["fingerprint"], row["author_id"], row["value_id"]))
        if triples:
            yield from dedupe_triples(triples)


@lru_cache(maxsize=1024000)  # 1GB
def _get_aggregated_id(table: Table, author_id: str) -> str:
    res = table.find_one(agg_id=author_id)
    if res:
        return author_id
    res = table.find_one(author_id=author_id)
    if not res:
        return author_id
    return res["agg_id"]


def rewrite_entity(table: str, entity: dict, conn=None) -> dict:
    """
    rewrite author ids for `entity` fetched from generated pairs table
    """
    if entity["schema"] not in ("Person", "Membership", "Documentation"):
        return entity

    if conn is None:
        conn = get_connection()

    table = conn[table]

    if entity["schema"] == "Person":
        entity["id"] = _get_aggregated_id(table, entity["id"])
        return entity

    if entity["schema"] == "Membership":
        author_id = entity["properties"]["member"][0]
        entity["properties"]["member"] = [_get_aggregated_id(table, author_id)]
        return entity

    if entity["schema"] == "Documentation":
        role = entity["properties"]["role"][0]
        if role == "author" or "conflict of interest statement" in role:
            author_id = entity["properties"]["entity"][0]
            entity["properties"]["entity"] = [_get_aggregated_id(table, author_id)]
            return entity

    return entity
