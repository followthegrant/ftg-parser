from collections import defaultdict
from functools import lru_cache
from itertools import combinations
from typing import Iterable, Iterator

import networkx as nx
from dataset.table import Table
from followthemoney.util import make_entity_id

from ..db import get_connection
from ..schema import ArticleFullOutput


@lru_cache(maxsize=1024 * 1000 * 10)  # 10GB
def _get_fingerprint_id(fingerprint: str) -> str:
    return make_entity_id(fingerprint)


def explode_triples(article: ArticleFullOutput) -> Iterable[tuple[str, str, str]]:
    """
    generate author triples for institutions and co-authors:

    fingerprint,author_id,coauthor_id
    fingerprint,author_id,institution_id
    ...
    """
    for author in article.authors:
        if author.fingerprint:
            f = _get_fingerprint_id(author.fingerprint)
            for institution in author.institutions:
                yield f, author.id, institution.id
            for coauthor in article.authors:
                if author.id != coauthor.id:
                    cf = _get_fingerprint_id(coauthor.fingerprint)
                    yield f, author.id, cf


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

        # generate graph from connected pairs
        G = nx.Graph()
        G.add_edges_from(res)
        for components in nx.connected_components(G):
            base, *rest = sorted(components)
            for r in rest:
                yield base, r


def dedupe_db(
    table: str, fingerprint: str, dataset: str = None, conn=None
) -> Iterator[tuple[str, str, str]]:
    if conn is None:
        conn = get_connection()
    with conn as tx:
        table = tx[table]
        triples = set()

        if dataset is not None:
            rows = table.find(fingerprint=fingerprint, dataset=dataset)
        else:
            rows = table.find(fingerprint=fingerprint)

        for row in rows:
            triples.add((row["fingerprint"], row["author_id"], row["value_id"]))

        if triples:
            yield from dedupe_triples(triples)


@lru_cache(maxsize=1024 * 1000 * 10)  # 10GB
def _get_aggregated_id(table: Table, author_id: str, dataset: str = None) -> str:
    if dataset is not None:
        res = table.find_one(agg_id=author_id, dataset=dataset)
    else:
        res = table.find_one(agg_id=author_id)

    if res:
        return author_id

    if dataset is not None:
        res = table.find_one(author_id=author_id, dataset=dataset)
    else:
        res = table.find_one(author_id=author_id)

    if res:
        return res["agg_id"]

    return author_id


AUTHOR_ROLES = (
    "author",
    "individual conflict of interest statement",
    "individual acknowledgement statement",
)


def rewrite_entity(table: str, entity: dict, dataset: str = None, conn=None) -> dict:
    """
    rewrite author ids for `entity` fetched from generated pairs table
    """
    if entity["schema"] not in ("Person", "Membership", "Documentation"):
        return entity

    if conn is None:
        conn = get_connection()

    with conn as tx:
        table = tx[table]

        if entity["schema"] == "Person":
            entity["id"] = _get_aggregated_id(table, entity["id"], dataset)
            return entity

        if entity["schema"] == "Membership":
            author_ids = entity["properties"]["member"]
            entity["properties"]["member"] = [
                _get_aggregated_id(table, author_id, dataset)
                for author_id in author_ids
            ]
            return entity

        if entity["schema"] == "Documentation":
            role = entity["properties"]["role"][0]
            if role in AUTHOR_ROLES:
                author_ids = entity["properties"]["entity"]
                entity["properties"]["entity"] = [
                    _get_aggregated_id(table, author_id, dataset)
                    for author_id in author_ids
                ]
                return entity

        return entity
