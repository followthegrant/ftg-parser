from collections import defaultdict
from functools import lru_cache
from itertools import combinations
from typing import Iterable, Iterator, Optional

import networkx as nx
import pandas as pd
from dataset.database import Database
from followthemoney.proxy import EntityProxy
from followthemoney.util import make_entity_id
from ftmstore.dataset import Dataset
from sqlalchemy.sql.expression import cast

from ..db import get_connection
from ..logging import get_logger
from ..schema import ArticleFullOutput

log = get_logger(__name__)


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
    table: str,
    fingerprint: str,
    dataset: Optional[str] = None,
    conn: Optional[Database] = None,
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


@lru_cache(1024 * 1000)  # 1GB
def get_aggregation_mapping(
    conn: Optional[Database] = None,
    table: Optional[str] = "author_aggregation",
    dataset: Optional[str] = None,
) -> dict:
    """the dict returned is < 500MB for the full PUBMED CENTRAL (according to `sys.getsizeof()`)"""

    if conn is None:
        conn = get_connection()

    log.info("Loading author aggregation mapping...", table=table, dataset=dataset)

    q = f"SELECT * FROM {table}"
    if dataset is not None:
        q += f" WHERE dataset = '{dataset}'"

    df = pd.read_sql(q, conn.engine)
    df = df.set_index("author_id")
    data = df["agg_id"].T.to_dict()

    log.info(f"Loaded {len(data)} author aggregations.", table=table, dataset=dataset)

    return data


AUTHOR_ROLES = (
    "author",
    "individual conflict of interest statement",
    "individual acknowledgement statement",
)


def rewrite_entity(
    entity: dict,
    table: Optional[str] = "author_aggregation",
    dataset: Optional[str] = None,
    conn: Optional[Database] = None,
) -> dict:
    """
    rewrite author ids for `entity` fetched from generated pairs table
    """
    if conn is None:
        conn = get_connection()

    if entity["schema"] not in ("Person", "Membership", "Documentation"):
        return entity

    mapping = get_aggregation_mapping(conn, table, dataset)

    if entity["schema"] == "Person":
        entity["id"] = mapping.get(entity["id"], entity["id"])
        return entity

    if entity["schema"] == "Membership":
        author_ids = entity["properties"]["member"]
        entity["properties"]["member"] = [
            mapping.get(author_id, author_id) for author_id in author_ids
        ]
        return entity

    if entity["schema"] == "Documentation":
        role = entity["properties"]["role"][0]
        if role in AUTHOR_ROLES:
            author_ids = entity["properties"]["entity"]
            entity["properties"]["entity"] = [
                mapping.get(author_id, author_id) for author_id in author_ids
            ]
            return entity

    return entity


def rewrite_entity_inplace(
    dataset: Dataset, entity_id: str, conn: Optional[Database] = None
) -> dict:
    """rewrite an entity in the ftm store"""
    entity = dataset.get(entity_id)
    if entity is not None:
        new_entity = rewrite_entity(entity.to_dict(), conn=conn)
        if new_entity != entity.to_dict():  # FIXME better comparison ?
            dataset.delete(entity_id=entity_id)
            dataset.put(new_entity)
        return new_entity


def get_entities_to_rewrite(
    dataset: Dataset, aggregations: Optional[dict] = None
) -> Iterator[EntityProxy]:
    """yield entities from ftm store that need to be rewritten"""

    json_type = dataset.table.columns.entity.type
    roles = [cast(r, json_type) for r in AUTHOR_ROLES]
    entity = dataset.table.c.entity
    q_authors = dataset.table.select(entity["schema"] == cast("Person", json_type))
    q_memberships = dataset.table.select(
        entity["schema"] == cast("Membership", json_type)
    )
    q_relations = (
        dataset.table.select()
        .filter(entity["schema"] == cast("Documentation", json_type))
        .filter(entity["role"][0].in_(roles))
    )

    if aggregations is not None:
        author_ids = aggregations.keys()
        q_authors = q_authors.where(dataset.table.c.id.in_(author_ids))
        author_ids = [cast(i, json_type) for i in author_ids]
        q_memberships = q_memberships.where(entity["member"][0].in_(author_ids))
        q_relations = q_relations.where(entity["entity"][0].in_(author_ids))

    def _res():
        yield from q_authors.execute()
        yield from q_memberships.execute()
        yield from q_relations.execute()

    for data in _res():
        yield dict(data)
