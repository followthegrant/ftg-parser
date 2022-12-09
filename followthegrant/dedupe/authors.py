from collections import defaultdict
from functools import lru_cache
from itertools import combinations
from typing import Iterable, Iterator, Optional

import networkx as nx
import pandas as pd
from followthemoney.util import make_entity_id
from ftm_columnstore.query import Query

from ..logging import get_logger
from ..schema import ArticleFullOutput
from ..store import get_store

log = get_logger(__name__)


@lru_cache(maxsize=1024 * 1000 * 10)  # 10GB
def _get_fingerprint_id(fingerprint: str) -> str:
    return make_entity_id(fingerprint)


def explode_triples(article: ArticleFullOutput) -> Iterable[tuple[str, str, str]]:
    """
    generate author triples (+ property type) for institutions and co-authors:

    fingerprint,author_id,coauthor_id,"coauthor"
    fingerprint,author_id,institution_id,"affiliation"
    ...
    """
    for author in article.authors:
        if author.fingerprint:
            f = _get_fingerprint_id(author.fingerprint)
            for institution in author.institutions:
                yield f, author.id, institution.id, "affiliation"
            for coauthor in article.authors:
                if author.id != coauthor.id:
                    cf = _get_fingerprint_id(coauthor.fingerprint)
                    yield f, author.id, cf, "coauthor"


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
            canonical, *rest = sorted(components)
            yield canonical, canonical
            for r in rest:
                yield canonical, r


def dedupe_from_db(dataset: Optional[str] = None) -> Iterator[tuple[str, str, str]]:
    store = get_store()
    for triples in store.iterate_author_triple_packs(dataset=dataset):
        yield from dedupe_triples(triples)


def update_canonical(dataset: Optional[str] = None) -> pd.DataFrame:
    store = get_store()
    q = (
        Query(store.author_triples_table)
        .select("dataset, count(DISTINCT author_id) as candidates")
        .group_by("dataset")
    )
    if dataset is not None:
        q = q.where(dataset=dataset)
    df = store.driver.query_dataframe(q)
    for _dataset in df["dataset"]:
        ds = store.get_dataset(_dataset)
        for canonical_id, entity_id in dedupe_from_db(_dataset):
            ds.canonize(entity_id, canonical_id)
    # get result status
    q = (
        Query()
        .select(
            "dataset, count(DISTINCT entity_id) as deduped, count(DISTINCT canonical_id) as canonicals"
        )
        .where(schema="Person", origin="canonical")
        .group_by("dataset")
    )
    if dataset is not None:
        q = q.where(dataset=dataset)
    df_res = store.driver.query_dataframe(q)
    if df_res.empty:
        df_res = pd.DataFrame([{"dataset": dataset, "deduped": 0, "canonicals": 0}])
    df = df.merge(df_res, on="dataset")
    return df
