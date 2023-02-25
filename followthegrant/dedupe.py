from collections import defaultdict
from itertools import combinations
from typing import Iterable, Iterator, Optional

import networkx as nx
import pandas as pd
from followthemoney.types import registry
from ftm_columnstore.query import Query

from .logging import get_logger
from .model import CE, SKDict, make_proxy
from .store import get_store

log = get_logger(__name__)


def explode_triples(proxy: SKDict | CE) -> Iterable[tuple[str, str, str]]:
    """
    generate triples useful for deduping for institutions and authors, using
    interval schemata and identifiers:

    entity_id,<role>,other_id
    entity_id,<identifier>,value
    ...
    """
    proxy = make_proxy(proxy)
    if proxy.schema.is_a("Thing"):
        for prop, value in proxy.itervalues():
            if prop.type == registry.identifier:
                yield proxy.id, prop.name, value

    if proxy.schema.is_a("Membership"):
        for row in zip(
            proxy.get("member"), proxy.get("role"), proxy.get("organization")
        ):
            yield row

    if proxy.schema.is_a("Employment"):
        for row in zip(proxy.get("employee"), proxy.get("role"), proxy.get("employer")):
            yield row

    if proxy.schema.is_a("Documentation"):
        if "AUTHORSHIP" in proxy.get("role"):
            for row in zip(
                proxy.get("entity"), proxy.get("role"), proxy.get("document")
            ):
                yield row


def dedupe_triples(
    triples: Iterable[tuple[str, str, str]]
) -> Iterable[tuple[str, str]]:
    """
    dedupe things based on triples

    input:
        triples from entities (1st column id) that are candidates within a
        deduping block
    """

    items: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for source, rel, target in triples:
        items[rel][source].add(target)

    rels: dict[tuple, str] = {}
    for rel, sources in items.items():
        # generate matching pairs
        res: set[tuple] = set()
        for a1, a2 in combinations(sources.items(), 2):
            if a1[1] & a2[1]:  # they share some target values
                pair = (a1[0], a2[0])  # matching source id pair
                res.add(pair)  # store result
                rels[pair] = rel  # keep track of which relation is this pair from

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
