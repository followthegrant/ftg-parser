from typing import Iterable
from collections import defaultdict
from itertools import combinations

import fingerprints
from followthemoney.util import make_entity_id

from ..schema import ArticleFullOutput
from ..psql import get_connection


def explode_triples(article: ArticleFullOutput) -> Iterable[tuple[str, str, str]]:
    """
    generate author triples for institutions and co-authors:

    fingerprint,author_id,coauthor_id
    fingerprint,author_id,institution_id
    ...
    """
    for author in article.authors:
        f = make_entity_id(fingerprints.generate(author.name))
        if f:
            for institution in author.institutions:
                yield f, author.id, institution.id
            for coauthor in article.authors:
                if author.id != coauthor.id:
                    yield f, author.id, coauthor.id


def dedupe(triples: Iterable) -> Iterable[tuple[str, str]]:
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

        for a, b in res:
            yield sorted((a, b))

        # more dedupe via id pairs
        # (if a = b and b = c then also a = c)
        for a1, a2 in combinations(res, 2):
            match = set(a1) & set(a2)
            if match:
                m = match.pop()
                for i in (set(a1) | set(a2)) - {m}:
                    yield sorted((m, i))


def dedupe_psql(table, source=None, conn=None):
    if conn is None:
        conn = get_connection()
    table = conn[table]
    triples = set()
    current_f = None
    if source is not None:
        rows = table.find(source=source, order_by="fingerprint")
    else:
        rows = table.find(order_by="fingerprint")
    for row in rows:
        if row["fingerprint"] != current_f:
            # first flush triples
            yield from dedupe(triples)
            triples = set()
            current_f = row["fingerprint"]
        triples.add((row["fingerprint"], row["author_id"], row["value_id"]))
    if triples:
        yield from dedupe(triples)
