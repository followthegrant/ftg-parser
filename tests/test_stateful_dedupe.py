import glob
from unittest import TestCase

import dataset
from followthemoney import model

from ftg import db, ftm, load, parse
from ftg.dedupe import authors as dedupe


class StatefulDedupTestCase(TestCase):
    """
    here we have access to the whole dataset (or a subset)
    we want to dedupe after parsing

    for the advanced dedupe we collect author triples afterwards and re-dedupe
    with a state to detect "overlapping" instititions or co-authors between
    authors with the same name fingerprint
    """

    def test_stateful_dedupe_triples(self):
        """
        simple deduping via triples:
            fingerprint,author_id,value (institution id or co-author id)

        we group by fingerprinted name as these are the candidates
        and check if they have overlapping values
        """
        # in this test, all alices are the same alice
        triples = (
            ("alice", "id1", "institute 1"),
            ("alice", "id2", "institute 1"),
            ("alice", "id2", "institute 2"),
            ("alice", "id3", "institute 2"),
            ("alice", "id3", "co-author bob"),
            ("alice", "id4", "co-author bob"),
            ("bob", "id5", "institute 1"),
        )

        res = [r for r in dedupe.dedupe_triples(triples)]
        self.assertSequenceEqual(
            sorted(res),
            [("id1", "id2"), ("id1", "id3"), ("id1", "id4")],
        )

        # here we have slightly differen alices
        triples = (
            ("alice", "id1", "institute 1"),
            ("alice", "id2", "institute 1"),
            ("alice", "id2", "institute 2"),
            ("alice", "id3", "institute 2"),
            ("alice", "id3", "co-author bob"),
            ("alice", "id4", "co-author bob"),
            ("alice", "id5", "institute 3"),
            ("alice", "id6", "institute 3"),
            ("bob", "id5", "institute 1"),
        )

        res = [r for r in dedupe.dedupe_triples(triples)]
        self.assertSequenceEqual(
            sorted(res),
            [("id1", "id2"), ("id1", "id3"), ("id1", "id4"), ("id5", "id6")],
        )

    def test_stateful_workflow(self):
        """
        we know in biorxiv testdata are duplicated authors
        """

        # 1) parse source data to get author triples
        triples = set()
        entities = set()
        for path in glob.glob("./testdata/biorxiv/*.xml"):
            data = load.pubmed(path)
            for d in data:
                article = parse.parse_article(d)
                for triple in dedupe.explode_triples(article):
                    triples.add(triple)
                for entity in ftm.make_entities(article):
                    if entity.schema.name in ("Person", "Membership"):
                        entities.add(entity)
                    if entity.schema.name == "Documentation":
                        role = entity.get("role")[0]
                        if role == "author" or "conflict of interest statement" in role:
                            entities.add(entity)

        # 2) get aggregated id pairs from triples
        pairs = dedupe.dedupe_triples(triples)

        # 3) insert into intermediate database
        with dataset.connect("sqlite:///:memory:") as conn:
            conn.query(
                """
                create table if not exists author_aggregation (
                  agg_id char(40) not null,
                  author_id char(40) not null,
                  unique (agg_id, author_id)
                )
                """
            )
            db.insert_many("author_aggregation", pairs, conn=conn)

            merged_entities = set()

            for entity in entities:
                merged_entity = dedupe.rewrite_entity(
                    "author_aggregation", entity.to_dict(), conn=conn
                )
                merged_entities.add(model.get_proxy(merged_entity))

        author_ids = set([a.id for a in entities if a.schema.name == "Person"])
        merged_author_ids = set(
            [a.id for a in merged_entities if a.schema.name == "Person"]
        )

        # there shozld be less different authors now
        self.assertGreater(len(author_ids), len(merged_author_ids))
        self.assertFalse(merged_author_ids - author_ids)

        members = set(
            [m.get("member")[0] for m in entities if m.schema.name == "Membership"]
        )
        merged_members = set(
            [
                m.get("member")[0]
                for m in merged_entities
                if m.schema.name == "Membership"
            ]
        )
        self.assertGreater(len(members), len(merged_members))
        self.assertFalse(merged_members - members)

        docs = set(
            [m.get("entity")[0] for m in entities if m.schema.name == "Documentation"]
        )
        merged_docs = set(
            [
                m.get("entity")[0]
                for m in merged_entities
                if m.schema.name == "Documentation"
            ]
        )
        self.assertGreater(len(docs), len(merged_docs))
        self.assertFalse(merged_docs - docs)
