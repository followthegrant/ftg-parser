import csv
import glob
from unittest import TestCase

import dataset
from followthemoney import model
from ftmstore import get_dataset
from ftmstore.settings import DATABASE_URI

from ftg import db, ftm, parse
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
            ("bob", "id", "institute 1"),
        )

        res = [r for r in dedupe.dedupe_triples(triples)]
        self.assertSequenceEqual(
            sorted(res),
            [("id1", "id2"), ("id1", "id3"), ("id1", "id4"), ("id5", "id6")],
        )

        # more real world
        with open("./testdata/author_triples.txt") as f:
            reader = csv.reader(f, delimiter="\t")
            triples = [r[:3] for r in reader]

        self.assertEqual(len(triples), 21127)
        res = [r for r in dedupe.dedupe_triples(triples)]
        self.assertEqual(len(res), 1084)

        # deduped 1084 different authors to 30:
        self.assertEqual(len(set([r[0] for r in res])), 30)
        self.assertEqual(len(set([r[1] for r in res])), 1084)

    def test_stateful_workflow(self):
        """
        we know in biorxiv testdata are duplicated authors
        """

        # 1) parse source data to get author triples
        triples = set()
        entities = set()
        for path in glob.glob("./testdata/biorxiv/*.xml"):
            data = parse.jats(path)
            for article in data:
                for triple in dedupe.explode_triples(article):
                    triples.add(triple)
                for entity in ftm.make_entities(article):
                    if entity.schema.name in ("Person", "Membership"):
                        entities.add(entity)
                    if entity.schema.name == "Documentation":
                        role = entity.get("role")[0]
                        if (
                            role == "author"
                            or "infividual conflict of interest statement" in role
                        ):
                            entities.add(entity)

        # 2) get aggregated id pairs from triples
        pairs = dedupe.dedupe_triples(triples)

        # 3) insert into intermediate database
        with dataset.connect(DATABASE_URI) as conn:
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
                merged_entity = dedupe.rewrite_entity(entity.to_dict(), conn=conn)
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

    def test_stateful_dedupe_triples_fingerprint(self):
        triples = set()
        entities = set()
        for path in glob.glob("./testdata/biorxiv/*.xml"):
            data = parse.jats(path)
            for article in data:
                for triple in dedupe.explode_triples(article):
                    triples.add(triple)
                for entity in ftm.make_entities(article):
                    if entity.schema.name in ("Person", "Membership"):
                        entities.add(entity)
                    if entity.schema.name == "Documentation":
                        role = entity.get("role")[0]
                        if (
                            role == "author"
                            or "infividual conflict of interest statement" in role
                        ):
                            entities.add(entity)

        merged_entities = set()
        with dataset.connect(DATABASE_URI) as conn:
            conn.query(
                """
                create table if not exists author_triples (
                  fingerprint char(40) not null,
                  author_id char(40) not null,
                  value_id char(40) not null,
                  unique (fingerprint, author_id, value_id)
                )
                """
            )
            conn.query(
                """
                create table if not exists author_aggregation (
                  agg_id char(40) not null,
                  author_id char(40) not null,
                  unique (agg_id, author_id)
                )
                """
            )
            db.insert_many("author_triples", triples, conn=conn)

            for fp in conn.query("select distinct fingerprint from author_triples"):
                fp = fp["fingerprint"]
                pairs = dedupe.dedupe_db("author_triples", fp, conn=conn)
                db.insert_many("author_aggregation", pairs, conn=conn)

            for entity in entities:
                merged_entity = dedupe.rewrite_entity(entity.to_dict(), conn=conn)
                merged_entities.add(model.get_proxy(merged_entity))

        author_ids = set([a.id for a in entities if a.schema.name == "Person"])
        merged_author_ids = set(
            [a.id for a in merged_entities if a.schema.name == "Person"]
        )

        # there shozld be less different authors now
        self.assertGreater(len(author_ids), len(merged_author_ids))
        self.assertFalse(merged_author_ids - author_ids)

    def test_stateful_dedupe_rewrite_inplace(self):
        triples = set()
        entities = set()
        for path in glob.glob("./testdata/biorxiv/*.xml"):
            data = parse.jats(path)
            for article in data:
                for triple in dedupe.explode_triples(article):
                    triples.add(triple)
                for entity in ftm.make_entities(article):
                    entities.add(entity)

        merged_entities = set()
        with dataset.connect(DATABASE_URI) as conn:
            conn.query(
                """
                create table if not exists author_triples (
                  fingerprint char(40) not null,
                  author_id char(40) not null,
                  value_id char(40) not null,
                  unique (fingerprint, author_id, value_id)
                )
                """
            )
            conn.query(
                """
                create table if not exists author_aggregation (
                  agg_id char(40) not null,
                  author_id char(40) not null,
                  unique (agg_id, author_id)
                )
                """
            )
            db.insert_many("author_triples", triples, conn=conn)

            for fp in conn.query("select distinct fingerprint from author_triples"):
                fp = fp["fingerprint"]
                pairs = dedupe.dedupe_db("author_triples", fp, conn=conn)
                db.insert_many("author_aggregation", pairs, conn=conn)

            # add entities to ftm store
            ftm_dataset = get_dataset("ftg_test")
            bulk = ftm_dataset.bulk()
            for entity in entities:
                bulk.put(entity)
            bulk.flush()

            entities = [e for e in ftm_dataset.iterate()]

        # commit
        with dataset.connect(DATABASE_URI) as conn:
            aggregations = dedupe.get_aggregation_mapping(conn=conn)
            to_merge = dedupe.get_entities_to_rewrite(ftm_dataset, aggregations)

            for entity in to_merge:
                merged_entity = dedupe.rewrite_entity_inplace(
                    ftm_dataset, entity["id"], conn=conn
                )
                merged_entities.add(model.get_proxy(merged_entity))

        author_ids = set([a.id for a in entities if a.schema.name == "Person"])
        merged_author_ids = set(
            [a.id for a in merged_entities if a.schema.name == "Person"]
        )

        # there shozld be less different authors now
        self.assertGreater(len(author_ids), len(merged_author_ids))
        self.assertFalse(merged_author_ids - author_ids)

        # there should be less entities in ftm store now:
        merged_entities = [e for e in ftm_dataset.iterate()]
        self.assertGreater(len(entities), len(merged_entities))
