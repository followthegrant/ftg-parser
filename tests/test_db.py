from unittest import TestCase

import dataset

from ftg import db


class DbTestCase(TestCase):
    maxDiff = None

    def test_psql_insert_many(self):
        with dataset.connect("sqlite:///:memory:") as cx:
            cx.query("drop table if exists test_author_triples")
            cx.query(
                """create table test_author_triples (
              fingerprint char(40) not null,
              author_id char(40) not null,
              value_id char(40) not null,
              unique (fingerprint, author_id, value_id)
            )"""
            )

            values = (
                tuple(v.split(","))
                for v in """761e66733ec8d4f7eef83f6ae3f3376a17082510,d959af2032d6e58d5dc6a6b780b85fed3d342bb6,5eaef8e8465e7c50293841084e9280f20f394f09
761e66733ec8d4f7eef83f6ae3f3376a17082510,d959af2032d6e58d5dc6a6b780b85fed3d342bb6,40c21531eb06c0da246281ac1f077fc52e24a721
761e66733ec8d4f7eef83f6ae3f3376a17082510,d959af2032d6e58d5dc6a6b780b85fed3d342bb6,40c21531eb06c0da246281ac1f077fc52e24a721
761e66733ec8d4f7eef83f6ae3f3376a17082510,d959af2032d6e58d5dc6a6b780b85fed3d342bb6,d1ee8d88e05112308cebecdabdd004a1f0641181
761e66733ec8d4f7eef83f6ae3f3376a17082510,d959af2032d6e58d5dc6a6b780b85fed3d342bb6,d1ee8d88e05112308cebecdabdd004a1f0641181""".split()
            )

            db.insert_many("test_author_triples", values, conn=cx)
