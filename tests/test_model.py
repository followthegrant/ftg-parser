import datetime
from unittest import TestCase

import fingerprints
from nomenklatura.entity import CompositeEntity
from pydantic.error_wrappers import ValidationError

from followthegrant import model


class ModelTestCase(TestCase):
    maxDiff = None

    def test_journal(self):
        """
        most of the behaviour tested applies to all models
        """

        data = {"name": "PLoS ONE"}
        journal = model.Journal(**data)
        self.assertListEqual(journal.get_id_parts(), ["one plos"])  # fingerprinting
        self.assertEqual(journal.id, "journal-45a6bd84130dd5fbbaefba02664a8e5ee014934a")
        self.assertIsInstance(journal.proxy, CompositeEntity)
        self.assertDictEqual(
            {
                "id": "journal-45a6bd84130dd5fbbaefba02664a8e5ee014934a",
                "properties": {"name": ["PLoS ONE"], "summary": ["journal"]},
                "schema": "Thing",
                "datasets": [],
                "referents": [],
            },
            journal.proxy,
        )

        # all props are multi valued and defaults to empty set
        self.assertIsInstance(journal.name, set)
        self.assertEqual(journal.name, {"PLoS ONE"})
        self.assertEqual(journal.website, set())
        # except id of course
        self.assertIsInstance(journal.id, str)
        self.assertEqual(journal.id, journal.proxy.id)

        # invalid ftm prop
        import ipdb

        ipdb.set_trace()

        # prop cleaning on init, always ensure set
        name = {"PLoS ONE"}
        journal = Journal(name="PLoS ONE")
        self.assertEqual(journal.name, name)
        journal = Journal(name=["PLoS ONE"])
        self.assertEqual(journal.name, name)
        journal = Journal(name={"PLoS ONE"})
        self.assertEqual(journal.name, name)
        journal = Journal(name="PLoS  ONE ")  # collapse spaces
        self.assertEqual(journal.name, name)
        journal = Journal(
            name=["PLoS ONE", "", " ", None, "-", "."]
        )  # filter falsish values
        self.assertEqual(journal.name, name)

        # with identifier
        data = {"ident": "journal-1", "name": "PLoS ONE"}
        journal = model.Journal(**data)
        self.assertListEqual(journal.get_id_parts(), ["journal-1"])
        self.assertEqual(journal.id, "journal-462ddec499bdd34aed79dbdce5bb6acb0586e3de")

        # data validation
        with self.assertRaises(ValidationError):
            model.Journal()
        with self.assertRaises(ValidationError):
            model.Journal(**{"name": None})

    def test_institution(self):
        data = {
            "name": "Verseon Corporation, Fremont, California, United States of America"
        }
        institution = model.Institution(**data)
        self.assertIsInstance(institution.input, model.InstitutionInput)
        self.assertIsInstance(institution.output, model.InstitutionOutput)
        self.assertEqual(institution.id, "a9090414a28a7fefc37335edb92d46f07ac99582")
        self.assertEqual(institution.country, "us")
        self.assertDictEqual(
            {
                "name": "Verseon Corporation, Fremont, California, United States of America",  # noqa
                "id": "a9090414a28a7fefc37335edb92d46f07ac99582",
                "country": "us",
            },
            institution.serialize(),
        )
        data = {"name": "University of Parma, ITALY"}
        institution = model.Institution(**data)
        self.assertDictEqual(
            {
                "name": "University of Parma, ITALY",
                "id": "a72f1eecf2ae3dbd791ec2d94d85268f57c5de53",
                "country": "it",
            },
            institution.serialize(),
        )

        # data validation
        with self.assertRaises(ValidationError):
            model.Institution()
        with self.assertRaises(ValidationError):
            model.Institution(**{"name": None})

    def test_authors(self):
        data = {"name": "Alice Smith"}
        author = model.Author(**data)
        self.assertIsInstance(author.input, model.AuthorInput)
        self.assertIsInstance(author.output, model.AuthorOutput)
        # without any identification context author ids are randomized based on uuid4
        # self.assertEqual(author.id, "29f97601cce345abc9795620fb48eb83a4c15e8c")

        # identification hints (e.g. a journal id)
        data = {"name": "Alice Smith", "identifier_hints": ["journalId123"]}
        author = model.Author(**data)
        self.assertListEqual(author.get_id_parts(), ["alice smith", "journalId123"])

        # with institutions
        data = {
            "name": "Alice Smith",
            "institutions": "University of Parma, ITALY",  # -> has to be `InstitutionInput`
        }
        with self.assertRaises(ValidationError):
            author = model.Author(**data)
        data = {
            "name": "Alice Smith",
            "institutions": [{"name": "University of Parma, ITALY"}],
        }
        author = model.Author(**data)
        self.assertIn(author.institutions[0].id, author.get_id_parts())
        self.assertEqual(author.id, "b3505d345dbc447b44a7c83e19e6d3e01162ede9")
        self.assertIn("it", author.countries)
        self.assertEqual(author.fingerprint, fingerprints.generate("Alice Smith"))

    def test_article(self):
        data = {
            "title": "Reversible covalent direct thrombin inhibitors",
            "abstract": "Introduction In recent years, the traditional...",
            "published_at": "2018-02-08",
            "journal": {"name": "PLoS ONE"},
            "authors": [
                {
                    "name": "Mohanram Sivaraja",
                },
                {"name": "Nicola Pozzi"},
            ],
            "keywords": [
                "Research Article",
                "Biology and Life Sciences",
                "Biochemistry",
            ],
        }
        article = model.Article(**data)
        self.assertIsInstance(article.input, model.ArticleInput)
        self.assertIsInstance(article.output, model.ArticleOutput)

        # without any identifiers, article title and journal id is used for id creation
        self.assertEqual(article.journal.id, "45a6bd84130dd5fbbaefba02664a8e5ee014934a")
        self.assertEqual(article.id, "a6bc0d02711e5a486f6954c598193eab9ee3d9cb")
        self.assertIn(article.journal.id, article.get_id_parts())
        self.assertIn(article.input.title, article.get_id_parts())
        self.assertDictEqual(
            {
                "id": "a6bc0d02711e5a486f6954c598193eab9ee3d9cb",
                "title": "Reversible covalent direct thrombin inhibitors",
                "published_at": datetime.date(2018, 2, 8),
                "abstract": "Introduction In recent years, the traditional...",
                "journal": {
                    "id": "45a6bd84130dd5fbbaefba02664a8e5ee014934a",
                    "name": "PLoS ONE",
                    "website": None,
                },
                "authors": [
                    {
                        "id": "60ecf3f6d39e3c634c00d218aed4cddc8298b237",
                        "name": "Mohanram Sivaraja",
                        "fingerprint": "mohanram sivaraja",
                        "first_name": "Mohanram",
                        "last_name": "Sivaraja",
                        "middle_names": None,
                        "institutions": [],
                        "countries": [],
                    },
                    {
                        "id": "0ccb67aa5446279a76a38d711ccc7241c50cce55",
                        "name": "Nicola Pozzi",
                        "fingerprint": "nicola pozzi",
                        "first_name": "Nicola",
                        "last_name": "Pozzi",
                        "middle_names": None,
                        "institutions": [],
                        "countries": [],
                    },
                ],
                "index_text": None,
            },
            article.serialize(),
        )

        # article ids
        identifiers = {"pmid": "p1", "pmcid": "pmc1", "doi": "doi42"}
        i1, i2, i3 = [
            model.ArticleIdentifier(**i)
            for i in [{"key": k, "value": v} for k, v in identifiers.items()]
        ]
        self.assertIsInstance(i1.input, model.ArticleIdentifierInput)
        self.assertIsInstance(i1.output, model.ArticleIdentifierOutput)
        self.assertDictEqual(
            {
                "id": "16e60e9ef370e30b9ebe134512ff1fcf39584d8a",
                "key": "pmid",
                "label": "PubMed ID",
                "value": "p1",
            },
            i1.serialize(),
        )

        data["identifiers"] = identifiers
        article = model.Article(**data)
        self.assertEqual(article.id, "e9eaa49b62a0e9aa6090d5ccf30f6f12ebc80d40")
        self.assertTupleEqual(article.get_id_parts(), ("pmid", "p1"))

        # id migration pmc -> pmcid
        identifiers = {"pmid": "p1", "pmc": "pmc1", "doi": "doi42"}
        data["identifiers"] = identifiers
        article = model.Article(**data)
        self.assertEqual(article.id, "e9eaa49b62a0e9aa6090d5ccf30f6f12ebc80d40")
        self.assertTupleEqual(article.get_id_parts(), ("pmid", "p1"))

    def test_article_cois(self):
        data = {
            "title": "Reversible covalent direct thrombin inhibitors",
            "abstract": "Introduction In recent years, the traditional...",
            "published_at": "2018-02-08",
            "journal": {"name": "PLoS ONE"},
            "authors": [
                {
                    "name": "Mohanram Sivaraja",
                },
                {"name": "Nicola Pozzi"},
            ],
            "keywords": [
                "Research Article",
                "Biology and Life Sciences",
                "Biochemistry",
            ],
            "coi_statement": """Competing Interests: \nMS, MR, KL, TPS, DMC,
                LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon
                Corporation. NP discloses a financial interest in Hemadvance, LLC
                and funding through AHA grant AHA15SDG25550094. This does not alter
                our adherence to PLOS ONE policies on sharing data and
                materials.""",
        }
        article = model.Article(**data)
        self.assertIsInstance(article.coi_statement, model.CoiStatement)
        self.assertIsInstance(article.coi_statement.output, model.CoiStatementOutput)
        cois = article.individual_coi_statements
        self.assertEqual(len(cois), 2)
        self.assertIsInstance(cois[0], model.CoiStatement)
        self.assertIsInstance(cois[0].output, model.CoiStatementOutput)
        self.assertDictEqual(
            {
                "id": "1432f3f55a7dc130cd3f04ceaac8014e1da7b9e9",
                "article_id": "a6bc0d02711e5a486f6954c598193eab9ee3d9cb",
                "article_title": "Reversible covalent direct thrombin inhibitors",
                "author_id": "60ecf3f6d39e3c634c00d218aed4cddc8298b237",
                "author_name": "Mohanram Sivaraja",
                "journal_name": "PLoS ONE",
                "role": "individual conflict of interest statement",
                "title": "individual conflict of interest statement (Mohanram Sivaraja)",
                "text": "Competing Interests: MS, MR, KL, TPS, DMC, LI, SC, PZ, MAE, KMS, DCW, AD, and DBK are employees of Verseon Corporation. This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                "published_at": datetime.date(2018, 2, 8),
                "flag": 1,
                "index_text": "flag:1",
            },
            cois[0].serialize(),
        )
        self.assertDictEqual(
            {
                "id": "782c01d5c57b76515b642c0eb692b6c57cb87d0d",
                "article_id": "a6bc0d02711e5a486f6954c598193eab9ee3d9cb",
                "article_title": "Reversible covalent direct thrombin inhibitors",
                "author_id": "0ccb67aa5446279a76a38d711ccc7241c50cce55",
                "author_name": "Nicola Pozzi",
                "journal_name": "PLoS ONE",
                "role": "individual conflict of interest statement",
                "title": "individual conflict of interest statement (Nicola Pozzi)",
                "text": "NP discloses a financial interest in Hemadvance, LLC and funding through AHA grant AHA15SDG25550094. This does not alter our adherence to PLOS ONE policies on sharing data and materials.",
                "published_at": datetime.date(2018, 2, 8),
                "flag": 1,
                "index_text": "flag:1",
            },
            cois[1].serialize(),
        )

    def test_identifiers(self):
        identifiers = {"pmid": "123"}
        identifier = model.ArticleIdentifier(**{"key": "pmid", "value": 123})
        data = {
            "title": "Reversible covalent direct thrombin inhibitors",
            "abstract": "Introduction In recent years, the traditional...",
            "published_at": "2018-02-08",
            "journal": {"name": "PLoS ONE"},
            "identifiers": identifiers,
        }
        article = model.Article(**data)
        self.assertEqual(article.id, identifier.get_article_id())

    def test_pmcid(self):
        identifier1 = model.ArticleIdentifier(**{"key": "pmcid", "value": 123})
        identifier2 = model.ArticleIdentifier(**{"key": "pmcid", "value": "PMC123"})
        self.assertEqual(identifier1.id, identifier2.id)
        self.assertEqual(identifier1.get_article_id(), identifier2.get_article_id())

    def test_id_shorthand(self):
        self.assertEqual(
            model.Journal.make_id(name="Plos ONE"),
            "45a6bd84130dd5fbbaefba02664a8e5ee014934a",
        )
        self.assertEqual(
            model.Institution.make_id(
                name="Verseon Corporation, Fremont, California, United States of America"
            ),
            "a9090414a28a7fefc37335edb92d46f07ac99582",
        )
