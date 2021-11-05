import glob
from collections import Counter
from unittest import TestCase

from ftg import parse, load, ftm, schema
from ftg.mapping import MappedModel


class ModelTestCase(TestCase):
    maxDiff = None

    def test_ftm_model(self):
        path = "./testdata/pubmed/PMC4844427/opth-10-713.nxml"  # an article with all required data
        data = load.pubmed(path)
        data = parse.parse_article(data)
        data = MappedModel(data)

        self.assertIsInstance(data.publisher, schema.PublisherFtm)
        self.assertDictEqual(
            {
                "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                "journal_id": "9d03ad6a37bbf4dec4878d21ab6f2f9afc3e66ae",
            },
            data.publisher.dict(),
        )
        self.assertIsInstance(data.article, schema.ArticleFtm)
        self.assertDictEqual(
            {
                "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                "article_title": "Ectasia risk factors in refractive surgery",
                "article_published_at": "2016-04-20",
                "article_abstract": "This review outlines risk factors of post-laser in situ keratomileusis (LASIK) ectasia that can be detected preoperatively and presents a new metric to be considered in the detection of ectasia risk. Relevant factors in refractive surgery screening include the analysis of intrinsic biomechanical properties (information obtained from corneal topography/tomography and patient’s age), as well as the analysis of alterable biomechanical properties (information obtained from the amount of tissue altered by surgery and the remaining load-bearing tissue). Corneal topography patterns of placido disk seem to play a pivotal role as a surrogate of corneal strength, and abnormal corneal topography remains to be the most important identifiable risk factor for ectasia. Information derived from tomography, such as pachymetric and epithelial maps as well as computational strategies, to help in the detection of keratoconus is additional and relevant. High percentage of tissue altered (PTA) is the most robust risk factor for ectasia after LASIK in patients with normal preoperative corneal topography. Compared to specific residual stromal bed (RSB) or central corneal thickness values, percentage of tissue altered likely provides a more individualized measure of biomechanical alteration because it considers the relationship between thickness, tissue altered through ablation and flap creation, and ultimate RSB thickness. Other recognized risk factors include low RSB, thin cornea, and high myopia. Age is also a very important risk factor and still remains as one of the most overlooked ones. A comprehensive screening approach with the Ectasia Risk Score System, which evaluates multiple risk factors simultaneously, is also a helpful tool in the screening strategy.",
                "article_index_text": "pmid:27143849\ndoi:10.2147/OPTH.S51313\npmcid:4844427",
                "article_authors": "David Smadja,Marcony R Santhiago,Natalia T Giacomin,Samir J Bechara",
                "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
            },
            data.article.dict(),
        )

        self.assertIsInstance(data.articlepublished, schema.ArticlePublishedFtm)
        self.assertDictEqual(
            {
                "journal_id": "9d03ad6a37bbf4dec4878d21ab6f2f9afc3e66ae",
                "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                "article_published_at": "2016-04-20",
            },
            data.articlepublished.dict(),
        )

        identifiers = list(data.identifiers)
        self.assertEqual(len(identifiers), 3)
        for identifier in identifiers:
            self.assertIsInstance(identifier, schema.ArticleIdentifierFtm)
        self.assertSequenceEqual(
            [
                {
                    "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "articleidentifier_id": "40b29ed479b216666eb545ada1d6dfb684147841",
                    "articleidentifier_label": "PubMed ID",
                    "articleidentifier_value": "27143849",
                },
                {
                    "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "articleidentifier_id": "b5a8bfad1fc6879397c8c4d49f305bfe013e1b94",
                    "articleidentifier_label": "Digital Object Identifier",
                    "articleidentifier_value": "10.2147/OPTH.S51313",
                },
                {
                    "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "articleidentifier_id": "8be8e69834c8d792d914770ef100063ae40825af",
                    "articleidentifier_label": "PubMed Central ID",
                    "articleidentifier_value": "4844427",
                },
            ],
            [i.dict() for i in identifiers],
        )

        authors = list(data.authors)
        self.assertEqual(len(authors), 4)
        for author in authors:
            self.assertIsInstance(author, schema.AuthorFtm)
        self.assertSequenceEqual(
            [
                {
                    "author_id": "b197e07356885d0c2ecc6fd2011de74293404099",
                    "author_name": "David Smadja",
                    "author_first_name": "David",
                    "author_middle_names": None,
                    "author_last_name": "Smadja",
                    "author_countries": "il",
                },
                {
                    "author_id": "5ee2c50b04ddd72555db5410f6fe362036f03512",
                    "author_name": "Marcony R Santhiago",
                    "author_first_name": "Marcony R",
                    "author_middle_names": None,
                    "author_last_name": "Santhiago",
                    "author_countries": "br",
                },
                {
                    "author_id": "d847b3a2e8b32dc1ef76a24d07d6cc89cb82ead2",
                    "author_name": "Natalia T Giacomin",
                    "author_first_name": "Natalia T",
                    "author_middle_names": None,
                    "author_last_name": "Giacomin",
                    "author_countries": "br",
                },
                {
                    "author_id": "8cf2a4a342e41e586f4fec7e23b0bd672e5df73d",
                    "author_name": "Samir J Bechara",
                    "author_first_name": "Samir J",
                    "author_middle_names": None,
                    "author_last_name": "Bechara",
                    "author_countries": "br",
                },
            ],
            [a.dict() for a in authors],
        )

        authorships = list(data.authorships)
        self.assertEqual(len(authorships), 4)
        for authorship in authorships:
            self.assertIsInstance(authorship, schema.AuthorshipFtm)
            self.assertEqual(authorship.article_id, data.article.article_id)
            self.assertIn(authorship.author_id, [a.author_id for a in authors])
        self.assertSequenceEqual(
            [
                {
                    "author_id": "b197e07356885d0c2ecc6fd2011de74293404099",
                    "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "article_published_at": "2016-04-20",
                    "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                },
                {
                    "author_id": "5ee2c50b04ddd72555db5410f6fe362036f03512",
                    "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "article_published_at": "2016-04-20",
                    "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                },
                {
                    "author_id": "d847b3a2e8b32dc1ef76a24d07d6cc89cb82ead2",
                    "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "article_published_at": "2016-04-20",
                    "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                },
                {
                    "author_id": "8cf2a4a342e41e586f4fec7e23b0bd672e5df73d",
                    "article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "article_published_at": "2016-04-20",
                    "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                },
            ],
            [a.dict() for a in authorships],
        )

        organizations = list(data.organizations)
        self.assertEqual(len(organizations), 4)
        org_ids = set(o.institution_id for o in organizations)
        self.assertEqual(len(org_ids), 2)
        for organization in organizations:
            self.assertIsInstance(organization, schema.OrganizationFtm)
        self.assertSequenceEqual(
            [
                {
                    "institution_id": "62845aa73dbc15653d3bbde47678bb13831c7e31",
                    "institution_name": "Ophthalmology Department, Tel Aviv Sourasky Medical Center, Tel Aviv, Israel",
                    "institution_country": "il",
                },
                {
                    "institution_id": "4007e685cb687256b78361bec82a243a14bf037a",
                    "institution_name": "Department of Ophthalmology, Federal University of São Paulo, São Paulo, Brazil",
                    "institution_country": "br",
                },
                {
                    "institution_id": "4007e685cb687256b78361bec82a243a14bf037a",
                    "institution_name": "Department of Ophthalmology, Federal University of São Paulo, São Paulo, Brazil",
                    "institution_country": "br",
                },
                {
                    "institution_id": "4007e685cb687256b78361bec82a243a14bf037a",
                    "institution_name": "Department of Ophthalmology, Federal University of São Paulo, São Paulo, Brazil",
                    "institution_country": "br",
                },
            ],
            [o.dict() for o in organizations],
        )
        memberships = list(data.memberships)
        self.assertEqual(len(memberships), 4)
        for membership in memberships:
            self.assertIsInstance(membership, schema.MembershipFtm)
            self.assertIn(membership.author_id, [a.author_id for a in authors])
            self.assertIn(
                membership.institution_id, [o.institution_id for o in organizations]
            )
        self.assertSequenceEqual(
            [
                {
                    "author_id": "b197e07356885d0c2ecc6fd2011de74293404099",
                    "institution_id": "62845aa73dbc15653d3bbde47678bb13831c7e31",
                    "article_published_at": "2016-04-20",
                    "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                },
                {
                    "author_id": "5ee2c50b04ddd72555db5410f6fe362036f03512",
                    "institution_id": "4007e685cb687256b78361bec82a243a14bf037a",
                    "article_published_at": "2016-04-20",
                    "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                },
                {
                    "author_id": "d847b3a2e8b32dc1ef76a24d07d6cc89cb82ead2",
                    "institution_id": "4007e685cb687256b78361bec82a243a14bf037a",
                    "article_published_at": "2016-04-20",
                    "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                },
                {
                    "author_id": "8cf2a4a342e41e586f4fec7e23b0bd672e5df73d",
                    "institution_id": "4007e685cb687256b78361bec82a243a14bf037a",
                    "article_published_at": "2016-04-20",
                    "journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                },
            ],
            [m.dict() for m in memberships],
        )

        coi_statements = list(data.coi_statements)
        self.assertEqual(len(coi_statements), 3)
        for stmt in coi_statements:
            self.assertEqual(stmt.coi_article_id, data.article.article_id)
            self.assertEqual(stmt.coi_published_at, data.article.article_published_at)
            if stmt.coi_role == "conflict of interest statement (article)":
                self.assertSetEqual(
                    set(stmt.coi_authors.split(",")),
                    set((a.author_name for a in authors)),
                )
            else:
                self.assertIn(stmt.coi_author_id, [a.author_id for a in authors])
        self.assertSequenceEqual(
            [
                {
                    "coi_id": "7d2e27fbae1eb8e64860af1483f0cc31794b3848",
                    "coi_title": "conflict of interest statement (article)",
                    "coi_text": "Dr Santhiago is a consultant for Ziemer (Port, Switzerland) and Alcon (Fort Worth, TX, USA). Dr Smadja is a consultant for Ziemer (Port, Switzerland). The authors report no other conflicts of interest in this work.",
                    "coi_journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                    "coi_article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "coi_published_at": "2016-04-20",
                    "coi_author_id": None,
                    "coi_author_name": None,
                    "coi_authors": "David Smadja,Marcony R Santhiago,Natalia T Giacomin,Samir J Bechara",
                    "coi_flag": "True",
                    "coi_index_text": "flag:1",
                    "coi_role": "conflict of interest statement (article)",
                },
                {
                    "coi_id": "72e9487468b06df78555e2ddf6ef0fdd60f1f06e",
                    "coi_title": "individual conflict of interest statement (Marcony R Santhiago)",
                    "coi_text": "Dr Santhiago is a consultant for Ziemer (Port, Switzerland) and Alcon (Fort Worth, TX, USA). The authors report no other conflicts of interest in this work.",
                    "coi_journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                    "coi_article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "coi_published_at": "2016-04-20",
                    "coi_author_id": "5ee2c50b04ddd72555db5410f6fe362036f03512",
                    "coi_author_name": "Marcony R Santhiago",
                    "coi_authors": None,
                    "coi_flag": "True",
                    "coi_index_text": "flag:1",
                    "coi_role": "individual conflict of interest statement",
                },
                {
                    "coi_id": "366dce3f5a8fab389a9a62d1c71a39be5b79f530",
                    "coi_title": "individual conflict of interest statement (David Smadja)",
                    "coi_text": "Dr Smadja is a consultant for Ziemer (Port, Switzerland). The authors report no other conflicts of interest in this work.",
                    "coi_journal_name": "Clinical Ophthalmology (Auckland, N.Z.)",
                    "coi_article_id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "coi_published_at": "2016-04-20",
                    "coi_author_id": "b197e07356885d0c2ecc6fd2011de74293404099",
                    "coi_author_name": "David Smadja",
                    "coi_authors": None,
                    "coi_flag": "True",
                    "coi_index_text": "flag:1",
                    "coi_role": "individual conflict of interest statement",
                },
            ],
            [s.dict() for s in coi_statements],
        )

    def test_ftm_mapping(self):
        path = "./testdata/pubmed/PMC4844427/opth-10-713.nxml"  # an article with all required data
        data = load.pubmed(path)
        data = parse.parse_article(data)
        res = {"entities": [], "schemas": Counter()}
        for entity in ftm.make_entities(data):
            res["entities"].append(entity)
            res["schemas"][entity.schema.name] += 1

        self.assertDictEqual(
            {
                "Documentation": 10,
                "Person": 4,
                "Organization": 4,
                "Membership": 4,
                "Note": 3,
                "PlainText": 3,
                "LegalEntity": 1,
                "Article": 1,
            },
            res["schemas"],
        )

        self.assertSequenceEqual(
            [
                {
                    "id": "9d03ad6a37bbf4dec4878d21ab6f2f9afc3e66ae",
                    "schema": "LegalEntity",
                    "properties": {"name": ["Clinical Ophthalmology (Auckland, N.Z.)"]},
                },
                {
                    "id": "e9143144bc6b75cfa36503f3e749ce434150a3ca",
                    "schema": "Article",
                    "properties": {
                        "title": ["Ectasia risk factors in refractive surgery"],
                        "summary": [
                            "This review outlines risk factors of post-laser in situ keratomileusis (LASIK) ectasia that can be detected preoperatively and presents a new metric to be considered in the detection of ectasia risk. Relevant factors in refractive surgery screening include the analysis of intrinsic biomechanical properties (information obtained from corneal topography/tomography and patient’s age), as well as the analysis of alterable biomechanical properties (information obtained from the amount of tissue altered by surgery and the remaining load-bearing tissue). Corneal topography patterns of placido disk seem to play a pivotal role as a surrogate of corneal strength, and abnormal corneal topography remains to be the most important identifiable risk factor for ectasia. Information derived from tomography, such as pachymetric and epithelial maps as well as computational strategies, to help in the detection of keratoconus is additional and relevant. High percentage of tissue altered (PTA) is the most robust risk factor for ectasia after LASIK in patients with normal preoperative corneal topography. Compared to specific residual stromal bed (RSB) or central corneal thickness values, percentage of tissue altered likely provides a more individualized measure of biomechanical alteration because it considers the relationship between thickness, tissue altered through ablation and flap creation, and ultimate RSB thickness. Other recognized risk factors include low RSB, thin cornea, and high myopia. Age is also a very important risk factor and still remains as one of the most overlooked ones. A comprehensive screening approach with the Ectasia Risk Score System, which evaluates multiple risk factors simultaneously, is also a helpful tool in the screening strategy."
                        ],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                        "publishedAt": ["2016-04-20"],
                        "author": [
                            "Marcony R Santhiago",
                            "Samir J Bechara",
                            "Natalia T Giacomin",
                            "David Smadja",
                        ],
                        "indexText": [
                            "pmid:27143849\ndoi:10.2147/OPTH.S51313\npmcid:4844427"
                        ],
                    },
                },
                {
                    "id": "61b0e146a40cdb8685ca84ed8ab0e687d9f35d36",
                    "schema": "Documentation",
                    "properties": {
                        "entity": ["9d03ad6a37bbf4dec4878d21ab6f2f9afc3e66ae"],
                        "document": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "role": ["publisher"],
                        "date": ["2016-04-20"],
                    },
                },
                {
                    "id": "40b29ed479b216666eb545ada1d6dfb684147841",
                    "schema": "Note",
                    "properties": {
                        "name": ["PubMed ID"],
                        "description": ["27143849"],
                        "entity": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                    },
                },
                {
                    "id": "b5a8bfad1fc6879397c8c4d49f305bfe013e1b94",
                    "schema": "Note",
                    "properties": {
                        "name": ["Digital Object Identifier"],
                        "description": ["10.2147/OPTH.S51313"],
                        "entity": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                    },
                },
                {
                    "id": "8be8e69834c8d792d914770ef100063ae40825af",
                    "schema": "Note",
                    "properties": {
                        "name": ["PubMed Central ID"],
                        "description": ["4844427"],
                        "entity": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                    },
                },
                {
                    "id": "b197e07356885d0c2ecc6fd2011de74293404099",
                    "schema": "Person",
                    "properties": {
                        "country": ["il"],
                        "name": ["David Smadja"],
                        "firstName": ["David"],
                        "lastName": ["Smadja"],
                    },
                },
                {
                    "id": "5ee2c50b04ddd72555db5410f6fe362036f03512",
                    "schema": "Person",
                    "properties": {
                        "country": ["br"],
                        "name": ["Marcony R Santhiago"],
                        "firstName": ["Marcony R"],
                        "lastName": ["Santhiago"],
                    },
                },
                {
                    "id": "d847b3a2e8b32dc1ef76a24d07d6cc89cb82ead2",
                    "schema": "Person",
                    "properties": {
                        "country": ["br"],
                        "name": ["Natalia T Giacomin"],
                        "firstName": ["Natalia T"],
                        "lastName": ["Giacomin"],
                    },
                },
                {
                    "id": "8cf2a4a342e41e586f4fec7e23b0bd672e5df73d",
                    "schema": "Person",
                    "properties": {
                        "country": ["br"],
                        "name": ["Samir J Bechara"],
                        "firstName": ["Samir J"],
                        "lastName": ["Bechara"],
                    },
                },
                {
                    "id": "9b4772f7221d9e9e39d3d4e37960a8a4d5fb2b1b",
                    "schema": "Documentation",
                    "properties": {
                        "entity": ["b197e07356885d0c2ecc6fd2011de74293404099"],
                        "document": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "date": ["2016-04-20"],
                        "role": ["author"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "f27466b4a69117b7b7daaee422f08567db699619",
                    "schema": "Documentation",
                    "properties": {
                        "entity": ["5ee2c50b04ddd72555db5410f6fe362036f03512"],
                        "document": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "date": ["2016-04-20"],
                        "role": ["author"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "b660545e3d481e4decf533bd3037761a885406b0",
                    "schema": "Documentation",
                    "properties": {
                        "entity": ["d847b3a2e8b32dc1ef76a24d07d6cc89cb82ead2"],
                        "document": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "date": ["2016-04-20"],
                        "role": ["author"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "d3820fe190ade8ae712a492cddb6932797a67010",
                    "schema": "Documentation",
                    "properties": {
                        "entity": ["8cf2a4a342e41e586f4fec7e23b0bd672e5df73d"],
                        "document": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "date": ["2016-04-20"],
                        "role": ["author"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "62845aa73dbc15653d3bbde47678bb13831c7e31",
                    "schema": "Organization",
                    "properties": {
                        "country": ["il"],
                        "name": [
                            "Ophthalmology Department, Tel Aviv Sourasky Medical Center, Tel Aviv, Israel"
                        ],
                    },
                },
                {
                    "id": "4007e685cb687256b78361bec82a243a14bf037a",
                    "schema": "Organization",
                    "properties": {
                        "country": ["br"],
                        "name": [
                            "Department of Ophthalmology, Federal University of São Paulo, São Paulo, Brazil"
                        ],
                    },
                },
                {
                    "id": "4007e685cb687256b78361bec82a243a14bf037a",
                    "schema": "Organization",
                    "properties": {
                        "country": ["br"],
                        "name": [
                            "Department of Ophthalmology, Federal University of São Paulo, São Paulo, Brazil"
                        ],
                    },
                },
                {
                    "id": "4007e685cb687256b78361bec82a243a14bf037a",
                    "schema": "Organization",
                    "properties": {
                        "country": ["br"],
                        "name": [
                            "Department of Ophthalmology, Federal University of São Paulo, São Paulo, Brazil"
                        ],
                    },
                },
                {
                    "id": "972ffe1e9153859c0f7a1ee9a277b1f54c30b89d",
                    "schema": "Membership",
                    "properties": {
                        "member": ["b197e07356885d0c2ecc6fd2011de74293404099"],
                        "organization": ["62845aa73dbc15653d3bbde47678bb13831c7e31"],
                        "date": ["2016-04-20"],
                        "role": ["affiliated with"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "ab3d7b76fd6ebe8672d393d038f2df5421d89b6e",
                    "schema": "Membership",
                    "properties": {
                        "member": ["5ee2c50b04ddd72555db5410f6fe362036f03512"],
                        "organization": ["4007e685cb687256b78361bec82a243a14bf037a"],
                        "date": ["2016-04-20"],
                        "role": ["affiliated with"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "6ce0b76d19e1b47e12ce403cfc2feeb6d32fd449",
                    "schema": "Membership",
                    "properties": {
                        "member": ["d847b3a2e8b32dc1ef76a24d07d6cc89cb82ead2"],
                        "organization": ["4007e685cb687256b78361bec82a243a14bf037a"],
                        "date": ["2016-04-20"],
                        "role": ["affiliated with"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "e10ddcc856f8fabc64a95633ee4e83073740c859",
                    "schema": "Membership",
                    "properties": {
                        "member": ["8cf2a4a342e41e586f4fec7e23b0bd672e5df73d"],
                        "organization": ["4007e685cb687256b78361bec82a243a14bf037a"],
                        "date": ["2016-04-20"],
                        "role": ["affiliated with"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "7d2e27fbae1eb8e64860af1483f0cc31794b3848",
                    "schema": "PlainText",
                    "properties": {
                        "title": ["conflict of interest statement (article)"],
                        "bodyText": [
                            "Dr Santhiago is a consultant for Ziemer (Port, Switzerland) and Alcon (Fort Worth, TX, USA). Dr Smadja is a consultant for Ziemer (Port, Switzerland). The authors report no other conflicts of interest in this work."
                        ],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                        "date": ["2016-04-20"],
                        "parent": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "author": [
                            "Marcony R Santhiago",
                            "Samir J Bechara",
                            "Natalia T Giacomin",
                            "David Smadja",
                        ],
                        "notes": ["True"],
                        "indexText": ["flag:1"],
                    },
                },
                {
                    "id": "f473cd8bb26abd8c829a93efafd2c7800184951a",
                    "schema": "Documentation",
                    "properties": {
                        "document": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "entity": ["7d2e27fbae1eb8e64860af1483f0cc31794b3848"],
                        "role": ["conflict of interest statement (article)"],
                        "summary": [
                            "Dr Santhiago is a consultant for Ziemer (Port, Switzerland) and Alcon (Fort Worth, TX, USA). Dr Smadja is a consultant for Ziemer (Port, Switzerland). The authors report no other conflicts of interest in this work."
                        ],
                        "date": ["2016-04-20"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "72e9487468b06df78555e2ddf6ef0fdd60f1f06e",
                    "schema": "PlainText",
                    "properties": {
                        "title": [
                            "individual conflict of interest statement (Marcony R Santhiago)"
                        ],
                        "bodyText": [
                            "Dr Santhiago is a consultant for Ziemer (Port, Switzerland) and Alcon (Fort Worth, TX, USA). The authors report no other conflicts of interest in this work."
                        ],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                        "date": ["2016-04-20"],
                        "parent": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "author": ["Marcony R Santhiago"],
                        "notes": ["True"],
                        "indexText": ["flag:1"],
                    },
                },
                {
                    "id": "e28022c974a6b430715afae4821d23bc01f5d291",
                    "schema": "Documentation",
                    "properties": {
                        "document": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "entity": ["72e9487468b06df78555e2ddf6ef0fdd60f1f06e"],
                        "role": ["individual conflict of interest statement"],
                        "summary": [
                            "Dr Santhiago is a consultant for Ziemer (Port, Switzerland) and Alcon (Fort Worth, TX, USA). The authors report no other conflicts of interest in this work."
                        ],
                        "date": ["2016-04-20"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "7efd31437af05705d18c203a3816571643fe034d",
                    "schema": "Documentation",
                    "properties": {
                        "document": ["72e9487468b06df78555e2ddf6ef0fdd60f1f06e"],
                        "entity": ["5ee2c50b04ddd72555db5410f6fe362036f03512"],
                        "summary": [
                            "Dr Santhiago is a consultant for Ziemer (Port, Switzerland) and Alcon (Fort Worth, TX, USA). The authors report no other conflicts of interest in this work."
                        ],
                        "role": ["individual conflict of interest statement"],
                        "date": ["2016-04-20"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "366dce3f5a8fab389a9a62d1c71a39be5b79f530",
                    "schema": "PlainText",
                    "properties": {
                        "title": [
                            "individual conflict of interest statement (David Smadja)"
                        ],
                        "bodyText": [
                            "Dr Smadja is a consultant for Ziemer (Port, Switzerland). The authors report no other conflicts of interest in this work."
                        ],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                        "date": ["2016-04-20"],
                        "parent": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "author": ["David Smadja"],
                        "notes": ["True"],
                        "indexText": ["flag:1"],
                    },
                },
                {
                    "id": "622d70d6b22dedffa21d5066397bc76bc95345c9",
                    "schema": "Documentation",
                    "properties": {
                        "document": ["e9143144bc6b75cfa36503f3e749ce434150a3ca"],
                        "entity": ["366dce3f5a8fab389a9a62d1c71a39be5b79f530"],
                        "role": ["individual conflict of interest statement"],
                        "summary": [
                            "Dr Smadja is a consultant for Ziemer (Port, Switzerland). The authors report no other conflicts of interest in this work."
                        ],
                        "date": ["2016-04-20"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
                {
                    "id": "ecf05e77804c4c0c168f4ea97927d2fc7852f571",
                    "schema": "Documentation",
                    "properties": {
                        "document": ["366dce3f5a8fab389a9a62d1c71a39be5b79f530"],
                        "entity": ["b197e07356885d0c2ecc6fd2011de74293404099"],
                        "summary": [
                            "Dr Smadja is a consultant for Ziemer (Port, Switzerland). The authors report no other conflicts of interest in this work."
                        ],
                        "role": ["individual conflict of interest statement"],
                        "date": ["2016-04-20"],
                        "publisher": ["Clinical Ophthalmology (Auckland, N.Z.)"],
                    },
                },
            ],
            [e.to_dict() for e in res["entities"]],
        )

    def test_ftm_make_entities(self):
        def _test(path):
            data = load.pubmed(path)
            if data is not None:
                data = parse.parse_article(data)
                author_ids = [a.id for a in data.authors]
                institution_ids = [i.id for a in data.authors for i in a.institutions]
                for entity in ftm.make_entities(data):
                    if entity.schema.name == "LegalEntity":  # journal
                        self.assertEqual(entity.id, data.journal.id)
                        self.assertIn(data.journal.name, entity.properties["name"])
                    if entity.schema.name == "Article":
                        self.assertEqual(entity.id, data.id)
                        self.assertIn(data.title, entity.properties["title"])
                    if entity.schema.name == "Person":  # authors
                        self.assertIn(entity.id, author_ids)
                    if entity.schema.name == "Organization":  # institutions
                        self.assertIn(entity.id, institution_ids)
                    if entity.schema.name == "Membership":  # author affiliation
                        self.assertIn(entity.properties["member"][0], author_ids)
                        self.assertIn(
                            entity.properties["organization"][0], institution_ids
                        )

        for path in glob.glob("./testdata/pubmed/*/*xml"):
            _test(path)
        for path in glob.glob("./testdata/biorxiv/*xml"):
            _test(path)
