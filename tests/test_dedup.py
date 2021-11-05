from ftg import model


from unittest import TestCase


class DedupTestCase(TestCase):
    def test_journal_dedup(self):
        # only name: dedupe via fingerprint
        journal1 = model.Journal(**{"name": "Journal 1"})
        journal2 = model.Journal(**{"name": "Journal 2"})
        self.assertNotEqual(journal1.id, journal2.id)
        journal3 = model.Journal(**{"name": "  2 -  journal"})
        self.assertEqual(journal2.id, journal3.id)

        # dedupe via identifier regardless of name
        journal1 = model.Journal(**{"name": "Journal 1", "identifier": "j1"})
        journal2 = model.Journal(**{"name": "Journal 2", "identifier": "j1"})
        self.assertEqual(journal1.id, journal2.id)

        journal1 = model.Journal(**{"name": "Journal 1", "identifier": "j1"})
        journal2 = model.Journal(**{"name": "Journal 1", "identifier": "j2"})
        self.assertNotEqual(journal1.id, journal2.id)

    def test_institutions_dedup(self):
        # only name: dedupe via fingerprint
        institution1 = model.Institution(**{"name": "Institution 1"})
        institution2 = model.Institution(**{"name": "Institution 2"})
        self.assertNotEqual(institution1.id, institution2.id)
        institution1 = model.Institution(**{"name": "Institution North"})
        institution2 = model.Institution(**{"name": "Institution South"})
        self.assertNotEqual(institution1.id, institution2.id)
        institution3 = model.Institution(**{"name": "  south institution"})
        self.assertEqual(institution2.id, institution3.id)

        # dedupe via identifier regardless of name
        institution1 = model.Institution(**{"name": "Dep. North", "identifier": "1"})
        institution2 = model.Institution(**{"name": "Dep. South", "identifier": "1"})
        self.assertEqual(institution1.id, institution2.id)

        # some cleaning magic:

        # weird stuff in pubmed data, FIXME
        institution1 = model.Institution(**{"name": "Institution 2"})
        institution2 = model.Institution(**{"name": "Xgrid.1000 Institution  2"})
        self.assertEqual(institution1.id, institution2.id)
        institution3 = model.Institution(**{"name": ".1000  .0010. Institution  2"})
        self.assertEqual(institution1.id, institution3.id)

        # real world name cleaning example
        institution = model.Institution(
            **{
                "name": "grid.415869.7  Department of Cardiology,   Renji Hospital, School of Medicine Shanghai Jiaotong University,   Shanghai, 200127 China"
            }
        )
        self.assertEqual(
            institution.output.name,
            "Department of Cardiology, Renji Hospital, School of Medicine Shanghai Jiaotong University, Shanghai, 200127 China",
        )

        # stopwords
        institution1 = model.Institution(**{"name": "department of medicine"})
        institution2 = model.Institution(**{"name": "MEdicine"})
        self.assertEqual(institution1.id, institution2.id)
        institution1 = model.Institution(
            **{"name": "Department of medicine and division for the animals"}
        )
        institution2 = model.Institution(**{"name": "MEdicine /   Animals"})
        self.assertEqual(institution1.id, institution2.id)

    def test_authors_dedup(self):
        """
        author deduplication: fingerprinted name and first institution (sorted by id),
        or if no institution using journal id as identifier hint
        """
        journal = model.Journal(**{"name": "Journal 1"})
        authors = (
            {"name": "Alice Smith"},
            {"name": "Smith, Alice"},
            {"name": "Dr. Alice Smith", "institutions": [{"name": "Medicine"}]},
            {
                "name": "Dr. Smith, Alice",
                "institutions": [{"name": "Department of Medicine"}],
            },
            {"name": "Alice Smith", "identifier_hints": [journal.id]},
        )
        a1, a2, a3, a4, a5 = [model.Author(**a) for a in authors]

        self.assertEqual(a1.id, a2.id)
        self.assertNotEqual(a1.id, a3.id)
        self.assertEqual(a3.id, a4.id)
        self.assertNotEqual(a1.id, a5.id)
