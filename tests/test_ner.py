from unittest import TestCase

from followthemoney import model

from ftg.ner import analyze


class NerTestCase(TestCase):
    def test_ner(self):
        entity = {
            "schema": "PlainText",
            "properties": {
                "date": ["2021-11-12"],
                "notes": ["1"],
                "bodyText": [
                    "M. Hacker received consulting fees and/or honoraria from Bayer Healthcare BMS, Eli Lilly, EZAG, GE Healthcare, Ipsen, ITM, Janssen, Roche, and Siemens Healthineers. (Center for Biomarker Research in Medicine, Graz, Austria). M. Hacker received consulting fees and/or honoraria from Bayer Healthcare BMS, Eli Lilly, EZAG, GE Healthcare, Ipsen, ITM, Janssen, Roche, and Siemens Healthineers. (Center for Biomarker Research in Medicine, Graz, Austria)."
                ],
                "author": ["Marcus Hacker"],
                "publisher": ["bioRxiv"],
                "title": ["individual conflict of interest statement (Marcus Hacker)"],
            },
            "id": "1a2257a9ed02f27c371f89967722080ec541f498",
        }
        entity = model.get_proxy(entity)
        entities = list(analyze(entity))
        self.assertEqual(entity.id, entities[-1].id)
        for entity in entities[:-1]:
            self.assertEqual(entity.schema.name, "Mention")
            # we only want to get organiuations (aka companies)
            self.assertEqual(entity.properties["detectedSchema"], ["Organization"])
        names = [e.caption for e in entities]
        self.assertIn("Bayer Healthcare BMS", names)
        self.assertIn("GE Healthcare", names)
        self.assertIn("Siemens Healthineers", names)
        self.assertIn("Center for Biomarker Research", names)
