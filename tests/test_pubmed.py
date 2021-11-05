import glob
from unittest import TestCase
from collections import Counter

from ftg import parse, load


class ModelTestCase(TestCase):
    maxDiff = None

    def test_pubmed(self):
        article_ids = Counter()
        for path in glob.glob("./testdata/pubmed/*/*xml"):
            data = load.pubmed(path)
            if data is not None:
                data = parse.parse_article(data)
                article_ids[data.id] += 1

        # make sure all articles have unique ids
        self.assertSetEqual(set(article_ids.values()), {1})
