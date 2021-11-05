import glob
from unittest import TestCase
from collections import defaultdict

from ftg import parse, load


class ModelTestCase(TestCase):
    maxDiff = None

    def test_biorxiv(self):
        articles = defaultdict(list)
        for path in glob.glob("./testdata/biorxiv/*xml"):
            data = load.biorxiv(path)
            if data is not None:
                article = parse.parse_article(data)
                articles[article.id].append(article)

        # make sure all articles have unique ids
        # duplicates are possible during scraping, make sure they are kind of identical:
        for article in [a for a in articles.values() if len(a) > 1]:
            a1 = article[0]
            for a in article[1:]:
                self.assertEqual(a1.title, a.title)
                self.assertEqual(a1.published_at, a.published_at)
                self.assertEqual(a1.journal, a.journal)
                self.assertEqual(a1.identifiers, a.identifiers)
