import glob
from unittest import TestCase
from collections import defaultdict

from ftg import parse


class ParserTestCase(TestCase):
    maxDiff = None

    def _test(self, parser, path):
        articles = defaultdict(list)
        for path in glob.glob(path):
            data = parser(path)
            for article in data:
                articles[article.id].append(
                    (
                        article.title,
                        article.published_at,
                        article.journal,
                        article.identifiers,
                    )
                )

        # make sure all articles have unique ids
        # duplicates are possible during scraping, make sure they are kind of identical:
        for article in [a for a in articles.values() if len(a) > 1]:
            title, published_at, journal, identifiers = article[0]
            for title2, published_at2, journal2, identifiers2 in article[1:]:
                self.assertEqual(title, title2)
                self.assertEqual(published_at, published_at2)
                self.assertEqual(journal, journal2)
                self.assertEqual(identifiers, identifiers2)

    def test_biorxiv(self):
        self._test(parse.jats, "./testdata/biorxiv/*.xml")

    def test_medrxiv(self):
        self._test(parse.medrxiv, "./testdata/medrxiv/*/*.meca")

    def test_cord(self):
        self._test(parse.cord, "./testdata/cord/*.json")

    def test_pubmed(self):
        self._test(parse.jats, "./testdata/pubmed/*xml")

    def test_europepmc_xml(self):
        self._test(parse.europepmc, "./testdata/europepmc/*xml")

    def test_europepmc_gz(self):
        self._test(parse.europepmc, "./testdata/europepmc/*xml.gz")

    def test_semanticscholar(self):
        self._test(parse.semanticscholar, "./testdata/semanticscholar/*")

    def test_openaire(self):
        self._test(parse.openaire, "./testdata/openaire/*")

    def test_crossref(self):
        self._test(parse.crossref, "./testdata/crossref/*")
