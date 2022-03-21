from typing import Iterator

from ftg.parse import parsers
from ftg.model import Article
from ftg.schema import ArticleFullOutput


def _parse(data: dict) -> ArticleFullOutput:
    """
    data input: output from any of the parsers in `ftg.parse.parsers`
    """
    article = Article(**data)
    data = article.serialize()
    if article.identifiers:
        data["identifiers"] = [i.serialize() for i in article.identifiers]
    if article.coi_statement:
        data["coi_statement"] = article.coi_statement.serialize()
        data["individual_coi_statements"] = [
            s.serialize() for s in article.individual_coi_statements
        ]
    if article.ack_statement:
        data["ack_statement"] = article.ack_statement.serialize()
        data["individual_ack_statements"] = [
            s.serialize() for s in article.individual_ack_statements
        ]

    return ArticleFullOutput(**data)


def jats(fpath: str) -> Iterator[ArticleFullOutput]:
    for article in parsers.jats(fpath):
        yield _parse(article)


def europepmc(fpath: str) -> Iterator[ArticleFullOutput]:
    for article in parsers.europepmc(fpath):
        yield _parse(article)


def medrxiv(fpath: str) -> Iterator[ArticleFullOutput]:
    for article in parsers.medrxiv(fpath):
        yield _parse(article)


def cord(fpath: str) -> Iterator[ArticleFullOutput]:
    for article in parsers.cord(fpath):
        yield _parse(article)


def crossref(fpath: str) -> Iterator[ArticleFullOutput]:
    for article in parsers.crossref(fpath):
        yield _parse(article)


def openaire(fpath: str) -> Iterator[ArticleFullOutput]:
    for article in parsers.openaire(fpath):
        yield _parse(article)


def semanticscholar(fpath: str) -> Iterator[ArticleFullOutput]:
    for article in parsers.semanticscholar(fpath):
        yield _parse(article)
