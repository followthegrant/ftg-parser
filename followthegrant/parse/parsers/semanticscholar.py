"""
SEMANTICSCHOLAR dataset (full-corpus)

source data: https://api.semanticscholar.org/corpus/download/

expects as input file paths to either gzipped or already extracted json files,
which have one json record per line

usage:

    find ./data/ -type f -name "s2-corpus-*.gz" | ftg parse semanticscholar


"""


import json
from typing import Iterator

from ...util import clean_dict, load_or_extract
from ..util import get_mapped_data

MAPPING = {
    "doi": "article.doi",
    "magId": "article.magid",
    "doiUrl": "article.sourceUrl",
    "s2Url": "article.sourceUrl",
    "s2PdfUrl": "article.sourceUrl",
    "pdfUrls": "article.sourceUrl",
    "year": "article.publishedAt",
    "paperAbstract": "article.abstract",
    "fieldsOfStudy": "article.keywords",
    "journalName": "journal.name",
    "authors.name": "authors[].name",
    "authors.structuredName": "authors[].name",
    "authors.ids": "authors[].s2id",
}


def _get_authors(authors):
    for author in authors:
        if author["name"].strip():
            try:
                last_name, *middle_names, first_name = author["name"].split()
                author["name"] = " ".join([first_name, *middle_names, last_name])
                author["identifier_hints"] = author["ids"]
            except ValueError:
                pass
            yield author


def wrangle(data: dict) -> dict:
    data = get_mapped_data(MAPPING, data)
    import ipdb

    ipdb.set_trace()
    return clean_dict(data)


def _read(fpath: str) -> Iterator[dict]:
    f = load_or_extract(fpath)
    for line in f.split("\n"):
        line = line.strip()
        if line:
            yield json.loads(line)


def parse(fpath: str) -> Iterator[dict]:
    for data in _read(fpath):
        yield wrangle(data)
