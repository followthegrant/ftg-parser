"""
S2ORC: The Semantic Scholar Open Research Corpus

source data: https://api.semanticscholar.org/corpus/download/
https://github.com/allenai/s2orc#download-instructions

expects as input file paths to either gzipped or already extracted json files,
which have one json record per line

usage:

    find ./data/ -type f -name "s2-corpus-*.gz" | ftg parse s2orc


"""


from collections import defaultdict
from typing import Generator

from normality import collapse_spaces

from ...logging import get_logger
from ...transform import ParsedResult
from ..util import iter_jsonl, parse_dict

log = get_logger(__name__)

ARTICLE = {
    "doi": "doi",
    "pmid": "pmid",
    "magId": "magid",
    "doiUrl": "sourceUrl",
    "id": "s2Id",
    "s2Url": "sourceUrl",
    "s2PdfUrl": "sourceUrl",
    "pdfUrls": "sourceUrl",
    "year": "publishedAt",
    "title": "title",
    "paperAbstract": "abstract",
    "fieldsOfStudy": "keywords",
}

JOURNAL = {"journalName": "name", "venue": "name", "sources": "publisher"}

AUTHOR = {"name": "name", "structuredName": "name", "ids": "s2Id"}


def _wrangle_author(author: dict[str, set[str]]) -> dict[str, set[str]]:
    names = set()
    for name in author["name"]:
        name = collapse_spaces(name)
        try:
            last_name, *middle_names, first_name = name.split()
            names.add(" ".join([first_name, *middle_names, last_name]))
        except ValueError:
            pass
    author["name"] = names
    return author


def wrangle(data: dict) -> ParsedResult:
    result = defaultdict(dict)
    is_venue = bool(data["venue"]) and not bool(data["journalName"])
    result["journal"] = parse_dict(data, JOURNAL)
    result["article"] = parse_dict(data, ARTICLE)
    if is_venue:
        result["journal"]["description"] = "venue"
    result["authors"] = []
    for author in data.pop("authors", []):
        result["authors"].append(_wrangle_author(parse_dict(author, AUTHOR)))
    return ParsedResult(**result)


def parse(fpath: str) -> Generator[ParsedResult, None, None]:
    for data in iter_jsonl(fpath):
        yield wrangle(data)
