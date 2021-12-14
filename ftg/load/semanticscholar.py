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

from ..model import ArticleIdentifier
from ..util import clean_dict, load_or_extract


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
    data["abstract"] = data.pop("paperAbstract")
    data["published_at"] = data.pop("year")
    data["journal"] = {"name": data.pop("journalName") or "SEMANTIC SCHOLAR"}
    data["mag"] = data.pop("magId")
    data["identifiers"] = clean_dict(
        {k: data[k] for k in ArticleIdentifier.identifiers_dict if k in data}
    )
    data["keywords"] = data.pop("fieldsOfStudy")
    data["source_url"] = data.pop("doiUrl") or data.pop("s2Url")
    data["authors"] = [a for a in _get_authors(data["authors"])]
    return clean_dict(data)


def _read(fpath: str) -> Iterator[dict]:
    f = load_or_extract(fpath)
    for line in f.split("\n"):
        line = line.strip()
        if line:
            yield json.loads(line)


def load(fpath: str) -> Iterator[dict]:
    for data in _read(fpath):
        yield wrangle(data)
