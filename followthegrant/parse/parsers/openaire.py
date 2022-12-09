"""
OPENAIRE dataset

source data:

expects as input file paths to either gzipped or already extracted json files,
which have one json record per line

usage:

    find ./data/ -type f -name "*.json.gz" | ftg parse openaire

"""


import json
from typing import Iterator

from dateparser import parse as dateparse
from normality import slugify

from ...util import clean_dict, load_or_extract

DEFAULT_JOURNAL = "OPENAIRE (missing journal name)"
DEFAULT_TITLE = "TITLE MISSING"


def _get_authors(authors):
    for author in authors:
        data = {
            "name": author["fullname"],
            "first_name": author.get("name"),
            "last_name": author.get("surname"),
        }
        if not data["name"]:
            if data["first_name"] is not None and data["last_name"] is not None:
                data["name"] = " ".join((data["first_name"], data["last_name"]))
        if slugify(data["name"]) is not None:
            if "pid" in author:
                if "id" in author["pid"]:
                    data["identifier_hints"] = [
                        author["pid"]["id"]["scheme"],
                        author["pid"]["id"]["value"],
                    ]
            yield data


def wrangle(data: dict) -> dict:
    data["title"] = data.pop("maintitle", None)
    data["abstract"] = (data.pop("description")[:1] or [None]).pop()
    if not slugify(data["title"]):
        if data["abstract"] is not None:
            data["title"] = data["abstract"][:300]
        else:
            data["title"] = DEFAULT_TITLE
    data["published_at"] = data.pop("publicationdate", None)
    if data["published_at"] is not None:
        published_at = dateparse(data["published_at"])
        if published_at is not None:
            published_at = published_at.date()
        data["published_at"] = published_at
    data["journal"] = {"name": data.pop("publisher", None) or DEFAULT_JOURNAL}
    if not slugify(data["journal"]["name"]):
        data["journal"]["name"] = DEFAULT_JOURNAL
    data["openaireid"] = data["id"]
    data["keywords"] = [s["subject"]["value"] for s in data.pop("subjects", [])]
    data["authors"] = [a for a in _get_authors(data.pop("author", []))]
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
