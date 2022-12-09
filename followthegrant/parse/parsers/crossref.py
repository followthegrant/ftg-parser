"""
CROSSREF dataset

source data:
https://academictorrents.com/details/e4287cb7619999709f6e9db5c359dda17e93d515

expects as input file paths to either gzipped or already extracted json files,
which have an array of objects in the key "items"

usage:

    find ./data/ -type f -name "*.json.gz" | ftg parse crossref

"""


import json
from typing import Iterator

from banal import ensure_list
from dateparser import parse as dateparse
from normality import slugify

from ...util import clean_dict, load_or_extract


TYPES = ("journal-article",)
DEFAULT_TITLE = "TITLE MISSING"


def _get_authors(authors):
    for author in authors:
        name = (
            author.get("name")
            or " ".join((author.get("given", ""), author.get("family", ""))).strip()
        )
        if slugify(name) is not None:
            data = {
                "name": name,
                "first_name": author.get("given"),
                "last_name": author.get("family"),
                "institutions": author["affiliation"],
            }

            if "ORCID" in author:
                data["identifier_hints"] = ["orcid", author["ORCID"]]
            yield data


def wrangle(original_data: dict) -> dict:
    data = {}
    data["journal"] = {"name": original_data["publisher"]}
    data["title"] = original_data["title"][0] or DEFAULT_TITLE
    data["published_at"] = dateparse(original_data["created"]["date-time"]).date()
    data["identifiers"] = {"doi": original_data["DOI"]}
    data["authors"] = [
        a for a in _get_authors(ensure_list(original_data.get("author")))
    ]
    return clean_dict(data)


def _read(fpath: str) -> Iterator[dict]:
    content = load_or_extract(fpath)
    data = json.loads(content)
    for item in ensure_list(data.get("items")):
        if item.get("type") in TYPES:
            yield item


def parse(fpath: str) -> Iterator[dict]:
    for data in _read(fpath):
        yield wrangle(data)
