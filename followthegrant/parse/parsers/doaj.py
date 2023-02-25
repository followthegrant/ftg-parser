"""
DOAJ



unpack to batch json files, then:

find ./doaj/ -type f -name "*.json" | ftg parse doaj

"""

from collections import defaultdict
from pathlib import Path
from typing import Any, Generator

from zavod.util import join_slug

from ...logging import get_logger
from ...model import ParsedResult
from ..util import iter_ijson, parse_dict

log = get_logger(__name__)

JOURNAL = {
    "bibjson.journal.issns": "issn",
    "bibjson.journal.publisher": "publisher",
    "bibjson.journal.country": "country",
    "bibjson.journal.title": "name",
}

ARTICLE = {
    "id": "ident",
    "bibjson.title": "title",
    "bibjson.abstract": "summary",
    "bibjson.subject[].term": "keywords",
    "bibjson.keywords[]": "keywords",
    "bibjson.link[].url": "sourceUrl",
}


def get_date(data: dict[str, Any]) -> str:
    return join_slug(data.get("year"), data.get("month"), data.get("day"))


def wrangle(data: dict[str, Any]) -> ParsedResult:
    result = defaultdict(list)
    result["journal"] = parse_dict(data, JOURNAL)
    result["article"] = parse_dict(data, ARTICLE)
    for ident in data["bibjson"]["identifier"]:
        if ident["type"].lower() == "doi":
            result["article"]["doi"].add(ident["id"])
    for author in data["bibjson"].get("author", []):
        if "affiliation" in author:
            author["xref_affiliation"] = author["affiliation"]
            result["institutions"].append({"name": author["affiliation"]})
        result["authors"].append(author)

    return ParsedResult(**result)


def parse(fpath: str | Path) -> Generator[ParsedResult, None, None]:
    fpath = Path(fpath)
    for item in iter_ijson(fpath, "item"):
        yield wrangle(item)
