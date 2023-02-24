"""
CORD

https://github.com/allenai/cord19

use the metadata.csv that then references the extracted json:

ftg parse cord -f ./.../metadata.csv

"""

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Generator

import orjson
from followthemoney.util import join_text

from ...logging import get_logger
from ...model import ParsedResult, get_firsts
from ..util import load_or_extract, parse_dict

log = get_logger(__name__)

JOURNAL = {"journal": "name"}

ARTICLE = {
    "paper_id": "ident",
    "source_x": "publisher",
    "doi": "doi",
    "pmcid": "pmc",
    "pubmed_id": "pmid",
    "publish_time": "publishedAt",
    "mag_id": "magId",
    "arxiv_id": "arxivId",
    "url": "sourceUrl",
    "s2_id": "s2Id",
    "abstract[].text": "summary",
    "metadata.title": "title",
}

AUTHOR = {
    "first": "firstName",
    "middle": "middleName",
    "last": "lastName",
    "email": "email",
}

INSTITUTION = {
    "affiliation.institution": "name",
    "affiliation.location.country": "country",
    "affiliation.location.addrLine": "address",
}

ADDRESS = {
    "affiliation.location.settlement": "city",
    "affiliation.location.postBox": "postBox",
    "affiliation.location.postCode": "postCode",
    "affiliation.location.region": "region",
}


def make_address(data: dict[str, Any]) -> str:
    data = parse_dict(data, ADDRESS)
    return join_text(
        *get_firsts(data["postBox"], data["postCode"], data["city"], data["region"])
    )


def _load_coi_statement(data: dict) -> str:
    pass


def wrangle(data: dict[str, Any]) -> ParsedResult:
    result = defaultdict(list)
    result["article"] = parse_dict(data, ARTICLE)
    result["journal"] = parse_dict(data, JOURNAL)
    for author in data["metadata"].get("authors", []):
        affiliation = parse_dict(author, INSTITUTION)
        affiliation["xref"] = affiliation["name"]
        affiliation["address"].add(make_address(author))
        author = parse_dict(author, AUTHOR)
        author["xref_affiliation"] = affiliation["xref"]
        result["authors"].append(author)
        result["institutions"].append(affiliation)
    for section in data.get("back_matter", []):
        section_name = section["section"].lower()
        if "acknowl" in section_name:
            result["article"]["ack_statement"].add(section["text"])
        if "competing" in section_name or "conflict" in section_name:
            result["article"]["coi_statement"].add(section["text"])
        if "funding" in section_name:
            result["article"]["funding_statement"].add(section["text"])
    return ParsedResult(**result)


def wrangle_meta(data: dict[str, Any], fpath: Path) -> ParsedResult:
    journal = parse_dict(data, JOURNAL)
    article = parse_dict(data, ARTICLE)
    authors = []
    institutions = []
    for fp in data["pdf_json_files"].split(";"):
        fp = fp.strip()
        if fp:
            fp = fpath.parent / fp
            if fp.exists():
                for result in parse(fp):
                    for k, v in result.article.items():
                        article[k].update(v)
                    for k, v in result.journal.items():
                        journal[k].update(v)
                    authors.extend(result.authors)
                    institutions.extend(result.institutions)
            else:
                log.warning("json file does not exist", fpath=str(fp))
    return ParsedResult(
        journal=journal, article=article, authors=authors, institutions=institutions
    )


def parse(fpath: str | Path) -> Generator[ParsedResult, None, None]:
    fpath = Path(fpath)
    if fpath.suffix == ".json":
        # usually don't use json files directly as there is no useful metadata,
        # prefer metadata.csv which parses the referenced json files
        data = load_or_extract(fpath)
        data = orjson.loads(data)
        yield wrangle(data)
    elif fpath.suffix == ".csv":
        # metadata.csv
        reader = csv.DictReader(fpath.open())
        ix = 0
        for ix, row in enumerate(reader):
            yield wrangle_meta(row, fpath)
            if ix and ix % 10_000 == 0:
                log.info("Parsing csv row %d ..." % ix)
        if ix:
            log.info("Parsed %d csv rows %d" % (ix + 1), fp=fpath.name)
