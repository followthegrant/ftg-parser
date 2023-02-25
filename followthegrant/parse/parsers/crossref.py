"""
CROSSREF dataset

source data:
https://academictorrents.com/details/4dcfdf804775f2d92b7a030305fa0350ebef6f3e

expects as input file paths to either gzipped or already extracted json files,
which have an array of objects in the key "items"

usage:

    find ./data/ -type f -name "*.json.gz" | ftg parse crossref

"""


from collections import defaultdict
from typing import Any, Generator

import orjson
from banal import ensure_list
from html2text import html2text

from ...transform import ParsedResult
from ...util import clean_list
from ..util import load_or_extract, parse_dict

IDS = {"ROR": "rorId", "ORCID": "orcId", "ISSN": "issn", "DOI": "doi"}

JOURNAL = {
    "issn-type[].value": "issn",
    "ISSN": "issn",
    "container-title": "name",
    "short-container-title": "name",
    "short-title": "name",
    "publisher": "publisher",
    "source": "publisher",
}

JOURNAL_TYPE = {
    "DOI": "doi",
    "URL": "sourceUrl",
    "link[].URL": "sourceUrl",
    "resource.primary.URL": "website",
    "subject[]": "keywords",
}
JOURNAL_TYPE.update(JOURNAL)

ARTICLE = {
    "DOI": "doi",
    "created.date-parts.date-time": "publishedAt",
    "published.date-parts.date-time": "publishedAt",
    "title": "title",
    "original-title": "title",
    "URL": "sourceUrl",
    "resource.primary.URL": "sourceUrl",
    "subject[]": "keywords",
    "publisher": "publisher",
    "source": "publisher",
    "abstract": "summary",
}

AUTHOR = {"ORCID": "orcId", "given": "firstName", "family": "lastName"}

INSTITUTION = {"ROR": "rorId", "name": "name", "award[]": "grants"}

# GRANT_INLINE = {"funder[].award[]": "projectId"}

GRANT = {
    "award": "projectId",
    "URL": "sourceUrl",
    "resource.primary.URL": "sourceUrl",
    "project[].project-title[].title": "title",
    "project[].project-description[].description": "summary",
    "project[].award-amount.amount": "amount",
    "project[].award-amount.currency": "currency",
}


def parse_date(data: dict[str, list[list[int]]]) -> str:
    dates = set()
    parts = data.get("date-parts", [])
    for part in parts:
        dates.add("%d-%d-%d" % tuple(part))
    return dates


def parse_ids(data: list[dict[str, str]]) -> Generator[tuple[str, str], None, None]:
    for item in data:
        yield IDS.get(item["id-type"], item["id-type"]), item["id"]


def make_institution(data: dict[str, str]) -> dict[str, set]:
    institution = parse_dict(data, INSTITUTION)
    for key, value in parse_ids(data.get("id", [])):
        institution[key].add(value)
    return institution


def wrangle(data: dict[str, Any]) -> ParsedResult | None:
    result = defaultdict(list)

    if data["type"] == "journal":
        result["journal"] = parse_dict(data, JOURNAL_TYPE)
        return ParsedResult(**result)

    if data["type"] == "journal-article" or data["type"] == "component":
        result["journal"] = parse_dict(data, JOURNAL)
        result["article"] = parse_dict(data, ARTICLE)
        result["article"]["summary"] = [
            html2text(i) for i in clean_list(result["article"]["summary"])
        ]
        for author_data in data.get("author", []):
            author = parse_dict(author_data, AUTHOR)
            for affiliation in author_data.get("affiliation", []):
                institution = make_institution(affiliation)
                institution["xref"] = institution["name"]
                author["xref_affiliation"] = institution["xref"]
                result["institutions"].append(institution)
            result["authors"].append(author)

        for funder in data.get("funder", []):
            funder = parse_dict(funder, INSTITUTION)
            funder["xref"] = funder["name"]
            result["article"]["xref_funding"].update(funder["xref"])
            result["institutions"].append(funder)

        return ParsedResult(**result)

    if data["type"] == "grant":
        grant = parse_dict(data, GRANT)
        grant["xref"] = "grant"
        investigators = []
        for project in data["project"]:
            grant["startDate"] = parse_date(project.get("award-start", {}))
            grant["endData"] = parse_date(project.get("award-end", {}))
            investigators.extend(project.get("investigator", []))
            investigators.extend(project.get("lead-investigator", []))
        for author_data in investigators:
            author = parse_dict(author_data, AUTHOR)
            author["xref_grant_investigator"] = grant["xref"]
            for affiliation in author_data.get("affiliation", []):
                institution = make_institution(affiliation)
                institution["xref"] = institution["name"]
                author["xref_affiliation"] = institution["xref"]
                result["institutions"].append(institution)
            result["authors"].append(author)
        result["grants"].append(grant)

        return ParsedResult(**result)


def parse(fpath: str) -> Generator[ParsedResult, None, None]:
    content = load_or_extract(fpath)
    data = orjson.loads(content)
    for item in ensure_list(data.get("items")):
        result = wrangle(item)
        if result:
            yield result
