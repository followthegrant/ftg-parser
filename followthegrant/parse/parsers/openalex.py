"""
OpenAlex dataset
https://docs.openalex.org/download-snapshot

source data:

expects as input file paths to either gzipped or already extracted json files,
which have one json record per line

usage:

    find ./data/ -type f -name "*.json.gz" | ftg parse openalex

"""

from collections import defaultdict
from pathlib import Path
from typing import Generator

from zavod.parse.addresses import format_line

from ...ftm import get_first
from ...logging import get_logger
from ...transform import ParsedResult
from ..util import iter_jsonl, parse_dict

log = get_logger(__name__)


IDS = {
    "id": "openalexId",
    "doi": "doi",
    "ids.doi": "doi",
    "openalex": "openalexId",
    "ids.openalex": "openalexId",
    "orcid": "orcId",
    "ids.orcid": "orcId",
    "ror": "rorId",
    "ids.ror": "rorId",
    "grid": "gridId",
    "ids.grid": "gridId",
    "wikipedia": "wikipediaUrl",
    "ids.wikipedia": "wikipediaUrl",
    "wikidata": "wikidataId",
    "ids.wikidata": "wikidataId",
    "mag": "magId",
    "ids.mag": "magId",
    "issn_l": "issn",
    "ids.issn_l": "issn",
    "issn": "issn",
    "ids.issn": "issn",
}

GEO = {
    "city": "city",
    "region": "state",
    "country_code": "country_code",
}

# https://docs.openalex.org/about-the-data/author
AUTHOR = {
    "display_name": "name",
    "display_name_alternatives": "name",
}
AUTHOR.update(IDS)

# https://docs.openalex.org/about-the-data/institution
INSTITUTION = {
    "display_name": "name",
    "display_name_alternatives": "name",
    "display_name_acronyms": "alias",
    "country_code": "country",
    "type": "legalForm",
    "homepage_url": "website",
}
INSTITUTION.update(IDS)

# https://docs.openalex.org/about-the-data/venue
VENUE = {
    "publisher": "publisher",
    "display_name": "name",
    "alternate_titles": "alias",
    "homepage_url": "website",
    "type": "description",
}
VENUE.update(IDS)

# https://docs.openalex.org/about-the-data/work
WORK = {
    "title": "title",
    "publication_date": "publishedAt",
    "publication_year": "publishedAt",
    "type": "description",
}
WORK.update(IDS)


def wrangle(data: dict, schema: str) -> ParsedResult:
    result = defaultdict(list)
    if schema == "authors":
        author = parse_dict(data, AUTHOR)
        if data["last_known_institution"]:
            institution = parse_dict(data["last_known_institution"], INSTITUTION)
            author["xref_affiliation"] = institution["xref"] = institution["openalexId"]
            result["institutions"].append(institution)
        result["authors"].append(author)
        return ParsedResult(**result)
    if schema == "institutions":
        institution = parse_dict(data, INSTITUTION)
        address = {k: get_first(v) for k, v in parse_dict(data["geo"], GEO).items()}
        institution["address"].add(format_line(**address))
        result["institutions"].append(institution)
        return ParsedResult(**result)
    if schema == "venues":
        journal = parse_dict(data, VENUE)
        journal["description"].add("Venue")
        return ParsedResult(journal=journal)
    if schema == "works":
        result["article"] = parse_dict(data, WORK)
        result["journal"] = parse_dict(data["host_venue"], VENUE)
        result["article"]["sourceUrl"].add(data["host_venue"]["url"])
        result["article"]["sourceUrl"].update(
            [x["url"] for x in data["alternate_host_venues"]]
        )
        institutions = []
        for authorship in data["authorships"]:
            author = parse_dict(authorship["author"], AUTHOR)
            for institution in authorship["institutions"]:
                institution = parse_dict(institution, INSTITUTION)
                institution["xref"].update(institution["openalexId"])
                institution["xref"].add(authorship["raw_affiliation_string"])
                institutions.append(institution)
                author["xref_affiliation"].update(institution["xref"])
            result["authors"].append(author)
        result["institutions"] = institutions
        return ParsedResult(**result)


def parse(fpath: str | Path) -> Generator[ParsedResult, None, None]:
    fpath = Path(fpath)
    schema = None
    for s in ("authors", "institutions", "venues", "works"):
        if s in fpath.parts:
            schema = s
            break
    if schema is None:
        log.error(f"Not a valid OpenAlex scheme: `{schema}`", fpath=str(fpath))
    for data in iter_jsonl(fpath, 10_000):
        yield wrangle(data, schema=schema)
