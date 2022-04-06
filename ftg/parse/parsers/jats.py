"""
JATS format
https://jats.nlm.nih.gov

used by most open access datasets, e.g:
PUBMED CENTRAL dataset
EUROPEPMC (+ preprints)
BIORXIV
MEDRXIV

based on forked `pubmed_parser`: https://github.com/simonwoerpel/pubmed_parser

usage:

    find ./data/ -type f -name "*xml" | ftg parse jats

"""
import logging
from typing import Iterator, Optional

import pubmed_parser as pp
from banal import ensure_list
from dateparser import parse as dateparse

from ...coi import extract_coi_from_fulltext
from ...exceptions import ParserException
from ...model import ArticleIdentifier
from ...util import clean_dict

log = logging.getLogger(__name__)


def wrangle(data: dict, fpath: Optional[str] = None) -> dict:
    if "id" in data:
        del data["id"]
    # keys re-mapping
    data["title"] = data.pop("full_title")
    published_at = dateparse(data.pop("publication_date"))
    if published_at is None:
        published_at = data.pop("publication_year")
    data["published_at"] = published_at
    data["journal"] = {
        "name": data["journal"],
        # "identifier": data.pop("publisher_id", None),
    }
    data["keywords"] = [
        k.strip()
        for k in data.pop("keywords").split(";") + data.pop("subjects").split(";")
        if k.strip()
    ]

    # reshaping authors and their institutions
    institutions = {k: v for k, v in ensure_list(data.pop("affiliation_list", None))}
    author_list = ensure_list(data.pop("author_list", None))
    memberships = {
        f"{first_name} {last_name}": []
        for last_name, first_name, _ in author_list  # noqa
    }
    for last_name, first_name, institute_key in author_list:
        if institutions.get(institute_key) is not None:  # FIXME improve pubmed parsing
            memberships[f"{first_name} {last_name}"].append(
                {"name": institutions[institute_key]}
            )
    data["authors"] = [
        {
            "name": f"{first_name} {last_name}",
            "first_name": first_name,
            "last_name": last_name,
            "institutions": ensure_list(memberships.get(f"{first_name} {last_name}")),
        }
        for last_name, first_name, _ in author_list
        if last_name and first_name
    ]

    # reshaping identifiers
    data["identifiers"] = clean_dict(
        {k: data[k] for k in ArticleIdentifier.identifiers_dict if k in data}
    )

    if not data["coi_statement"]:
        if fpath is not None:
            # try to extract from fulltext
            data["coi_statement"] = extract_coi_from_fulltext(fpath)
    return clean_dict(data, expensive=True)


def parse(fpath: str) -> Iterator[dict]:
    try:
        data = pp.parse_pubmed_xml(fpath)
    except Exception as e:
        log.error(f"Cannot parse jats at `{fpath}`: `{e}`")
        raise ParserException(e)

    yield wrangle(data, fpath)
