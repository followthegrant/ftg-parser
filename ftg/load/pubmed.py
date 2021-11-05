import logging
from typing import Optional

import pubmed_parser as pp
from banal import ensure_list
from dateparser import parse

from ..coi import extract_coi_from_fulltext
from ..util import clean_dict

log = logging.getLogger(__name__)


# day first in pubmed parser
PUBMED_DATE_FORMATS = ("%-d-%-m-%y", "%-d-%-m-%y", "%d-%m-%Y", "%d-%m-%y")


def wrangle(data: dict, fpath: Optional[str] = None) -> dict:
    # keys re-mapping
    data["title"] = data.pop("full_title")
    published_at = parse(data.pop("publication_date"), date_formats=PUBMED_DATE_FORMATS)
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
        {k: data[k] for k in ("pmc", "pmcid", "pmid", "doi") if k in data}
    )

    if not data["coi_statement"]:
        if fpath is not None:
            # try to extract from fulltext
            data["coi_statement"] = extract_coi_from_fulltext(fpath)
    return clean_dict(data, expensive=True)


def pubmed(fpath):
    try:
        data = pp.parse_pubmed_xml(fpath)
    except Exception as e:
        log.error(f"Cannot load `{fpath}`: `{e}`")
        return

    return wrangle(data, fpath)
