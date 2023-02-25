"""

ORCID Public Data File (Summaries)
https://info.orcid.org/documentation/integration-guide/working-with-bulk-data/

source data:
extract into json files with https://github.com/ORCID/orcid-conversion-lib

java -jar lib/orcid-conversion-lib-3.0.7-full.jar --tarball -i ORCID_2022_10_summaries.tar.gz -o orcid.json.tar.gz
tar -xf orcid.json.tar.gz

expects as input file paths to either gzipped or already extracted json files,
one per orcid author (as extracted from the summaries file)

usage:

    find ./data/ -type f -name "*.json" | ftg parse orcid

"""

from pathlib import Path
from typing import Any, Generator

import orjson
from zavod.util import join_slug

from ...ftm import get_firsts
from ...logging import get_logger
from ...transform import ParsedResult
from ..util import get_path_values, load_or_extract, parse_dict

log = get_logger(__name__)


AUTHOR = {
    # dict path -> prop mapping
    "orcid-identifier.uri": "sourceUrl",
    "orcid-identifier.path": "orcId",
    "person.name.given-names.value": "firstName",
    "person.name.family-name.value": "lastName",
    "person.name.credit-name.value": "name",
    "person.other-names.other-name.value": "alias",
    "person.biography.value": "description",
    "person.emails.email[].email": "email",
    "person.addresses.address[].country.value": "country",
}

FUNDINGS = "activities-summary.fundings.group[].funding-summary[]"
EMPLOYMENTS = (
    "activities-summary.employments.affiliation-group[].summaries[].employment-summary"
)
EDUCATIONS = (
    "activities-summary.educations.affiliation-group[].summaries[].education-summary"
)

INSTITUTION = {
    "organization.name": "name",
    "organization.address.country": "country",
    "organization.disambiguated-organization.disambiguated-organization-identifier": "ident",
}

AFFILIATION = {
    "start-date.year.value": "startYear",
    "start-date.month.value": "startMonth",
    "end-date.year.value": "endYear",
    "end-date.month.value": "endMonth",
    "role-title.value": "role",
}


def get_date(year: set[str | None] | None, month: set[str | None] | None) -> str | None:
    year, month = get_firsts(year, month)
    return join_slug(year, month, strict=False)


def get_affiliation(obj: dict[str, Any]) -> dict[str, set[str]]:
    data = parse_dict(obj, AFFILIATION)
    data["startDate"] = get_date(data["startYear"], data["startMonth"])
    data["endDate"] = get_date(data["endYear"], data["endMonth"])
    return data


def parse(fpath: str | Path) -> Generator[ParsedResult, None, None]:
    data = orjson.loads(load_or_extract(fpath))
    author = parse_dict(data, AUTHOR)
    institutions = []
    affiliations = []
    employments = []
    for education in get_path_values(data, EDUCATIONS):
        if education:
            institution = parse_dict(education, INSTITUTION)
            institution["xref"] = "edu"
            affiliation = get_affiliation(education)
            affiliation["xref"] = "edu"
            affiliation["role"].add("EDUCATION")
            affiliations.append(affiliation)
            institutions.append(institution)
    for employment in get_path_values(data, EMPLOYMENTS):
        if employment:
            institution = parse_dict(employment, INSTITUTION)
            institution["xref"] = "emp"
            affiliation = get_affiliation(employment)
            affiliation["xref"] = "emp"
            affiliation["role"].add("EMPLOYMENT")
            employments.append(affiliation)
            institutions.append(institution)

    author["xref_affiliation"] = "edu"
    author["xref_employment"] = "emp"

    result = {
        "authors": [author],
        "institutions": institutions,
        "affiliations": affiliations,
        "employments": employments,
    }
    yield ParsedResult(**result)
