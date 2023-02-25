from .ftm import Values
from .util import fp

# ordered from most canonical to less
IDENTIFIERS = (
    "doi",
    "oai",
    "orcId",
    "issn",
    "isni",
    "rorId",
    "gridId",
    "pmc",
    "pmid",
    "magId",
    "arxivId",
    "openalexId",
    "openaireId",
    "s2Id",
    "scopusId",
    "projectId",
)


def pick_best(obj: dict[str, set[str]]) -> tuple[str, str] | tuple[None, None]:
    for ident in IDENTIFIERS:
        if ident in obj:
            for ix in obj[ident]:  # sorted as we have a set
                if fp(ix):  # make sure we have a value
                    return ident, ix
    return None, None


def clean_ident(values: Values | None, identifier: str) -> str | None:
    if values is None:
        return set()
    if identifier == "doi":
        return set(
            v.replace("https://doi.org/", "").replace("http://doi.org/", "")
            for v in values
        )
    if identifier == "orcId":
        return set(v.split("/")[-1] for v in values)
    if identifier == "pmc":
        return set(v.replace("PMC", "") for v in values)
    if identifier == "isni":
        return set(v.replace(" ", "") for v in values)
    if identifier == "gridId":
        return set(v.replace("grid.", "") for v in values)
    return values
