"""
JATS format
https://jats.nlm.nih.gov

used by most open access datasets, e.g:
PUBMED CENTRAL dataset
EUROPEPMC (+ preprints)
BIORXIV
MEDRXIV

usage:

    find ./data/ -type f -name "*xml" | ftg parse jats

"""

from collections import defaultdict
from pathlib import Path
from typing import Generator

import yaml

from ...coi import extract_coi_from_fulltext
from ...exceptions import ParserException
from ...logging import get_logger
from ...transform import ParsedResult
from ..util import EXTRACT_SUFFIXES, ensure_set, load_or_extract
from ..xml import parse_xml, read_xml

log = get_logger(__name__)


with open(Path(__file__).parent.absolute() / "jats.yml") as f:
    XML_MAPPING = yaml.safe_load(f)


def fix_institution_names(institutions: list[defaultdict[set]]) -> None:
    # fill empty name from department / address data
    for institution in institutions:
        institution["name"] = ensure_set(institution["name"])
        if not institution["name"]:
            institution["name"].update(ensure_set(institution["weakAlias"]))
        if not institution["name"]:
            institution["name"].update(ensure_set(institution["address"]))


def parse(fpath: Path | str) -> Generator[ParsedResult, None, None]:
    if isinstance(fpath, Path) and fpath.suffix in EXTRACT_SUFFIXES:
        input_data = load_or_extract(fpath)
    else:
        input_data = fpath
    try:
        tree = read_xml(input_data)
        data = parse_xml(tree, XML_MAPPING)
        if data:
            if not data["article"]["coi_statement"]:
                if fpath is not None:
                    # try to extract from fulltext
                    data["article"]["coi_statement"] = extract_coi_from_fulltext(
                        input_data
                    )
            fix_institution_names(data["institutions"])
            yield ParsedResult(**data)
        else:
            raise ParserException("No data extracted")
    except Exception as e:
        log.error(f"Cannot parse jats: `{e}`", fpath=fpath.name)
        raise ParserException(e)
