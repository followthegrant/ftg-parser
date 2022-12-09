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

import yaml

from ...coi import extract_coi_from_fulltext
from ...exceptions import ParserException
from ...util import clean_dict
from ..xml import parse_xml, read_xml

log = logging.getLogger(__name__)


with open("./jats.yml") as f:
    XML_MAPPING = yaml.safe_load(f)


def wrangle(data: dict, fpath: Optional[str] = None) -> dict:
    if not data["coi_statement"]:
        if fpath is not None:
            # try to extract from fulltext
            data["coi_statement"] = extract_coi_from_fulltext(fpath)
    return clean_dict(data, expensive=True)


def parse(fpath: str) -> Iterator[dict]:
    try:
        tree = read_xml(fpath)
        data = parse_xml(tree, XML_MAPPING)
    except Exception as e:
        log.error(f"Cannot parse jats at `{fpath}`: `{e}`")
        raise ParserException(e)

    yield wrangle(data, fpath)
