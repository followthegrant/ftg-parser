"""
medRxiv dataset

source data: https://www.medrxiv.org/tdm

it is basically in jats xml format but zipped (though with a .meca extension)
so we extract the xml first and then use the jats logic

usage:

    find ./data/ -type f -name "*.meca" | ftg parse medrxiv

"""

import logging
from typing import Iterator

from ...util import load_or_extract
from .jats import parse as parse_jats

log = logging.getLogger(__name__)


def parse(fpath: str) -> Iterator[dict]:
    try:
        content = load_or_extract(fpath)
        try:
            yield from parse_jats(content)
        except Exception as e:
            log.error(f"Cannot parse jats at `{fpath}`: `{e}`")
    except Exception as e:
        log.error(f"Cannot extract XML at `{fpath}`: `{e}`")
