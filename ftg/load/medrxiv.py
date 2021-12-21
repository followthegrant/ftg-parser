"""
medRxiv dataset

source data: https://www.medrxiv.org/tdm

it is basically in pubmed xml format but zipped (though with a .meca extension)
so we extract the xml first and then use the pubmed logic

usage:

    find ./data/ -type f -name "*.meca" | ftg parse medrxiv

"""

import logging
from typing import Iterator

from ..util import load_or_extract
from .pubmed import load as load_pubmed

log = logging.getLogger(__name__)


def load(fpath: str) -> Iterator[dict]:
    try:
        content = load_or_extract(fpath)
        try:
            for data in load_pubmed(content):
                yield data
        except Exception as e:
            log.error(f"Cannot load via pubmed at `{fpath}`: `{e}`")
    except Exception as e:
        log.error(f"Cannot extract XML at `{fpath}`: `{e}`")
