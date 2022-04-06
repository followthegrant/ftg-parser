"""
EUROPEPMC dataset

source data: https://europepmc.org/ftp/oa/

it is basically in jats xml format, but in huge xml (or gzipped) files
containing many articles. So we extract them and then use the jats logic

usage (either use gzip files or extracted ones):

    find ./data/ -type f -name "*.xml" | ftg parse europepmc
"""


import gc
import logging
from io import BytesIO
from typing import Iterator

from lxml import etree

from ...exceptions import LoaderException, ParserException
from ...util import load_or_extract
from .jats import parse as parse_jats

log = logging.getLogger(__name__)


def parse(fpath: str) -> Iterator[dict]:
    try:
        content = load_or_extract(fpath)
        articles = etree.iterparse(
            BytesIO(content.encode()), tag="article", recover=True, huge_tree=True
        )
        for _, el in articles:
            try:
                yield from parse_jats(el)
            except Exception as e:
                log.error(f"Cannot load via jats at `{fpath}`: `{e}`")
                raise LoaderException(e)
            # FIXME memory leaks?
            el.clear()
            del el
            gc.collect()
        del articles
        gc.collect()
    except Exception as e:
        log.error(f"Cannot parse XML at `{fpath}`: `{e}`")
        raise ParserException(e)
