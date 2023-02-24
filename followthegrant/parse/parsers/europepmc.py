"""
EUROPEPMC dataset

source data: https://europepmc.org/ftp/oa/

it is basically in jats xml format, but in huge xml (or gzipped) files
containing many articles. So we extract them and then use the jats logic

usage (either use gzip files or extracted ones):

    find ./data/ -type f -name "*.xml" | ftg parse europepmc
"""


import gc
from io import BytesIO
from pathlib import Path
from typing import Generator

from lxml import etree

from ...exceptions import LoaderException, ParserException
from ...logging import get_logger
from ...transform import ParsedResult
from ..util import load_or_extract
from .jats import parse as parse_jats

log = get_logger(__name__)


def parse(fpath: Path) -> Generator[ParsedResult, None, None]:
    ix = 0
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
            ix += 1
            if ix and ix % 100 == 0:
                log.info("Parsing article %d ..." % ix)
        del articles
        gc.collect()
    except Exception as e:
        log.error(f"Cannot parse XML at `{fpath}`: `{e}`")
        raise ParserException(e)
    if ix:
        log.info("Extracted %d articles." % ix, fpath=fpath.name)
