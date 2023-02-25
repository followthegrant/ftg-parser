"""
CORE

https://core.ac.uk/documentation/dataset

unpack the big resync tar.gz to the smaller *.tar.xz, then

find ./core/ -type f -name "*.tar.xz" | ftg parse core

"""

from collections import defaultdict
from pathlib import Path
from typing import Any, Generator

import orjson

from ...logging import get_logger
from ...model import ParsedResult
from ..util import get_handler, iter_csv, parse_dict

log = get_logger(__name__)

JOURNAL = {
    "journals[].title": "name",
    "journals[].identifiers[]": "ident",
    "issn": "issn",
}

ARTICLE = {
    "doi": "doi",
    "coreId": "coreId",
    "oai": "oai",
    "magId": "magId",
    "title": "title",
    "datePublished": "publishedAt",
    "abstract": "summary",
    "downloadUrl": "sourceUrl",
    "publisher": "publisher",
    "topics[]": "keywords",
    "urls[]": "sourceUrl",
}


def wrangle(data: dict[str, Any]) -> ParsedResult:
    result = defaultdict(list)
    result["journal"] = parse_dict(data, JOURNAL)
    for ident in result["journal"]["ident"]:
        if ident.startswith("issn:"):
            result["journal"]["issn"].add(ident[5:])
    result["article"] = parse_dict(data, ARTICLE)
    for author in data["authors"]:
        result["authors"].append({"name": author})

    return ParsedResult(**result)


def parse(fpath: str | Path) -> Generator[ParsedResult, None, None]:
    fpath = Path(fpath)
    if fpath.suffix == ".xz":
        handler = get_handler(fpath)
        member = handler.next()
        ix = 0
        while member is not None:
            if member.name.endswith(".json"):
                content = handler.extractfile(member)
                data = orjson.loads(content.read())
                yield wrangle(data)
                ix += 1
                if ix and ix % 1000 == 0:
                    log.info("Parse json file %d ..." % ix)
            member = handler.next()
        if ix:
            log.info("Parsed %d json files." % (ix + 1), fp=fpath.name)
        handler.close()
    elif fpath.suffix == ".gz":  # mag id mapping
        for row in iter_csv(fpath):
            data = {"coreId": row["coreid"], "magId": row["magid"]}
            yield ParsedResult(article=data)
