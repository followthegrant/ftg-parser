import csv
import gzip
import tarfile
from collections import defaultdict
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Generator
from zipfile import ZipFile

import ijson
import orjson
from banal import ensure_dict, ensure_list

from ..logging import get_logger
from ..util import ensure_path, ensure_set

log = get_logger(__name__)


EXTRACT_SUFFIXES = (".gz", ".meca")


def load_or_extract(fp: str | Path) -> str:
    fp = ensure_path(fp)
    if fp is None:
        return ""
    if fp.suffix == ".gz":
        with gzip.open(fp) as f:
            content = f.read()
        return content.decode()
    if fp.suffix == ".meca":  # medRxiv
        with ZipFile(fp) as f:
            for file in f.infolist():
                if file.filename.endswith("xml"):
                    return f.open(file).read()
    else:
        with fp.open() as f:
            content = f.read()
        return content


def get_handler(fp: Path | str) -> TextIOWrapper:
    fp = Path(fp)
    if fp.suffix == ".gz":
        log.info("Opening gzip file", fpath=str(fp))
        return gzip.open(fp, "rt")
    if fp.suffix in (".meca", ".zip"):
        log.info("Opening zip file", fpath=str(fp))
        with ZipFile(fp) as f:
            for file in f.infolist():
                if file.filename.endswith("xml"):
                    return f.open(file)
    if ".tar." in fp.name:
        c = fp.suffix.lstrip(".")
        log.info("Opening tar file", fpath=str(fp))
        return tarfile.open(fp, f"r:{c}")
    return fp.open()


def iter_jsonl(
    fp: str | Path, log_chunk: int | None = 1_000
) -> Generator[dict, None, None]:
    fp = Path(fp)
    handler = get_handler(fp)
    ix = 0
    while True:
        line = handler.readline()
        if not line:
            break
        yield orjson.loads(line)
        ix += 1
        if ix and ix % log_chunk == 0:
            log.info("Parse json line %d ..." % ix, fpath=fp.name)
    handler.close()
    log.info("Parsed %d json lines." % (ix + 1), fpath=fp.name)


def iter_ijson(
    fp: str | Path, ijson_path: str, log_chunk: int | None = 1_000
) -> Generator[dict, None, None]:
    fp = Path(fp)
    handler = get_handler(fp)
    ix = 0
    items = ijson.items(handler, ijson_path)
    for item in items:
        yield item
        if ix and ix % log_chunk == 0:
            log.info("Parse ijson item %d ..." % ix, fpath=fp.name)
    handler.close()
    log.info("Parsed %d ijson items." % (ix + 1), fpath=fp.name)


def iter_csv(
    fp: str | Path, log_chunk: int | None = 10_000
) -> Generator[dict, None, None]:
    fp = Path(fp)
    handler = get_handler(fp)
    ix = 0
    reader = csv.DictReader(handler)
    for ix, row in enumerate(reader):
        yield row
        if ix and ix % log_chunk == 0:
            log.info("Parse csv row %d ..." % ix, fpath=fp.name)
    handler.close()
    log.info("Parsed %d csv rows." % (ix + 1), fpath=fp.name)


def get_path_values(obj: dict[Any, Any], path: str) -> Generator[Any, None, None]:
    """
    access a vaule in nested dictionary via dotted path of keys
    """
    item = obj
    paths = path.split(".")
    seen = False
    for i, p in enumerate(paths):
        if p.endswith("[]"):
            remaining = ".".join(paths[i + 1 :])
            for item in ensure_list(item.get(p[:-2])):
                if remaining:
                    yield from get_path_values(item, remaining)
                    return
                else:
                    yield item
            return
        else:
            item = ensure_dict(item).get(p)
            if not item:  # stop here
                yield item
                return
        seen = True

    if seen:
        yield item


def parse_dict(data: dict[Any, Any], mapping: dict[str, str]) -> defaultdict[set]:
    """
    mapping e.g.
        author.names[].value: name
    """
    result = defaultdict(set)
    for path, prop in mapping.items():
        for value in get_path_values(data, path):
            value = ensure_set(value)
            if value:
                result[prop].update(value)
    return result
