from functools import lru_cache
from pathlib import Path
from typing import Any, Hashable, Iterable

import dateparser
from banal import ensure_list
from fingerprints import generate
from normality import collapse_spaces

from . import settings


@lru_cache(1_000_000)
def fp(value: str | None) -> str | None:
    return generate(value)


def clean_value(value: str | None) -> str | None:
    if not value:
        return None
    value = collapse_spaces(value)
    if value:
        return value.strip(".,")
    return None


def clean_list(*data: Iterable[Any]) -> list:
    """
    flatten and filter out falsish values from iterables but keep order
    """
    res = []
    for items in data:
        for item in ensure_list(items):
            if item:
                res.append(item)
    return res


def get_path(fp: Path | str | None = None) -> Path:
    """fix path related to `DATA_ROOT`, used for docker volumes"""
    if not fp:
        return settings.DATA_ROOT
    fp = Path(fp)
    return settings.DATA_ROOT / fp


@lru_cache(1_000_000)
def clean_date(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if len(value) == 4:  # year
        return value
    parsed_value = dateparser.parse(value)
    if parsed_value:
        return parsed_value.date().isoformat()
    return value


def clean_ids(value: Iterable | str | None = None) -> set[str]:
    values = set()
    for value in ensure_list(value):
        value = value or ""
        if value.startswith("http"):
            values.add(value.split("/")[-1])
    return set(clean_list(values))


def ensure_set(values: Iterable[Hashable]) -> set[Hashable]:
    return set(clean_list(values))


def ensure_path(fp: str | Path | None = None) -> None | Path:
    if fp is None:
        return None
    return Path(fp)
