import json
from typing import Iterable, Iterator

from dateparser import parse as dateparse
from normality import slugify

from ...util import clean_dict

DEFAULT_JOURNAL = "CORD-19 (missing journal name)"
DEFAULT_TITLE = "TITLE MISSING"


def _load_coi_statement(data: dict) -> str:
    pass


def _get_authors(authors: Iterable[list]) -> Iterator[dict]:
    for author in authors.split(";"):
        author = author.strip()
        if slugify(author) is not None:
            if "," not in author:
                yield {"name": author}
            else:
                last, first, *middle = author.strip().split(",")
                last, first, middle = (
                    last.strip(),
                    first.strip() or None,
                    " ".join(middle) or None,
                )
                if middle is None and first is not None:
                    first, *middle = first.split()
                    middle = " ".join(middle) or None

                if first is not None:
                    if middle is not None:
                        name = f"{first} {middle} {last}"
                    else:
                        name = f"{first} {last}"
                else:
                    middle = None
                    name = last

                yield {
                    "name": name,
                    "first_name": first,
                    "last_name": last,
                    "middle_names": middle,
                }


def wrangle(data: dict) -> dict:
    # identifiers
    data["pmid"] = data.pop("pubmed_id")
    data["magid"] = data.pop("mag_id")
    data["whoid"] = data.pop("who_covidence_id")
    data["s2id"] = data.pop("s2_id")
    data["arxivid"] = data.pop("arxiv_id")
    data["cordid"] = data.pop("cord_uid")

    data["journal"] = {"name": data.pop("journal", None) or DEFAULT_JOURNAL}
    if slugify(data["journal"]["name"]) is None:
        data["journal"]["name"] = DEFAULT_JOURNAL
    data["authors"] = [a for a in _get_authors(data.pop("authors"))]
    if slugify(data["title"] or "") is None:
        if data["abstract"]:
            data["title"] = data["abstract"][:300]
        else:
            data["title"] = DEFAULT_TITLE
    data["published_at"] = data.pop("publish_time", None)
    if data["published_at"] is not None:
        published_at = dateparse(data["published_at"])
        if published_at is not None:
            published_at = published_at.date()
        data["published_at"] = published_at
    return clean_dict(data)


def parse(fpath: str) -> Iterator[dict]:
    with open(fpath) as f:
        data = json.load(f)
    yield wrangle(data)
