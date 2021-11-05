import logging

import lxml.etree as ET

from .pubmed import pubmed

log = logging.getLogger(__name__)


def _get_coi_statement(tree):
    def _parse():
        for xpath in (
            './/notes[@notes-type="competing-interest-statement"]',
            ".//ack",
        ):
            for el in tree.xpath(xpath):
                yield "\n".join(el.itertext())

    return " ".join(_parse())


def biorxiv(fpath):
    # first try pubmed parsing:
    data = pubmed(fpath)
    keys_missing = (
        set(
            (
                "abstract",
                "journal",
                "title",
                "published_at",
                "authors",
                "identifiers",
                "coi_statement",
            )
        )
        - set(data.keys())
    )
    if not keys_missing:
        return data

    if keys_missing == {"coi_statement"}:
        tree = ET.parse(fpath)
        data["coi_statement"] = _get_coi_statement(tree)
        return data

    import ipdb

    ipdb.set_trace()
