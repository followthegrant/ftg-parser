import csv
import logging
import re
import sys

import pubmed_parser as pp
from followthemoney.util import make_entity_id
from html2text import html2text
from pubmed_parser.utils import read_xml

log = logging.getLogger()
log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
log.addHandler(handler)


IDS = ("pmid", "pmc", "doi")


def _get_doc_id(data):
    for key in IDS:
        if data.get(key):
            return key, data[key]


def extract_coi_from_converted_fulltext(fpath, coi_text):
    with open(fpath) as f:
        article_text = html2text(f.read())
    match = re.search(coi_text, article_text, flags=re.IGNORECASE)
    if match is not None:
        start_pos = match.start()
        full_coi_text = ""
        i = 0
        while True:
            char = article_text[start_pos + i]
            try:
                if (
                    char == "\n"
                    and article_text[start_pos + i + 1] == "\n"
                    and i > len(coi_text)
                ):
                    break
            except IndexError:  # end of text
                break
            else:
                full_coi_text += article_text[start_pos + i]
                i += 1

        return full_coi_text.replace("\t", " ").replace("\n", " ")


def extract_coi_from_fulltext(fpath):
    tree = read_xml(fpath)
    xpath = './/*[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"interest") and (contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"competing") or contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"declaring") or contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"conflict"))]'  # noqa
    coi_statement = " ".join(
        " ".join(t for t in el.itertext()) for el in tree.xpath(xpath)
    )
    if len(coi_statement) > 36:
        return coi_statement
    if coi_statement:
        return extract_coi_from_converted_fulltext(fpath, coi_statement)


def extract_fulltext_coi():
    writer = csv.writer(sys.stdout)
    for fpath in sys.stdin:
        fpath = fpath.strip()
        try:
            data = pp.parse_pubmed_xml(fpath)
            if not data["coi_statement"]:
                doc_id = make_entity_id(*_get_doc_id(data))
                coi_statement = extract_coi_from_fulltext(fpath)
                if coi_statement:
                    writer.writerow((fpath, data["pmid"], doc_id, coi_statement))
        except Exception as e:
            log.error(f'Error: `{e.__class__.__name__}`: "{e}"')
