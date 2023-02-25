"""
OPENAIRE dataset

source data:

expects as input file paths to either gzipped or already extracted json files,
which have one json record per line

usage:

    find ./data/ -type f -name "*.json.gz" | ftg parse openaire

"""

from collections import defaultdict
from typing import Generator

from html2text import html2text

from ...transform import ParsedResult
from ...util import clean_list
from ..util import iter_jsonl, parse_dict

JOURNAL = {
    "publisher": "name",
    "countainer.issnOnline": "issn",
    "container.issnPrinted": "issn",
    "container.name": "name",
}

ARTICLE = {
    "maintitle": "title",
    "description": "summary",
    "publicationdate": "publishedAt",
    "id": "openaireId",
    "subjects[].subject.value": "keywords",
}

AUTHOR = {
    "fullname": "name",
    "name": "firstName",
    "surname": "lastName",
    "pid.id.value": "orcId",
}

IDENTS = {
    "doi": "doi",
    "pmid": "pmid",
    "pmc": "pmc",
    "arXiv": "arxivId",
}


def wrangle(data: dict) -> ParsedResult:
    result = defaultdict(list)
    result["journal"] = parse_dict(data, JOURNAL)
    result["journal"]["publisher"].add("OpenAIRE Research Graph")
    result["article"] = parse_dict(data, ARTICLE)
    result["article"]["summary"] = [
        html2text(i) for i in clean_list(result["article"]["summary"])
    ]
    for ident in data.get("pid", []):
        key = IDENTS.get(ident["scheme"], "ident")
        result["article"][key].add(ident["value"])
    for author in data.get("author", []):
        result["authors"].append(parse_dict(author, AUTHOR))

    return ParsedResult(**result)


def parse(fpath: str) -> Generator[ParsedResult, None, None]:
    for data in iter_jsonl(fpath):
        yield wrangle(data)
