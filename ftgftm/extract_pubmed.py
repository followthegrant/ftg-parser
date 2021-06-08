import json
import logging
import re
import sys
from datetime import datetime

import countrytagger
import fingerprints
import pubmed_parser as pp
from followthemoney import model

from .get_fulltext_coi import extract_coi_from_fulltext
from .split_coi import split_coi

log = logging.getLogger()
log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
log.addHandler(handler)


# pmid has precedence as identifier to assign grants later
IDS = ("pmid", "pmc", "doi")


# institution normalization
STOPWORDS = ("department", "division", "of", "and", "for", "the")


# fingerprinting shorthand
def _(x):
    return fingerprints.generate(x)  # noqa


def _get_doc_id(data):
    for key in IDS:
        if data.get(key):
            return key, data[key]
    return "article", data["full_title"], data["journal"]


def make_entities(data, fpath, meta_only):
    authors = data.get("author_list", [])
    # authors = [[i or '' for i in a] for a in data.get('author_list', [])]

    doc_id = _get_doc_id(data)

    publication_year = data["publication_year"]
    try:
        publication_date = datetime.strptime(data["publication_date"], "%d-%m-%Y")
    except ValueError:
        publication_date = publication_year

    document = model.make_entity("Document")
    document.make_id(*doc_id)
    # document.add('foreign_id', data['pmid'])
    document.add("title", data["full_title"])
    document.add("summary", data["abstract"])
    document.add("publishedAt", publication_date)
    document.add("publisher", data["journal"])
    document.add("author", ["{} {}".format(*reversed(a[:2])) for a in authors])
    document.add("messageId", "#".join(doc_id[:2]))
    document.add(
        "notes",
        "identifiers:\n\n{}".format(
            "\n".join("#".join((k, data[k])) for k in IDS if data[k])
        ),
    )
    kwds = data.get("keywords", "").split(";")
    if kwds:
        document.add("keywords", kwds)
    else:
        document.add("keywords", data["subjects"].split(";"))
    document_id = document.id
    yield document.to_dict()

    if not meta_only:
        publisher = model.make_entity("LegalEntity")
        publisher.make_id(_(data["journal"]))
        publisher.add("name", data["journal"])
        yield publisher.to_dict()

        # link article -> publisher
        published = model.make_entity("Documentation")
        published.make_id(publisher.id, document.id)
        published.add("entity", publisher)
        published.add("document", document)
        published.add("role", "publisher")
        published.add("date", publication_date)
        yield published.to_dict()

        affiliates = {}
        for key, name in dict(data.get("affiliation_list", [])).items():
            affiliate = model.make_entity("Organization")
            # move cleaning to another module?
            name = re.sub(r"^[Xgrid\.\d\s]*", "", str(name))
            name = re.sub(r"\d*", "", name)
            affiliate.add("name", name)
            affiliate.make_id(
                *sorted(
                    list(set(p for p in str(_(name)).split() if p not in STOPWORDS))
                )
            )
            if affiliate.id:
                countries = sorted(
                    countrytagger.tag_text_countries(name), key=lambda x: x[1]
                )
                if countries:
                    affiliate.add("country", countries[-1][2])
                yield affiliate.to_dict()
                affiliates[key] = affiliate

        author_affiliations = {tuple(a[:2]): [] for a in authors}
        for a in authors:
            author_affiliations[tuple(a[:2])].append(affiliates.get(a[2]))

        coi = None
        if not data["coi_statement"]:
            # try to extract from fulltext
            coi_statement = extract_coi_from_fulltext(fpath) or ""
            if len(coi_statement) > 22:
                data["coi_statement"] = coi_statement

        if data["coi_statement"]:
            # the full coi statement
            coi = model.make_entity("PlainText")
            coi.make_id("coi", document_id)
            coi.add("title", "conflict of interest statement (article)")
            coi.add("bodyText", data["coi_statement"])
            coi.add("publisher", data["journal"])
            coi.add("date", publication_date)
            coi.add("parent", document_id)

            # link article -> coi statement
            coi_ref = model.make_entity("Documentation")
            coi_ref.make_id("coi_ref", document_id)
            coi_ref.add("date", publication_date)
            coi_ref.add("publisher", data["journal"])
            coi_ref.add("document", document)
            coi_ref.add("entity", coi)
            coi_ref.add("role", "conflict of interest statement")
            coi_ref.add("summary", data["coi_statement"])
            yield coi_ref.to_dict()

            # coi splitting for authors
            coi_authors = [tuple(reversed(a[:2])) for a in authors]
            coi_statements = {
                a if a == "all" else " ".join(a): statements
                for a, statements in split_coi(data["coi_statement"], coi_authors)
            }

        for last_name, first_name, affiliate_key in authors:
            author = model.make_entity("Person")
            author_name = f"{first_name} {last_name}"
            # FIXME deduplication currently:
            # make id based on fingerprinted name and first affiliation (sorted by id)
            # found in this paper
            aff = sorted(
                a.id for a in author_affiliations[(last_name, first_name)] if a
            )
            if len(aff):
                author.make_id(_(author_name), aff[0])
            else:
                author.make_id(_(author_name), publisher.id)
            author.add("name", f"{first_name} {last_name}")
            author.add("firstName", first_name)
            author.add("lastName", last_name)

            if author.id:
                if coi is not None:
                    coi.add("author", author.get("name"))

                    # link author -> full coi statement
                    author_coi_ref = model.make_entity("Documentation")
                    author_coi_ref.make_id("author_coi_ref", document_id, author.id)
                    author_coi_ref.add("date", publication_date)
                    author_coi_ref.add("document", coi)
                    author_coi_ref.add("entity", author)
                    author_coi_ref.add("publisher", data["journal"])
                    author_coi_ref.add("role", "conflict of interest statement")
                    author_coi_ref.add("summary", data["coi_statement"])
                    yield author_coi_ref.to_dict()

                    for identifier in (author_name, "all"):
                        if identifier in coi_statements:
                            # individual coi statement for author
                            author_coi = model.make_entity("PlainText")
                            author_coi.make_id(
                                "author_coi", document_id, author.id, identifier
                            )
                            author_coi.add(
                                "title",
                                f"individual conflict of interest statement ({author_name})",  # noqa
                            )
                            author_coi.add("bodyText", coi_statements[identifier])
                            author_coi.add("publisher", data["journal"])
                            author_coi.add("date", publication_date)
                            author_coi.add("author", author_name)
                            yield author_coi.to_dict()

                            # link author -> individual coi statement
                            author_coi_ref = model.make_entity("Documentation")
                            author_coi_ref.make_id(
                                "author_coi_ref", document_id, author.id, author_coi.id
                            )
                            author_coi_ref.add("date", publication_date)
                            author_coi_ref.add("publisher", data["journal"])
                            author_coi_ref.add("document", author_coi)
                            author_coi_ref.add("entity", author)
                            author_coi_ref.add(
                                "role", "individual conflict of interest statement"
                            )
                            author_coi_ref.add("summary", coi_statements[identifier])
                            yield author_coi_ref.to_dict()

                            # link individual coi statement -> article
                            author_coi_article_ref = model.make_entity("Documentation")
                            author_coi_article_ref.make_id(
                                "author_coi_article_ref",
                                document_id,
                                author.id,
                                author_coi.id,
                            )
                            author_coi_article_ref.add("date", publication_date)
                            author_coi_article_ref.add("publisher", data["journal"])
                            author_coi_article_ref.add("document", document)
                            author_coi_article_ref.add("entity", author_coi)
                            author_coi_article_ref.add(
                                "role", "individual conflict of interest statement"
                            )
                            author_coi_article_ref.add(
                                "summary", coi_statements[identifier]
                            )
                            yield author_coi_article_ref.to_dict()

                # link author -> article
                authorship = model.make_entity("Documentation")
                authorship.make_id("authorship", author.id, document_id)
                authorship.add("entity", author)
                authorship.add("document", document_id)
                authorship.add("date", publication_date)
                authorship.add("role", "author")
                yield authorship.to_dict()

                # link author -> institutions
                affiliate = affiliates.get(affiliate_key)
                if affiliate:
                    affiliated = model.make_entity("Membership")
                    affiliated.make_id("affiliation", author.id, affiliate.id)
                    affiliated.add("member", author)
                    affiliated.add("organization", affiliate)
                    affiliated.add("date", publication_year)
                    affiliated.add("role", "affiliated with")
                    affiliated.add("publisher", data["journal"])
                    yield affiliated.to_dict()

                    for country in affiliate.countries:
                        author.add("country", country)
                yield author.to_dict()

        if coi is not None:
            yield coi.to_dict()


def extract(debug=False, meta_only=False):
    for fpath in sys.stdin:
        fpath = fpath.strip()
        try:
            data = pp.parse_pubmed_xml(fpath)
            for entity in make_entities(data, fpath, meta_only):
                sys.stdout.write(json.dumps(entity) + "\n")
        except Exception as e:
            log.error(f'Error: `{e.__class__.__name__}`: "{e}"')
            if debug:
                raise e
