"""
ftg -> ftm model
"""
from __future__ import annotations

from functools import lru_cache
from typing import Iterator

import countrytagger
import shortuuid
from banal import ensure_list
from fingerprints import generate as fp
from followthemoney import model
from followthemoney.proxy import E
from followthemoney.util import make_entity_id
from normality import collapse_spaces, slugify
from pydantic import BaseModel

from .coi import flag_coi, split_coi
from .ner import analyze
from .util import cached_property


def get_first_props(*props: list[str] | None) -> Iterator[str]:
    for prop in props:
        if prop:
            for value in prop:
                if fp(value):
                    yield value
                    break


@lru_cache
def guess_country(value: str) -> str | None:
    countries = sorted(countrytagger.tag_text_countries(value), key=lambda x: x[1])
    if countries:
        # get the last iso code as this is the highest scored match
        return countries[-1][2]


class Base:
    schema = None
    identifier_keys: tuple[str] = ()

    def __init__(self, **data):
        for k, v in data.items():
            data[k] = list(set([collapse_spaces(i) for i in ensure_list(v)]))
        super().__init__(**data)

    @cached_property
    def identifiers(self) -> dict[str | None]:
        return {k: getattr(self, k, None) for k in self.identifier_keys}

    @cached_property
    def key_prefix(self):
        return slugify(self.__class__.__name__)

    @cached_property
    def id(self) -> str:
        for key, values in self.identifiers.items():
            for value in values:
                if value is not None and fp(value):
                    return f'{key}-{value.replace(" ", "-")}'
        id_parts = self.get_id_parts()
        if not id_parts:
            id_parts = [shortuuid.uuid()]
        return f"{self.key_prefix}-{make_entity_id(*self.get_id_parts(), key_prefix=self.key_prefix)}"

    def get_id_parts(self) -> list[str] | None:
        if self.ident:
            return self.ident
        if self.name:
            for name in self.name:
                if fp(name):
                    return [fp(name)]

    def get_proxy(self) -> E:
        return model.get_proxy(
            {"id": self.id, "schema": self.schema, "properties": self.dict()}
        )

    def get_proxies(self) -> Iterator[E]:
        yield self.get_proxy()

    @cached_property
    def proxy(self):
        return self.get_proxy()

    @cached_property
    def article_meta(self):
        if hasattr(self, "article"):
            return {
                "publishedAt": self.article.proxy.get("publishedAt"),
                "publisher": self.article.proxy.get("publisher"),
                "date": self.article.proxy.get("publishedAt"),
            }

    @classmethod
    def make_id(cls, **data):
        entity = cls(**data)
        return entity.id


class Identifiers(BaseModel):
    ident: list[str] | None = None  # allow aribtrary identifiers
    doi: list[str] | None = None
    rorId: list[str] | None = None
    gridId: list[str] | None = None
    orcId: list[str] | None = None
    openaireId: list[str] | None = None
    s2Id: list[str] | None = None
    issn: list[str] | None = None


class Journal(Base, Identifiers):
    schema = "LegalEntity"
    identifier_keys = ("issn",)

    name: list[str]
    website: list[str] | None = None
    issn: list[str] | None = None
    legalForm = ["journal"]


class Institution(Base, Identifiers):
    schema = "Organization"
    identifier_keys = ("rorId", "gridId")

    name: list[str] | None = None
    country: list[str] | None = None
    description: list[str] | None = None  # department

    def __init__(self, **data):
        if "country" not in data:
            value = " ".join(
                sorted(
                    list(set([fp(n) for n in ensure_list(data.get("name"))]))
                )  # normalize for lru
            )
            data["country"] = guess_country(value)
        super().__init__(**data)

    def get_id_parts(self) -> list[str] | None:
        parts = get_first_props(self.country, self.name)
        if parts:
            return [fp(p) for p in parts]


class Author(Base, Identifiers):
    schema = "Person"
    identifier_keys = ("orcid", "openaireid", "s2id")

    article: Article | None
    name: list[str] | None
    firstName: list[str] | None
    lastName: list[str] | None
    middleNames: list[str] | None
    affiliations: list[Institution] | None
    country: list[str] | None

    def __init__(self, **data):
        name = ensure_list(data.get("name"))
        if not name:
            first = (ensure_list(data.get("firstName")),)
            middle = (ensure_list(data.get("middleNames")),)
            last = (ensure_list(data.get("lastName")),)
            name = list(get_first_props(first, middle, last))
            if name:
                data["name"] = [" ".join(name)]
        country = ensure_list(data.get("country"))
        if not country:
            data["country"] = []
            for institution in ensure_list(data.get("institutions")):
                for country in institution.country:
                    data["country"].append(country)
        super().__init__(data)

    def get_id_parts(self) -> list[str]:
        """
        author deduplication: if no identifier (orcid), use fingerprinted name
        and first institution (sorted by id), or if no institution using random
        id
        """
        if self.institutions:
            institution = sorted([i for i in self.institutions], key=lambda x: x.id)[0]
            return [self.fingerprint, institution.id]
        return [self.fingerprint, shortuuid.uuid()]

    @cached_property
    def fingerprint(self):
        if self.name:
            return " ".join([fp(n) for n in self.name])
        return shortuuid.uuid()

    @cached_property
    def names_key(self) -> tuple[str, str]:
        if self.name:
            *names, surname = self.name[0]
            return " ".join(names), surname

    def get_proxies(self):
        yield self.proxy
        for affiliation in self.affiliations:
            yield affiliation.get_proxy()
            yield model.get_proxy(
                {
                    "id": f"{self.id}-membership-{affiliation.id}",
                    "schema": "Membership",
                    "properties": {
                        **self.article_meta,
                        **{
                            "member": self.proxy,
                            "organization": affiliation.proxy,
                            "role": "affiliation",
                        },
                    },
                }
            )


class Statement(Base):
    schema = "PlainText"

    authors: list[Author] | None = None  # input data

    role: str | None = None
    article: Article | None = None
    bodyText: list[str] | None = None
    author: list[str] | None = None  # ftm prop
    publishedAt: list[str] | None = None

    def get_proxies(self):
        yield self.get_proxy()
        data = self.reference_data
        yield model.get_proxy(
            {
                "id": self.id,
                "schema": "Documentation",
                "properties": {**data, **{"document": self.article}},
            }
        )
        for author in self.authors:
            yield model.get_proxy(
                {
                    "id": f"{author.id}-{self.key_prefix}-{self.id}",
                    "schema": "Documentation",
                    "properties": {
                        **data,
                        **{
                            "entity": author.id,
                            "document": self.id,
                            "role": f"individual {self.role}",
                        },
                    },
                }
            )

    @property
    def reference_data(self):
        return {
            **self.article_meta,
            **{
                "document": self.article,
                "entity": self.id,
                "role": self.role,
                "summary": self.bodyText,
                "indexText": self.indexText,
            },
        }

    def get_id_parts(self) -> list[str]:
        if self.article:
            return [self.article.id]


class CoiStatement(Statement):
    role = "conflict of interest statement"
    indexText: list[str] | None = None

    @cached_property
    def flag(self):
        return flag_coi(" ".join(ensure_list(self.bodyText)))


class AckStatement(Statement):
    role = "acknowledgement statement"


class FundingStatement(Statement):
    role = "funding statement statement"


class Article(Base, Identifiers):
    schema = "Article"
    identifier_keys = ("doi", "pmc", "pmid", "magId", "arxivId", "openaireId", "s2id")

    journal: Journal | None = None
    coi_statement: CoiStatement | None = None
    ack_statement: AckStatement | None = None
    funding_statement: FundingStatement | None = None
    authors: list[Author] | None = None  # input prop for author data

    title: list[str] | None = None
    publishedAt: list[str] | None = None
    publisher: list[str] | None = None
    summary: list[str] | None = None
    author: list[str] | None = None  # ftm prop for authors
    keywords: list[str] | None = None
    sourceUrl: list[str] | None = None

    def __init__(self, **data):
        if "author" not in data and "authors" in data:
            data["author"] = [e.proxy.caption for e in data["authors"]]
        super().__init__(**data)

    @cached_property
    def individual_coi_statements(self) -> list[CoiStatement]:
        if self.coi_statement and self.authors:
            authors = {a.names_key: a for a in self.authors}
            statements = split_coi(self.coi_statement, authors.keys())
            return [
                CoiStatement(
                    article=self,
                    bodyText=" ".join(sentences),
                    author=authors[key],
                )
                for key, sentences in statements.items()
            ]

    @cached_property
    def individual_ack_statements(self) -> list[AckStatement]:
        if self.ack_statement and self.authors:
            authors = {a.names_key: a for a in self.authors}
            statements = split_coi(self.ack_statement, authors.keys())
            return [
                AckStatement(
                    article=self, author=authors[key], bodyText=" ".join(sentences)
                )
                for key, sentences in statements.items()
            ]

    @cached_property
    def individual_funding_statements(self) -> list[FundingStatement]:
        if self.funding_statement and self.authors:
            authors = {a.names_key: a for a in self.authors}
            statements = split_coi(self.funding_statement, authors.keys())
            return [
                FundingStatement(
                    article=self, author=authors[key], bodyText=" ".join(sentences)
                )
                for key, sentences in statements.items()
            ]

    def get_id_parts(self) -> list[str]:
        parts = list(get_first_props(self.title))
        if self.journal:
            parts.append(self.journal.id)
        return [parts]

    def get_proxies(self) -> Iterator[E]:
        yield self.proxy
        if self.coi_statement:
            yield from self.coi_statement.get_proxies()
        if self.ack_statement:
            yield from self.ack_statement.get_proxies()
        if self.funding_statement:
            yield from self.funding_statement.get_proxies()
        authorship = {
            "date": self.date,
            "publishedAt": self.date,
            "publisher": self.publisher,
            "role": "author",
        }
        for author in self.authors:
            yield model.make_entity(
                {
                    "id": f"{self.id}-authorship-{author.id}",
                    "schema": "Documentation",
                    "properties": {
                        **authorship,
                        **{"entity": author.id, "document": self.id},
                    },
                }
            )


def make_entities(
    data: Journal | Institution | Author | Article | Statement,
) -> Iterator[E]:
    for entity in data.get_proxies():
        for e in analyze(entity):
            yield e
