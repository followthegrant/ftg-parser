"""
ftg -> ftm model
"""

import re
import uuid
from collections.abc import Iterable
from typing import Optional, Tuple, Type

import countrytagger
import fingerprints
from followthemoney.util import make_entity_id
from normality import slugify

from .coi import flag_coi, split_coi
from .schema import (
    ArticleIdentifierInput,
    ArticleIdentifierOutput,
    ArticleInput,
    ArticleOutput,
    AuthorInput,
    AuthorOutput,
    CoiStatementInput,
    CoiStatementOutput,
    InstitutionInput,
    InstitutionOutput,
    JournalInput,
    JournalOutput,
)
from .util import cached_property, clean_dict, unique_list


class Base:
    def __init__(self, **data):
        self.input = self.InputSchema(**clean_dict(data))

    @cached_property
    def output(self):
        return self.OutputSchema(**self.get_output_data())

    @cached_property
    def key_prefix(self):
        return slugify(self.__class__.__name__)

    @cached_property
    def id(self) -> str:
        return make_entity_id(*self.get_id_parts(), key_prefix=self.key_prefix)

    def get_id_parts(self) -> Iterable[str]:
        if self.input.identifier is not None:
            return [self.input.identifier]
        return [fingerprints.generate(self.input.name)]

    def serialize(self) -> dict:
        return self.output.dict()

    @classmethod
    def make_id(cls, **data):
        entity = cls(**data)
        return entity.id


class Journal(Base):
    InputSchema = JournalInput
    OutputSchema = JournalOutput

    def get_output_data(self) -> Type[JournalOutput]:
        return {"id": self.id, "name": self.input.name, "website": self.input.website}


class Institution(Base):
    InputSchema = InstitutionInput
    OutputSchema = InstitutionOutput

    def __init__(self, **data):
        # clean name for better dedupe
        if data.get("name") is not None:
            name = " ".join(data["name"].split())
            data["name"] = re.sub(r"^(X?grid)?[\.\d\s]*", "", name)
        super().__init__(**data)

    def get_output_data(self):
        return {"id": self.id, "name": self.input.name, "country": self.country}

    def get_id_parts(self) -> Iterable[str]:
        if self.input.identifier is not None:
            return [self.input.identifier]
        # try a bit dedupe
        stops = ("department", "division", "of", "and", "for", "the")
        f = fingerprints.generate
        return sorted(
            list(set(p for p in f(self.input.name).split() if p not in stops))
        )

    @cached_property
    def country(self) -> Optional[str]:
        countries = sorted(
            countrytagger.tag_text_countries(self.input.name), key=lambda x: x[1]
        )
        if countries:
            # get the last iso code as this is the highest scored match
            return countries[-1][2]


class ArticleIdentifier(Base):
    InputSchema = ArticleIdentifierInput
    OutputSchema = ArticleIdentifierOutput

    # pmid has precedence as identifier to assign grants later
    identifiers = (
        ("pmid", "PubMed ID"),
        ("pmcid", "PubMed Central ID"),
        ("pmc", "Pubmed Central ID"),
        ("doi", "Digital Object Identifier"),
        ("mag", "Microsoft Academic Graph UID"),
        ("arxiv", "arXiv ID"),
        ("openaire", "OpenAIRE graph ID"),
        ("who", "WHO Covidence ID"),
        ("s2", "Semantic Scholar ID"),
        ("cord", "CORD-19 UID"),
    )
    identifiers_dict = dict(identifiers)

    def __init__(self, **data):
        # migrate PMC124 -> 124
        if data["key"] in ("pmcid", "pmc"):
            if str(data["value"]).lower().startswith("pmc"):
                data["value"] = data["value"][3:]
        super().__init__(**data)

    def get_id_parts(self) -> Iterable[str]:
        return [self.input.key, self.input.value]

    def get_article_id(self) -> str:
        return make_entity_id(self.input.key, self.input.value, key_prefix="article")

    def get_output_data(self) -> dict:
        return {
            "id": self.id,
            "key": self.input.key,
            "label": self.identifiers_dict[self.input.key],
            "value": self.input.value,
        }


class Author(Base):
    InputSchema = AuthorInput
    OutputSchema = AuthorOutput

    def get_id_parts(self) -> Iterable[str]:
        """
        author deduplication: fingerprinted name and first institution (sorted by id),
        or if no institution using random id
        """
        fingerprint = fingerprints.generate(self.input.name)
        if len(self.institutions):
            institution = sorted([i for i in self.institutions], key=lambda x: x.id)[0]
            return [fingerprint, institution.id]
        if len(self.input.identifier_hints):
            return [fingerprint, *self.input.identifier_hints]
        return [fingerprint, uuid.uuid4()]  # dedupe later

    @cached_property
    def institutions(self) -> Optional[Iterable[Institution]]:
        if self.input.institutions is not None:
            return [Institution(**i.dict()) for i in self.input.institutions]

    @cached_property
    def countries(self) -> Optional[Iterable[str]]:
        if self.institutions is not None:
            return unique_list([i.country for i in self.institutions])

    @cached_property
    def names_key(self) -> Tuple[str, str]:
        parts = self.input.name.split()
        return " ".join(parts[:-1]), parts[-1]

    def get_output_data(self) -> dict:
        first_name, middle_names, last_name = None, None, None
        if self.input.first_name is None and self.input.last_name is None:
            try:
                first_name, *middle_names, last_name = self.input.name.split()
                middle_names = " ".join(middle_names)
            except ValueError:
                pass
        return {
            "id": self.id,
            "name": self.input.name,
            "first_name": first_name or self.input.first_name,
            "last_name": last_name or self.input.last_name,
            "middle_names": middle_names or self.input.middle_names,
            "institutions": [i.serialize() for i in self.institutions],
            "countries": self.countries,
        }


class CoiStatement(Base):
    InputSchema = CoiStatementInput
    OutputSchema = CoiStatementOutput

    def __init__(self, article=None, author=None, **data):
        if article is not None:
            data["article_id"] = article.id
            data["article_title"] = article.title
            data["published_at"] = article.published_at
            data["journal_name"] = article.journal.name
        if author is not None:
            data["author_id"] = author.id
            data["author_name"] = author.name
        super().__init__(**data)

    @cached_property
    def flag(self):
        return flag_coi(self.input.text)

    @cached_property
    def title(self):
        if self.input.author_name is not None:
            return (
                f"individual conflict of interest statement ({self.input.author_name})"
            )
        return "conflict of interest statement (article)"

    @cached_property
    def role(self):
        if self.input.author_name is not None:
            return "individual conflict of interest statement"
        return "conflict of interest statement (article)"

    def get_id_parts(self) -> Iterable[str]:
        if self.input.article_id is not None and self.input.author_id is not None:
            return [self.input.article_id, self.input.author_id]
        if self.input.author_id is not None:
            return [self.input.author_id]
        if self.input.article_id is not None:
            return [self.input.article_id]
        return [self.title, self.input.text]

    def get_output_data(self) -> dict:
        if self.input.author_name is not None:
            authors = None
        elif self.input.article is not None:
            authors = sorted([a.name for a in self.input.article.authors])
        else:
            authors = None
        return {
            "id": self.id,
            "journal_name": self.input.journal_name,
            "article_id": self.input.article_id,
            "article_title": self.input.article_title,
            "author_id": self.input.author_id,
            "author_name": self.input.author_name,
            "authors": authors,
            "published_at": self.input.published_at,
            "title": self.title,
            "role": self.role,
            "text": " ".join(self.input.text.split()),
            "flag": self.flag,
            "index_text": f"flag:{int(self.flag)}",
        }


class Article(Base):
    InputSchema = ArticleInput
    OutputSchema = ArticleOutput

    def __init__(self, **data):
        # id migration
        if "identifiers" in data:
            i = data["identifiers"]
            if "pmcid" not in i and "pmc" in i:
                i["pmcid"] = i["pmc"]
                del i["pmc"]
            data["identifiers"] = i
        super().__init__(**data)

    @cached_property
    def journal(self):
        return Journal(**self.input.journal.dict())

    @cached_property
    def identifiers(self) -> Optional[Iterable[ArticleIdentifier]]:
        identifiers = [
            {"key": k, "value": v} for k, v in self.input.identifiers.items()
        ]
        return [ArticleIdentifier(**i) for i in identifiers]

    @cached_property
    def authors(self) -> Iterable[Author]:
        # use article id for identifier hint if nothing else available
        _authors = [a.dict() for a in self.input.authors]
        _authors = [
            {
                **a,
                **{
                    "identifier_hints": [self.id]
                    if not a.get("identifier_hints")
                    else []
                },
            }
            for a in _authors
        ]
        return [Author(**a) for a in _authors]

    @cached_property
    def coi_statement(self):
        if self.input.coi_statement is not None:
            return CoiStatement(article=self.output, text=self.input.coi_statement)

    @cached_property
    def individual_coi_statements(self) -> Iterable[CoiStatement]:
        if self.input.coi_statement is not None:
            authors = {a.names_key: a.output for a in self.authors}
            statements = split_coi(self.input.coi_statement, authors.keys())
            return [
                CoiStatement(
                    article=self.output, author=authors[key], text="\n".join(sentences)
                )
                for key, sentences in statements.items()
            ]

    def get_id_parts(self) -> Iterable[str]:
        if self.input.id is not None:
            return [self.input.id]  # allow literal id values
        identifiers = dict((i.output.key, i.output.value) for i in self.identifiers)
        for key, _ in ArticleIdentifier.identifiers:
            if key in identifiers:
                return key, identifiers[key]
            return self.input.title, self.journal.id

    def get_output_data(self) -> dict:
        index_text = (
            "\n".join((f"{i.output.key}:{i.output.value}" for i in self.identifiers))
            or None
        )
        return {
            "id": self.id,
            "title": self.input.title,
            "abstract": self.input.abstract,
            "published_at": self.input.published_at,
            "authors": [a.serialize() for a in self.authors],
            "keywords": self.input.keywords,
            "index_text": index_text,
            "journal": self.journal.serialize(),
        }
