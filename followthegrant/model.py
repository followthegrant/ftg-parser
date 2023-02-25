"""
ftg -> ftm model

model classes behave like the ftm model, aka. all props are multi values and a
`set` type for easy manipulation

the logic here is mostly used for deterministic id generation and automatic
initialization of props based on adjacent things
"""
from __future__ import annotations

from collections import defaultdict
from functools import cached_property
from typing import Any, Generator

import shortuuid
from banal import clean_dict
from followthemoney.helpers import remove_prefix_dates
from followthemoney.util import make_entity_id
from normality import slugify
from pydantic import BaseModel as PydanticBaseModel
from zavod.util import join_slug

from .coi import split_coi
from .exceptions import ModelException
from .ftm import (
    CE,
    Properties,
    SKDict,
    Values,
    get_first,
    get_firsts,
    make_proxy,
    make_safe_id,
    pick_name,
    schema,
    wrangle_person_names,
)
from .identifiers import IDENTIFIERS, clean_ident, pick_best
from .util import clean_date, clean_list, clean_value, ensure_set, fp


class BaseModel(PydanticBaseModel):
    _schema = "Thing"
    _id_prefix = None

    xref: Values = set()  # internal references for relations
    ident: Values = set()  # allow arbitrary identifiers for `make_id`
    _adjacents: ParsedResult | None = None
    _extra_data: dict[Any, Any] | None = {}

    class Config:
        keep_untouched = (cached_property,)
        underscore_attrs_are_private = True

    def __init__(
        self,
        adjacents: ParsedResult | None = None,
        extra_data: dict[Any, Any] | None = {},
        **data: SKDict,
    ):
        for k, v in data.items():
            values = ensure_set([clean_value(i) for i in clean_list(v)])
            if k in IDENTIFIERS:
                data[k] = clean_ident(values, k)
            else:
                data[k] = values
        super().__init__(**data)
        self._adjacents = adjacents or ParsedResult()
        self._extra_data = extra_data

    @cached_property
    def proxy(self) -> CE:
        return self.get_proxy()

    @cached_property
    def identifiers(self) -> dict[str, set[str]]:
        return clean_dict({k: getattr(self, k, None) for k in IDENTIFIERS})

    @cached_property
    def key_prefix(self):
        return slugify(self.__class__.__name__)

    @cached_property
    def id(self) -> str:
        return self.make_id()

    @cached_property
    def properties(self):
        return self.get_properties()

    def get_properties(self) -> Properties:
        # return cleaned properties as sets
        return {
            k: ensure_set(v)
            for k, v in clean_dict(self.dict()).items()
            if k in self.__fields__ and ensure_set(v)
        }

    @cached_property
    def fingerprint(self):
        if self.name:
            return fp(pick_name(self.name))
        return shortuuid.uuid()

    def make_id(self) -> str:
        """
        generate id from identifiers or `get_id_parts` or shortuuid
        """
        key, value = pick_best(self.identifiers)
        if value:
            return join_slug(self._id_prefix or key, value)

        id_ = join_slug(
            self.key_prefix,
            make_entity_id(*self.get_id_parts(), key_prefix=self.key_prefix),
        )
        if id_ is None:
            return f"{self.key_prefix}-{shortuuid.uuid()}"
        return id_

    def get_id_parts(self) -> Values:
        """
        if nothing else works, try to get first arbitrary ident or picked name as fingerprint
        """
        for ident in clean_list(self.ident):
            return [ident]
        return [self.fingerprint]

    def get_proxy(self) -> CE:
        proxy = make_proxy(
            {
                "id": self.id,
                "schema": self._schema,
                "properties": self.get_properties(),
            },
        )
        proxy = remove_prefix_dates(proxy)
        return proxy

    def merge(self, other: BaseModel) -> BaseModel:
        if self.__class__ != other.__class__:
            raise ModelException(f"Cannot merge {self} with {other}!")
        for key, update_value in other.properties.items():
            old_value = ensure_set(getattr(self, key))
            setattr(self, key, old_value | update_value)
        return self


class Journal(BaseModel, schema.Thing):
    def __init__(self, **data):
        data["description"] = data.get("description", "journal")
        data["publisher"] = clean_list(data.get("name"), data.get("publisher"))
        if "name" not in data:
            data["name"] = data.get("publisher")
        super().__init__(**data)


class Institution(BaseModel, schema.Organization):
    _schema = "Organization"

    xref_grant_funder: Values = set()

    def get_id_parts(self) -> Values:
        return clean_list(get_first(self.country), pick_name(self.name))


class Affiliation(BaseModel, schema.Membership):
    _schema = "Membership"

    def make_id(self) -> str:
        ident = make_safe_id(self.member, self.organization)
        return f"affiliation-{ident}"


class Employment(BaseModel, schema.Employment):
    _schema = "Employment"

    def make_id(self) -> str:
        ident = make_safe_id(self.employee, self.employer)
        return f"employment-{ident}"


class Author(BaseModel, schema.Person):
    _schema = "Person"

    xref_affiliation: Values = set()
    xref_employment: Values = set()
    xref_grant_recipient: Values = set()
    xref_grant_investigator: Values = set()

    def __init__(self, **data):
        data = wrangle_person_names(data)
        super().__init__(**data)

    @cached_property
    def names_key(self) -> tuple[str, str]:
        """
        return (first, last)
        """
        if self.name:
            *names, surname = pick_name(self.name).split()
            return " ".join(names), surname


class Documentation(BaseModel, schema.Documentation):
    _schema = "Documentation"

    def make_id(self) -> str:
        role, document, entity = get_firsts(self.role, self.document, self.entity)
        ident = make_safe_id(document, entity)
        return join_slug(role, ident)


class Statement(BaseModel, schema.PlainText):
    _schema = "PlainText"

    # for splitted statement
    _author: Author | None = None
    _is_splitted: bool | None = False

    def __init__(
        self,
        adjacents: ParsedResult | None = None,
        author: Author | None = None,
        **data,
    ):
        article = adjacents.article
        super().__init__(
            adjacents,
            date=article.publishedAt,
            publisher=article.publisher,
            publisherUrl=article.publisherUrl,
            sourceUrl=article.sourceUrl,
            parent=article.id,
            author=author.proxy.caption if author is not None else article.author,
            **data,
        )
        self._author = author
        self._is_splitted = author is not None

    @cached_property
    def splitted_statements(self) -> list[Statement]:
        return [s for s in self.get_splitted_statements()]

    @cached_property
    def label(self) -> str:
        return get_first(self.title)

    @cached_property
    def statement(self) -> str:
        # pick the longest one
        for stmt in sorted(self.bodyText, key=len):
            return stmt

    def make_id(self) -> str:
        return join_slug(self.label, self._adjacents.article.id)

    def get_splitted_statements(self) -> Generator[Statement, None, None]:
        article = self._adjacents.article
        if not self._is_splitted and article._adjacents.authors:
            # import ipdb

            # ipdb.set_trace()
            authors = {
                a.names_key: a for a in article._adjacents.authors if a.names_key
            }
            statements = split_coi(self.statement, authors.keys())
            for key, sentences in statements.items():
                yield self.__class__(
                    adjacents=self._adjacents,
                    author=authors[key],
                    bodyText=" ".join(sentences),
                    title=self.label,
                )

    def get_documentations(self) -> Generator[Documentation, None, None]:
        article = self._adjacents.article
        metadata = article.dict()
        metadata.pop("summary", None)
        metadata.pop("description", None)
        yield Documentation(
            document=article.id,
            entity=self.id,
            role=self.label,
            summary=self.bodyText,
            **metadata,
        )
        for statement in self.get_splitted_statements():
            yield Documentation(
                document=article.id,
                entity=statement._author.id,
                role=f"INDIVIDUAL_{self.label}",
                summary=statement.bodyText,
                **metadata,
            )


class Article(BaseModel, schema.Article):
    _schema = "Article"

    coi_statement: Values = set()
    ack_statement: Values = set()
    funding_statement: Values = set()
    xref_funding: Values = set()

    def __init__(
        self,
        adjacents: ParsedResult | None = None,
        **data,
    ):
        if adjacents:
            data = defaultdict(set, data)
            authors = clean_list(adjacents.authors)
            journal = adjacents.journal
            if journal:
                data["publisher"].update(journal.name)
                data["publisherUrl"].update(journal.publisherUrl)
            if "author" not in data:
                data["author"] = ensure_set([a.proxy.caption for a in authors])
        super().__init__(adjacents, **data)
        # FIXME erf
        dates = self.date | self.publishedAt
        dates = set([clean_date(d) for d in dates])
        self.date = dates
        self.publishedAt = dates

    def get_id_parts(self) -> list[str]:
        """
        invoked when no identifier given, try to generate a unique id that
        looks good and still can help deduping (unless we have to use uuid)
        """
        parts = []
        if self._adjacents.journal:
            parts.append(self._adjacents.journal.id)
        fingerprint = fp(pick_name(self.title))
        if fingerprint:
            parts.append(fingerprint)
        if len(parts) < 2:
            parts.append(shortuuid.uuid())
        return parts


class ProjectParticipant(BaseModel, schema.ProjectParticipant):
    _schema = "ProjectParticipant"

    def make_id(self) -> str:
        ident = make_safe_id(self.project, self.participant)
        return f"{self.key_prefix}-{ident}"


class Grant(BaseModel, schema.Project):
    _schema = "Project"
    _id_prefix = "grant"


class ParsedResult(PydanticBaseModel):
    """
    typed data from parser result
    """

    journal: SKDict | Journal | None = None
    article: SKDict | Article | None = None
    authors: list[SKDict | Author] | None = []
    institutions: list[SKDict | Institution] | None = []
    grants: list[SKDict | Grant] | None = []
    affiliations: list[SKDict | Affiliation] | None = []
    employments: list[SKDict | Employment] | None = []
