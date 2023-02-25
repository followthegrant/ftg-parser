"""
helpers for ftm related stuff, mostly to use pydantic for proxies
"""
from typing import Any, Generator, Iterable

from followthemoney import model
from followthemoney.model import registry
from followthemoney.proxy import E, EntityProxy
from followthemoney.util import make_entity_id
from nomenklatura.entity import CE, CompositeEntity
from pydantic import BaseModel, create_model
from zavod.util import join_slug

from .exceptions import IdentificationException
from .util import clean_list, ensure_set, fp

EGenerator = Generator[CE, None, None]
SKDict = dict[str, Any]
Values = set[str] | None
Properties = dict[Values]


def create_schema(schema: str) -> BaseModel:
    schema = model.get(schema)
    return create_model(
        schema.name,
        **{k: (Values, set()) for k in schema.properties},
        __base__=BaseModel,
    )


class schema:
    Thing = create_schema("Thing")
    Organization = create_schema("Organization")
    Article = create_schema("Article")
    Person = create_schema("Person")
    Project = create_schema("Project")
    ProjectParticipant = create_schema("ProjectParticipant")
    Documentation = create_schema("Documentation")
    Membership = create_schema("Membership")
    Employment = create_schema("Employment")
    PlainText = create_schema("PlainText")


def make_safe_id(*values: Iterable[Values]) -> str:
    id_ = make_entity_id(join_slug(*get_firsts(*values)))
    if id_ is None:
        raise IdentificationException(f"No entity id: {values}")
    return id_


def make_proxy(data: SKDict | CE | E) -> CE:
    if isinstance(data, CompositeEntity):
        return data
    if isinstance(data, EntityProxy):  # uplevel
        data = data.to_dict()
    return CompositeEntity.from_dict(model, data)


def get_first(value: Values, strict: bool | None = True) -> str | None:
    for value in sorted(clean_list(value)):
        if (strict and fp(value)) or (not strict and value):
            return value


def get_firsts(
    *values: Iterable[Values], strict: bool | None = True
) -> Generator[Any | None, None, None]:
    for value in values:
        yield get_first(value, strict)


def pick_name(names: Values) -> str:
    return registry.name.pick(names)


def wrangle_person_names(data: SKDict) -> SKDict:
    first, middle, last = [
        *get_firsts(
            data.get("firstName"),
            data.get("middleNames"),
            data.get("lastName"),
        )
    ]

    full_name = " ".join(clean_list(first, middle, last))
    data["name"] = ensure_set(data.get("name"))
    data["name"].add(full_name)

    rev_parts = clean_list(last, first, middle)
    reversed_name = " ".join(rev_parts)
    data["name"].discard(reversed_name)

    if len(rev_parts) > 1:
        last, *rest = rev_parts
        reversed_name_c = f"{last}, {' '.join(rest)}"
        data["name"].discard(reversed_name_c)
    return data
