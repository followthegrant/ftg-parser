# https://github.com/opensanctions/opensanctions/blob/main/opensanctions/core/statements.py

from datetime import datetime
from hashlib import sha1
from typing import List, TypedDict

from followthemoney import model


class Statement(TypedDict):
    """A single statement about a property relevant to an entity.
    For example, this could be useddocker to say: "In dataset A, entity X has the
    property `name` set to 'John Smith'. I first observed this at K, and last
    saw it at L."
    Null property values are not supported. This might need to change if we
    want to support making property-less entities.
    """

    id: str
    entity_id: str
    canonical_id: str
    prop: str
    prop_type: str
    schema: str
    value: str
    dataset: str
    first_seen: datetime
    last_seen: datetime


def stmt_key(dataset, entity_id, prop, value):
    """Hash the key properties of a statement record to make a unique ID."""
    key = f"{dataset}.{entity_id}.{prop}.{value}"
    return sha1(key.encode("utf-8")).hexdigest()


def statements_from_entity(
    entity: dict, dataset: str, unique: bool = False
) -> List[Statement]:
    entity = model.get_proxy(entity)
    if entity.id is None or entity.schema is None:
        return []
    for prop, value in entity.itervalues():
        if value:
            stmt: Statement = {
                "id": stmt_key(dataset, entity.id, prop.name, value),
                "entity_id": entity.id,
                "canonical_id": entity.id,
                "prop": prop.name,
                "prop_type": prop.type.name,
                "schema": entity.schema.name,
                "value": value,
                "dataset": dataset,
                "first_seen": datetime.now().isoformat(),
                "last_seen": datetime.now().isoformat(),
            }
            yield stmt
