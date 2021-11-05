"""
turn extracted ftg data into ftm entities based on `mapping.yml`
"""

import os

from followthemoney import model
from followthemoney.cli.util import load_mapping_file

from .mapping import MappedModel
from .schema import ArticleFullOutput


def load_mapping():
    mapping_path = os.path.join(os.path.dirname(__file__), "mapping.yml")
    config = load_mapping_file(mapping_path)
    mapping = config["ftg"]["query"]
    mapping["csv_url"] = "/dev/null"
    return model.make_mapping(mapping)


QUERY = load_mapping()


def make_entities(data: ArticleFullOutput):
    data = MappedModel(data)
    for item in data:
        item = item.dict()
        if QUERY.source.check_filters(item):
            res = QUERY.map(item)
            for entity in res.values():
                yield entity
