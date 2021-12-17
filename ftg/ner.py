"""
use NER extraction from alephs `ingest-file` to extract companies from coi
statements and abstracts

requirements:
install the spacy models and set the INGESTORS_LID_MODEL_PATH properly
(see Dockerfile here: https://github.com/alephdata/ingest-file)

download the type predict model:
curl -o "./models/model_type_prediction.ftz" "https://public.data.occrp.org/develop/models/types/type-08012020-7a69d1b.ftz"
"""

from followthemoney import model
from followthemoney.types import registry
from followthemoney.util import make_entity_id
from ingestors.analysis import Analyzer
from ingestors.analysis.aggregate import TagAggregator
from ingestors.analysis.extract import extract_entities
from ingestors.analysis.language import detect_languages
from ingestors.analysis.util import DOCUMENT, text_chunks


def analyze(entity):
    if not entity.schema.is_a(DOCUMENT):
        yield entity
        return

    aggregator = TagAggregator()
    texts = entity.get_type_values(registry.text)
    countries = set()

    for text in text_chunks(texts):
        detect_languages(entity, text)
        for (prop, tag) in extract_entities(entity, text):
            aggregator.add(prop, tag)

    results = list(aggregator.results())
    for (key, prop, values) in results:
        if prop.type == registry.country:
            countries.add(key)

    for (key, prop, values) in results:
        label = values[0]
        if prop.type == registry.name:
            label = registry.name.pick(values)

        schema = Analyzer.MENTIONS.get(prop)
        if schema == "Organization":
            mention = model.make_entity("Mention")
            mention.make_id("mention", entity.id, prop, key)
            mention.add("resolved", make_entity_id(key))
            mention.add("document", entity.id)
            mention.add("name", values)
            mention.add("detectedSchema", schema)
            mention.add("contextCountry", countries)
            yield mention

        entity.add(prop, label, cleaned=True, quiet=True)

    yield entity
