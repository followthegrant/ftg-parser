import logging
from collections import defaultdict
from pathlib import Path

from banal import ensure_dict, ensure_list, is_mapping
from lxml import etree
from normality import collapse_spaces

log = logging.getLogger(__name__)


def remove_namespace(tree: etree._ElementTree):
    """
    Strip namespace from parsed XML
    """
    for node in tree.iter():
        try:
            has_namespace = node.tag.startswith("{")
        except AttributeError:
            continue  # node.tag is not a string (node is a comment or similar)
        if has_namespace:
            node.tag = node.tag.split("}", 1)[1]


def read_xml(path: Path | etree._Element) -> etree._ElementTree | None:
    if isinstance(path, etree._Element):
        return path
    tree = None
    try:
        tree = etree.parse(path)
        if path.suffix == "nxml":
            remove_namespace(tree)
    except Exception:
        try:
            tree = etree.fromstring(path)
        except Exception as e:
            log.error(f"Error parsing xml at `{path}`: {e}")
    return tree


def parse_props(tree: etree._Element, mapping: dict[str, list]) -> dict[str, list]:
    data = defaultdict(list)
    for prop, paths in mapping.items():
        if is_mapping(paths):
            if "from" in paths:
                subpaths = ensure_list(paths["from"])
                prop_mapping = ensure_dict(paths.get("properties"))
                for subpath in subpaths:
                    for subtree in tree.xpath(subpath):
                        data[prop].append(parse_props(subtree, prop_mapping))
        else:
            for path in paths:
                try:
                    values = tree.xpath(path)
                    for value in values:
                        if isinstance(value, etree._Element):
                            if value.text is not None:
                                value = value.text
                            else:
                                value = collapse_spaces(" ".join(value.itertext()))
                        if value:
                            data[prop].append(value)
                except Exception as e:
                    log.error(f"{e} at `{path}`")
    return data


def parse_xml(
    tree: etree._Element, mapping: dict[str, dict[str, str | list]]
) -> dict[str, list[str]]:
    data = defaultdict(dict)
    for key, config in mapping.items():
        prop_mapping = ensure_dict(config.get("properties"))
        root = config.get("from")
        if root is not None:
            data[key] = list()
            for path in ensure_list(root):
                for item in tree.xpath(path):
                    data[key].append(parse_props(item, prop_mapping))
        else:
            data[key] = parse_props(tree, prop_mapping)
    return data
