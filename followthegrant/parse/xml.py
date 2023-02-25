from collections import defaultdict
from pathlib import Path
from typing import Any

from banal import ensure_dict, ensure_list, is_mapping
from lxml import etree
from normality import collapse_spaces
from zavod.parse.xml import remove_namespace as z_remove_namespace

from ..logging import get_logger

log = get_logger(__name__)


def remove_namespace(tree):
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


def read_xml(path: str | bytes | Path | etree._Element) -> etree._ElementTree | None:
    if isinstance(path, etree._Element):
        return path
    if isinstance(path, Path):
        try:
            # FIXME zavod namespacing seems to be broken here in some cases
            tree = etree.parse(path)
            try:
                return z_remove_namespace(tree)
            except Exception:
                remove_namespace(tree)
                return tree
        except Exception as e:
            log.error(f"Error parsing xml: {e}", fpath=path.name)
    if isinstance(path, (str, bytes)):  # FIXME ?
        try:
            return etree.fromstring(path)
        except Exception as e:
            log.error(f"Error parsing xml: {e}", fpath=path.name)


def parse_props(tree: etree._Element, mapping: dict[str, list]) -> dict[str, list]:
    data = defaultdict(set)
    for prop, paths in mapping.items():
        if is_mapping(paths):
            if "from" in paths:
                subpaths = ensure_list(paths["from"])
                prop_mapping = ensure_dict(paths.get("properties"))
                for subpath in subpaths:
                    for subtree in tree.xpath(subpath):
                        # defaultdict is not hashable so we have to use list here
                        data[prop] = ensure_list(data[prop])
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
                                value = " ".join(value.itertext())
                        if value:
                            data[prop].add(collapse_spaces(value))
                except Exception as e:
                    log.error(f"{e} at `{path}`")
    return data


def parse_xml(
    tree: etree._Element | None, mapping: dict[str, dict[str, str | list]]
) -> dict[str, list[Any] | set[Any]]:
    data = defaultdict(dict)
    if tree is None:
        return data
    for key, config in mapping.items():
        prop_mapping = ensure_dict(config.get("properties"))
        root = config.get("from")
        if root is not None:
            data[key] = []
            for path in ensure_list(root):
                for item in tree.xpath(path):
                    data[key].append(parse_props(item, prop_mapping))
        else:
            data[key] = parse_props(tree, prop_mapping)
    return data
