from collections import defaultdict

from banal import ensure_list


def get_mapped_data(MAPPING: dict[str, str], data: dict) -> dict[str, list[str]]:
    output_data = defaultdict(list)
    for source_key, target_key in MAPPING.items():
        values = ensure_list(data.get(source_key))
        for value in values:
            if value:
                output_data[target_key].append(value)
    return output_data
