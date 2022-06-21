from typing import Any, Dict


def item_to_filters(key: str, value: Any):
    if not isinstance(value, (list, set)):
        yield {"match": {key: value}}
        return
    for sub_value in value:
        yield {"match": {key: sub_value}}


def dict_to_filters(dict: Dict):
    clauses = []
    for k, v in dict.items():
        clauses.extend(item_to_filters(k, v))
    return clauses
