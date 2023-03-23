from typing import Any, Dict, Generator, Iterable, List


def item_to_filters(key: str, value: Any) -> Generator[Dict, None, None]:
    if isinstance(value, str) or not isinstance(value, Iterable):
        yield {"match": {key: value}}
        return
    terms = list(value)
    yield {
        "terms_set": {
            key: {
                "terms": terms,
                "minimum_should_match_script": {
                    "source": f"Math.min(params.num_terms, {len(terms)})",
                },
            },
        },
    }
    yield {
        "script": {
            "script": {
                "source": "doc[params.field_key].length == params.list_length",
                "lang": "painless",
                "params": {
                    "field_key": key,
                    "list_length": len(terms),
                },
            }
        }
    }


def dict_to_filters(dict: Dict) -> List[Dict]:
    clauses = []
    for k, v in dict.items():
        clauses.extend(item_to_filters(k, v))
    return clauses


class UpdateRequiredException(Exception):
    """
    This exceptions signals the need to update elasticsearch configuration,
    be it because the configuration is missing or needs to be updated.
    """


def check_dict_for_updated_entries(current_dict, new_dict):
    """
    Checks if all keys in the new configuration are present in the current configuration,
    and if their values match.
    The current configuration may have extra keys that are automatically added by
    elasticsearch and are therefore ignored.
    """
    for key in new_dict.keys():
        if key not in current_dict:
            raise UpdateRequiredException
        if type(current_dict[key]) is dict:
            check_dict_for_updated_entries(current_dict[key], new_dict[key])
        elif current_dict[key] != new_dict[key]:
            raise UpdateRequiredException
