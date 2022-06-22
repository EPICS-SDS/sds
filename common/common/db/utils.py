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
                "inline": "doc[params.field_key].length == params.list_length",
                "lang": "painless",
                "params": {
                    "field_key": key,
                    "list_length": len(terms),
                }
            }
        }
    }


def dict_to_filters(dict: Dict) -> List[Dict]:
    clauses = []
    for k, v in dict.items():
        clauses.extend(item_to_filters(k, v))
    return clauses
