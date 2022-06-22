from typing import Iterable

from pydantic import validators
from pydantic.datetime_parse import parse_datetime


class ESField:
    es_type: str

    @classmethod
    def to_es(cls, value):
        if not isinstance(value, str) and isinstance(value, Iterable):
            return list(value)
        return value

    @classmethod
    def __get_validators__(cls):
        yield cls.validate


class Date(ESField):
    es_type = "date"

    @classmethod
    def validate(cls, v):
        return parse_datetime(v)


class Keyword(ESField):
    es_type = "keyword"

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            try:
                iter(v)
                return list(map(validators.str_validator, v))
            except TypeError:
                raise TypeError("string or list of strings required")
        return validators.str_validator(v)


class Integer(ESField):
    es_type = "integer"

    @classmethod
    def validate(cls, v):
        return validators.int_validator(v)


class Text(ESField):
    es_type = "text"

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            try:
                iter(v)
                return list(map(validators.str_validator, v))
            except TypeError:
                raise TypeError("string or list of strings required")
        return validators.str_validator(v)
