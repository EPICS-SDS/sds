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
        if not isinstance(v, str) and isinstance(v, Iterable):
            return list(map(parse_datetime, v))
        return parse_datetime(v)


class Keyword(ESField):
    es_type = "keyword"

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str) and isinstance(v, Iterable):
            return list(map(validators.str_validator, v))
        return validators.str_validator(str(v))


class Integer(ESField):
    es_type = "integer"

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str) and isinstance(v, Iterable):
            return list(map(validators.int_validator, v))
        return validators.int_validator(v)


class Long(ESField):
    es_type = "long"

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str) and isinstance(v, Iterable):
            return list(map(validators.int_validator, v))
        return validators.int_validator(v)


class Double(ESField):
    es_type = "double"

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str) and isinstance(v, Iterable):
            return list(map(validators.float_validator, v))
        return validators.float_validator(v)


class Text(ESField):
    es_type = "text"

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str) and isinstance(v, Iterable):
            return list(map(validators.str_validator, v))
        return validators.str_validator(str(v))
