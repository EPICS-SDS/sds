from typing import Any, ClassVar, Iterable, List, Union

from pydantic_core import CoreSchema, core_schema
from pydantic import GetCoreSchemaHandler, TypeAdapter
from datetime import datetime


class ESField:
    @classmethod
    def to_es(cls, value):
        if not isinstance(value, str) and isinstance(value, Iterable):
            return list(value)
        return value

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.with_info_before_validator_function(
            function=cls.validate, schema=cls.schema
        )


class Date(ESField):
    schema: ClassVar[CoreSchema] = core_schema.union_schema(
        [
            core_schema.datetime_schema(),
            core_schema.list_schema(core_schema.datetime_schema()),
        ]
    )
    es_type: ClassVar[str] = "date"

    @classmethod
    def validate(self, data: Any, self_instance) -> Any:
        parse_datetime = TypeAdapter(datetime).validate_python

        if not isinstance(data, str) and isinstance(data, Iterable):
            return list(map(parse_datetime, data))
        return parse_datetime(data)


class Keyword(ESField):
    schema: ClassVar[CoreSchema] = core_schema.union_schema(
        [core_schema.str_schema(), core_schema.list_schema(core_schema.str_schema())]
    )
    es_type: ClassVar[str] = "keyword"

    @classmethod
    def validate(self, data: Any, self_instance) -> Any:
        parse_str = TypeAdapter(str).validate_python
        if not isinstance(data, str) and isinstance(data, Iterable):
            return list(map(parse_str, data))
        return parse_str(str(data))


class Integer(ESField):
    schema: ClassVar[CoreSchema] = core_schema.union_schema(
        [core_schema.int_schema(), core_schema.list_schema(core_schema.int_schema())]
    )
    es_type: ClassVar[str] = "integer"

    @classmethod
    def validate(self, data: Any, self_instance) -> Any:
        return data


class Long(ESField):
    schema: ClassVar[CoreSchema] = core_schema.union_schema(
        [core_schema.int_schema(), core_schema.list_schema(core_schema.int_schema())]
    )
    es_type: ClassVar[str] = "long"

    @classmethod
    def validate(self, data: Any, self_instance) -> Any:
        return data


class Double(ESField):
    schema: ClassVar[CoreSchema] = core_schema.union_schema(
        [
            core_schema.float_schema(),
            core_schema.list_schema(core_schema.float_schema()),
        ]
    )
    es_type: ClassVar[str] = "double"

    @classmethod
    def validate(self, data: Any, self_instance) -> Any:
        return data


class Text(ESField):
    schema: ClassVar[CoreSchema] = core_schema.union_schema(
        [core_schema.str_schema(), core_schema.list_schema(core_schema.str_schema())]
    )
    es_type: ClassVar[str] = "text"

    @classmethod
    def validate(self, data: Any, self_instance) -> Any:
        return data


class Json(ESField):
    schema: ClassVar[CoreSchema] = core_schema.dict_schema()
    es_type: ClassVar[str] = "object"

    @classmethod
    def validate(self, data: Any, self_instance) -> Any:
        return data
