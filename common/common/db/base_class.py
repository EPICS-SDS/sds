from __future__ import annotations

from typing import List, Optional

import logging
from elasticsearch import BadRequestError, NotFoundError
from pydantic import BaseModel, root_validator

from common.db.connection import get_connection
from common.config import settings


logger = logging.getLogger("sds_common")


class TooManyHitsException(Exception):
    """Raised when the number of hits exceeds the limit"""
    pass


class Base(BaseModel):
    id: Optional[str]

    @root_validator(pre=True)
    def from_hit(cls, values):
        hit = values.pop("hit", None)
        if not hit:
            return values
        values.update(
            id=hit.pop("_id"),
            **hit.pop("_source"),
        )
        return values

    class Config:
        arbitrary_types_allowed = True

    def document(self):
        document = {}
        for key, es_type in self.__annotations__.items():
            value = getattr(self, key)
            document[key] = es_type.to_es(value)
        return document

    async def save(self):
        async with get_connection() as es:
            response = await es.index(
                index=self.get_index(),
                document=self.document(),
            )
            self.id = response["_id"]
            return self

    async def set_expiry(self, date, expiry_index):
        await expiry_index.create({
            "index": self.get_index(),
            "id": self.id,
            "expire_by": date,
        })

    @classmethod
    def mappings(cls):
        properties = {}
        for key, es_type in cls.__annotations__.items():
            properties[key] = {"type": es_type.es_type}
        return {"properties": properties}

    @classmethod
    async def init(cls):
        async with get_connection() as es:
            try:
                await es.indices.create(
                    index=cls.get_index(),
                    mappings=cls.mappings(),
                )
                logger.info(f"ES index '{cls.get_index()}' created")
            except BadRequestError:
                logger.info(f"ES index '{cls.get_index()}' already exists")

    @classmethod
    async def get(cls, id):
        async with get_connection() as es:
            try:
                response = await es.get(
                    index=cls.get_index(),
                    id=id,
                )
                return cls(hit=response)
            except NotFoundError:
                return None

    @classmethod
    async def mget(cls, filters: Optional[List[dict]] = None,
                   query: Optional[dict] = None) -> List[Base]:
        if not query and filters:
            query = {"bool": {"must": list(filters)}}
        async with get_connection() as es:                
            try:
                response = await es.search(
                    index=cls.get_index(),
                    query=query,
                )
            except NotFoundError:
                return []

            n_total = response["hits"]["total"]["value"]

            if n_total == 0:
                return []

            if n_total > settings.max_query_size:
                raise TooManyHitsException()

            return list(map(
                lambda hit: cls(hit=hit),
                response["hits"]["hits"]))

    @ classmethod
    async def create(cls, dict):
        db_obj = cls(**dict)
        await db_obj.save()
        return db_obj

    @ classmethod
    def get_index(cls):
        return cls.Index.name
