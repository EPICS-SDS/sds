from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from common.db import settings
from common.db.connection import get_connection
from elasticsearch import BadRequestError, NotFoundError
from pydantic import BaseModel, root_validator

logger = logging.getLogger("sds_common")


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

    async def save(self):
        async with get_connection() as es:
            response = await es.index(
                index=self.get_index(),
                document=self.dict(by_alias=True, exclude_none=True),
            )
            self.id = response["_id"]
            return self

    async def set_expiry(self, date, expiry_index):
        await expiry_index.create(
            {
                "index": self.get_index(),
                "id": self.id,
                "expire_by": date,
            }
        )

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
                response = await es.search(
                    index=cls.get_index(),
                    query={"ids": {"values": [id]}},
                )
                if response["hits"]["hits"] == []:
                    return None
                return cls(hit=response["hits"]["hits"][0])
            except NotFoundError:
                return None

    @classmethod
    async def mget(
        cls,
        *,
        filters: Optional[List[dict]] = None,
        query: Optional[dict] = None,
        sort: Optional[dict] = None,
        search_after: Optional[int] = None,
    ) -> Tuple[int, List[Base], int]:
        if not query and filters:
            query = {"bool": {"must": list(filters)}}
        async with get_connection() as es:
            try:
                if search_after is not None:
                    search_after = [search_after]

                response = await es.search(
                    index=cls.get_index(),
                    query=query,
                    size=settings.max_query_size,
                    sort=sort,
                    search_after=search_after,
                )

            except NotFoundError:
                return (0, [], None)

            n_total = response["hits"]["total"]["value"]

            # Query got 0 hits, either in total or after paginating with search_after
            if n_total == 0 or response["hits"]["hits"] == []:
                return (n_total, [], None)

            hits = list(map(lambda hit: cls(hit=hit), response["hits"]["hits"]))

            if sort is None:
                search_after = None
            else:
                search_after = response["hits"]["hits"][-1]["sort"][0]

            return (n_total, hits, search_after)

    @classmethod
    async def create(cls, dict):
        db_obj = cls(**dict)
        await db_obj.save()
        return db_obj

    @classmethod
    def get_index(cls):
        return cls.Index.name

    @classmethod
    async def refresh_index(cls):
        async with get_connection() as es:
            return await es.indices.refresh(index=cls.Index.name)
