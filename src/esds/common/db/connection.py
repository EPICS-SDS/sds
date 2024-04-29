import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from functools import wraps

from elasticsearch import AsyncElasticsearch

from esds.common.db import settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_connection():
    if settings.elastic_password is not None:
        connection = AsyncElasticsearch(
            str(settings.elastic_url),
            basic_auth=("elastic", settings.elastic_password),
            verify_certs=False,
        )
    else:
        connection = AsyncElasticsearch(str(settings.elastic_url))
    try:
        yield connection
    finally:
        await connection.close()


async def wait_for_connection(timeout: int = settings.retry_connection):
    # Silencing Warnings from transport loggers
    transport_logger = logging.getLogger("elastic_transport")
    transport_logger_level = transport_logger.level
    transport_logger.setLevel("ERROR")

    async with get_connection() as connection:
        trials = 0
        while not await connection.ping():
            if trials > timeout:
                logger.error("Could not connect to the ElasticSearch server!")
                sys.exit(1)

            trials += 1
            await asyncio.sleep(1)

    transport_logger.setLevel(transport_logger_level)


def with_connection(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with get_connection() as connection:
            return await func(connection, *args, **kwargs)

    return wrapper
