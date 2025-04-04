import json
from datetime import UTC, datetime

import aiohttp
import pytest
import pytest_asyncio
import requests
from pydantic import ValidationError
from tests.functional.service_loader import INDEXER_PORT, indexer_service

from esds.common import schemas

ELASTIC_URL = "http://elasticsearch:9200"
COLLECTORS_ENDPOINT = "/collectors"
DATASETS_ENDPOINT = "/datasets"

INDEXER_URL = "http://0.0.0.0:" + str(INDEXER_PORT)


pytestmark = pytest.mark.usefixtures("indexer_service")


class TestCollector:
    test_collector = {
        "name": "indexer_test",
        "parent_path": "/",
        "event_code": 1,
        "pvs": ["PV:TEST:1", "PV:TEST:2", "PV:TEST:3"],
    }
    test_collector_bad_schema = {
        "name": "indexer_test_2",
        "parent_path": "/",
        "event_code": 2,
    }

    @classmethod
    def setup_class(cls):
        # Make sure there is no collector that matches the collector that is created in the test_create test
        query = {"query": {"match": {"name": "indexer_test"}}}
        requests.post(ELASTIC_URL + "/collector/_delete_by_query", json=query)
        requests.post(ELASTIC_URL + "/collector/_refresh")

    async def test_create(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector
            ) as response:
                assert response.status == 201

    async def test_create_bad_schema(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector_bad_schema
            ) as response:
                assert response.status == 422

    async def test_get(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector
            ) as response:
                assert response.status == 200

    async def test_validate_schema(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector
            ) as response:
                try:
                    schemas.Collector.model_validate(
                        json.loads(await response.content.read())
                    )
                except ValidationError:
                    assert False
                assert True


class TestDatasets:
    test_dataset = {
        "sds_event_timestamp": datetime.now(UTC).isoformat(),
        "sds_cycle_start_timestamp": datetime.now(UTC).isoformat(),
        "sds_event_cycle_id": 0,
        "data_timestamp": [datetime.now(UTC).isoformat()],
        "data_cycle_id": [0],
        "path": "/directory/file.h5",
        "beam_info": {
            "mode": "TestMode",
            "state": "ON",
            "present": "YES",
            "len": 2.86e-3,
            "energy": 2e9,
            "dest": "Target",
            "curr": 62.5e-3,
        },
    }
    test_dataset_bad_schema = {
        "sds_event_timestamp": "not a timestamp",
        "sds_event_cycle_id": 0,
        "data_timestamp": ["not a timestamp"],
        "data_cycle_id": [0],
        "path": "/directory/file.h5",
    }

    async def test_add_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=TestCollector.test_collector
            ) as response:
                collector = json.loads(await response.content.read())

                self.test_dataset["collector_id"] = collector["collector_id"]

    async def test_create(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + DATASETS_ENDPOINT, json=self.test_dataset
            ) as response:
                assert response.status == 201

    async def test_create_ttl(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + DATASETS_ENDPOINT,
                params={"ttl": 10},
                json=self.test_dataset,
            ) as response:
                assert response.status == 201

    async def test_create_bad_schema(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + DATASETS_ENDPOINT, json=self.test_dataset_bad_schema
            ) as response:
                assert response.status == 422

    async def test_validate_schema(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + DATASETS_ENDPOINT, json=self.test_dataset
            ) as response:
                try:
                    schemas.Dataset.model_validate(
                        json.loads(await response.content.read())
                    )
                except ValidationError:
                    assert False
                assert True
