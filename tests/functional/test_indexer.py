import json
from datetime import datetime

import aiohttp
import pytest
import pytest_asyncio
import requests
from common import schemas
from pydantic import ValidationError
from tests.functional.service_loader import INDEXER_PORT, indexer_service

ELASTIC_URL = "http://elasticsearch:9200"
COLLECTORS_ENDPOINT = "/collectors"
DATASETS_ENDPOINT = "/datasets"

INDEXER_URL = "http://0.0.0.0:" + str(INDEXER_PORT)


class TestCollector:
    test_collector = {
        "name": "indexer_test",
        "event_name": "test_event",
        "event_code": 1,
        "pvs": ["PV:TEST:1", "PV:TEST:2", "PV:TEST:3"],
    }
    test_collector_bad_schema = {
        "name": "indexer_test_2",
        "event_name": "test_event_2",
        "event_code": 2,
    }

    @pytest.fixture(autouse=True)
    def _start_indexer_service(self, indexer_service):
        pass

    @classmethod
    def setup_class(cls):
        # Make sure there is no collector that matches the collector that is created in the test_create test
        query = {"query": {"match": {"name": "indexer_test"}}}
        requests.post(ELASTIC_URL + "/collector/_delete_by_query", json=query)
        requests.post(ELASTIC_URL + "/collector/_refresh")

    @pytest.mark.asyncio
    async def test_create(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector
            ) as response:
                assert response.status == 201

    @pytest.mark.asyncio
    async def test_create_bad_schema(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector_bad_schema
            ) as response:
                assert response.status == 422

    @pytest.mark.asyncio
    async def test_get(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector
            ) as response:
                assert response.status == 200

    @pytest.mark.asyncio
    async def test_validate_schema(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector
            ) as response:
                try:
                    schemas.Collector.parse_obj(
                        json.loads(await response.content.read())
                    )
                except ValidationError:
                    assert False
                assert True


class TestDatasets:
    test_dataset = {
        "trigger_date": datetime.utcnow().isoformat(),
        "trigger_pulse_id": 0,
        "data_date": [datetime.utcnow().isoformat()],
        "data_pulse_id": [0],
        "path": "/directory/file.h5",
    }
    test_dataset_bad_schema = {
        "trigger_date": "not a timestamp",
        "trigger_pulse_id": 0,
        "data_date": ["not a timestamp"],
        "data_pulse_id": [0],
        "path": "/directory/file.h5",
    }

    @pytest_asyncio.fixture(autouse=True)
    async def _start_indexer_service(self, indexer_service):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=TestCollector.test_collector
            ) as response:
                collector = json.loads(await response.content.read())
                self.test_dataset["collector_id"] = collector["id"]

    @pytest.mark.asyncio
    async def test_create(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + DATASETS_ENDPOINT, json=self.test_dataset
            ) as response:
                assert response.status == 201

    @pytest.mark.asyncio
    async def test_create_ttl(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + DATASETS_ENDPOINT,
                params={"ttl": 10},
                json=self.test_dataset,
            ) as response:
                assert response.status == 201

    @pytest.mark.asyncio
    async def test_create_bad_schema(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + DATASETS_ENDPOINT, json=self.test_dataset_bad_schema
            ) as response:
                assert response.status == 422

    @pytest.mark.asyncio
    async def test_validate_schema(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + DATASETS_ENDPOINT, json=self.test_dataset
            ) as response:
                try:
                    schemas.Dataset.parse_obj(json.loads(await response.content.read()))
                except ValidationError:
                    assert False
                assert True
