import json
from datetime import datetime

import aiohttp
import pytest
import pytest_asyncio
import requests
from common import schemas
from pydantic import ValidationError
from tests.functional.service_loader import (
    INDEXER_PORT,
    RETRIEVER_PORT,
    indexer_service,
    retriever_service,
)

ELASTIC_URL = "http://elasticsearch:9200"
INDEXER_URL = "http://0.0.0.0:" + str(INDEXER_PORT)
RETRIEVER_URL = "http://0.0.0.0:" + str(RETRIEVER_PORT)

COLLECTORS_ENDPOINT = "/collectors"
DATASETS_ENDPOINT = "/datasets"


class TestCollector:
    test_collector = {
        "name": "retriever_test",
        "event_name": "test_event",
        "event_code": 1,
        "pvs": ["PV:TEST:1", "PV:TEST:2", "PV:TEST:3"],
    }

    @pytest_asyncio.fixture(autouse=True)
    async def _start_services(self, indexer_service, retriever_service):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=TestCollector.test_collector
            ) as response:
                collector = json.loads(await response.content.read())
                self.test_collector["collector_id"] = collector["id"]

    @pytest.mark.asyncio
    async def test_query_existing_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT, params={"name": "retriever_test"}
            ) as response:
                assert response.status == 200
                assert (
                    json.loads(await response.content.read())[0]["id"]
                    == self.test_collector["collector_id"]
                )

    @pytest.mark.asyncio
    async def test_query_non_existing_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT, params={"name": "retriever_test2"}
            ) as response:
                assert response.status == 200
                assert json.loads(await response.content.read()) == []

    @pytest.mark.asyncio
    async def test_get_existing_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL
                + COLLECTORS_ENDPOINT
                + "/"
                + self.test_collector["collector_id"]
            ) as response:
                assert response.status == 200
                assert (
                    json.loads(await response.content.read())["id"]
                    == self.test_collector["collector_id"]
                )

    @pytest.mark.asyncio
    async def test_get_non_existing_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT + "/wrong_id"
            ) as response:
                assert response.status == 404

    @pytest.mark.asyncio
    async def test_validate_schema_query_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT, params={"name": "retriever_test"}
            ) as response:
                try:
                    schemas.Collector.parse_obj(
                        json.loads(await response.content.read())[0]
                    )
                except ValidationError:
                    assert False
                assert True

    @pytest.mark.asyncio
    async def test_validate_schema_get_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL
                + COLLECTORS_ENDPOINT
                + "/"
                + self.test_collector["collector_id"]
            ) as response:
                try:
                    schemas.Collector.parse_obj(
                        json.loads(await response.content.read())
                    )
                except ValidationError:
                    assert False
                assert True


class TestDatasets:
    test_dataset_1 = {
        "trigger_date": datetime(2022, 1, 1, 0, 0, 0).isoformat(),
        "trigger_pulse_id": 1,
        "path": "/directory/file1.h5",
    }
    test_dataset_2 = {
        "trigger_date": datetime(2022, 1, 1, 0, 0, 1).isoformat(),
        "trigger_pulse_id": 2,
        "path": "/directory/file2.h5",
    }

    @pytest_asyncio.fixture(autouse=True)
    async def _start_services(self, indexer_service, retriever_service):
        # Make sure the collector exists
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=TestCollector.test_collector
            ) as response:
                collector = json.loads(await response.content.read())
                self.test_dataset_1["collector_id"] = collector["id"]
                self.test_dataset_2["collector_id"] = collector["id"]

        # Remove the datasets in case they already exist
        query = {"query": {"match": {"collector_id": collector["id"]}}}
        requests.post(ELASTIC_URL + "/dataset/_delete_by_query", json=query)
        requests.post(ELASTIC_URL + "/dataset/_refresh")

        # Create a dataset to test queries
        for dataset in [self.test_dataset_1, self.test_dataset_2]:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    INDEXER_URL + DATASETS_ENDPOINT, json=dataset
                ) as response:
                    assert response.status == 201
                    new_dataset = json.loads(await response.content.read())
                    dataset["dataset_id"] = new_dataset["id"]

        # Make sure the index is refreshed
        requests.post(ELASTIC_URL + "/dataset/_refresh")

    @pytest.mark.asyncio
    async def test_query_existing_dataset(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={"collector_id": self.test_dataset_1["collector_id"]},
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 2

    @pytest.mark.asyncio
    async def test_query_non_existing_dataset(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={"collector_id": "wrong_id"},
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 0

    @pytest.mark.asyncio
    async def test_get_existing_dataset(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL
                + DATASETS_ENDPOINT
                + "/"
                + self.test_dataset_1["dataset_id"]
            ) as response:
                assert response.status == 200
                assert (
                    json.loads(await response.content.read())["id"]
                    == self.test_dataset_1["dataset_id"]
                )

    @pytest.mark.asyncio
    async def test_get_non_existing_dataset(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT + "/wrong_id"
            ) as response:
                assert response.status == 404

    @pytest.mark.asyncio
    async def test_validate_schema_query_dataset(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={"collector_id": self.test_dataset_1["collector_id"]},
            ) as response:
                try:
                    schemas.Dataset.parse_obj(
                        json.loads(await response.content.read())[0]
                    )
                except ValidationError:
                    assert False
                assert True

    @pytest.mark.asyncio
    async def test_validate_schema_get_dataset(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL
                + DATASETS_ENDPOINT
                + "/"
                + self.test_dataset_1["dataset_id"]
            ) as response:
                try:
                    schemas.Dataset.parse_obj(json.loads(await response.content.read()))
                except ValidationError:
                    assert False
                assert True
