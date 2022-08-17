import json
import os
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path

import aiofiles
import aiohttp
import pytest
import pytest_asyncio
import requests
from common import schemas
from common.files.dataset import Dataset
from common.files.event import Event
from pydantic import ValidationError
from retriever.config import settings
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
FILES_ENDPOINT = "/files"


class TestCollector:
    test_collector = {
        "name": "retriever_test",
        "event_name": "test_event",
        "event_code": 1,
        "pvs": ["PV:TEST:1", "PV:TEST:2", "PV:TEST:3"],
    }

    test_collector_2 = {
        "name": "retriever_test_2",
        "event_name": "test_event_2",
        "event_code": 2,
        "pvs": ["PV:TEST:3", "PV:TEST:4"],
    }

    @pytest_asyncio.fixture(autouse=True)
    async def _start_services(self, indexer_service, retriever_service):
        async with aiohttp.ClientSession() as session:
            for collector in [
                TestCollector.test_collector,
                TestCollector.test_collector_2,
            ]:
                async with session.post(
                    INDEXER_URL + COLLECTORS_ENDPOINT, json=collector
                ) as response:
                    response_json = json.loads(await response.content.read())
                    collector["collector_id"] = response_json["id"]

    @pytest.mark.asyncio
    async def test_query_existing_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT,
                params={"name": self.test_collector["name"]},
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
    async def test_query_collector_all_filters(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT,
                params={
                    "name": self.test_collector["name"],
                    "event_name": self.test_collector["event_name"],
                    "event_code": self.test_collector["event_code"],
                    "pv": ["PV:TEST:1", "PV:TEST:2"],
                },
            ) as response:
                assert response.status == 200
                json_response = json.loads(await response.content.read())
                assert len(json_response) == 1
                assert json_response[0]["id"] == self.test_collector["collector_id"]

    @pytest.mark.asyncio
    async def test_query_collector_pv_filter_overlap(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT,
                params={
                    "name": "retriever_test*",
                    "pv": ["PV:TEST:3"],
                },
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 2

    @pytest.mark.asyncio
    async def test_query_collector_pv_filter_wildcard(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT,
                params={
                    "name": "retriever_test*",
                    "pv": ["PV:*:1"],
                },
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 1

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
    test_dataset_1 = [
        {
            "trigger_date": datetime(2022, 1, 1, 0, 0, 0).isoformat(),
            "trigger_pulse_id": 1,
        }
    ]
    test_dataset_2 = [
        {
            "trigger_date": datetime(2022, 1, 1, 0, 0, 1).isoformat(),
            "trigger_pulse_id": 2,
        },
        {
            "trigger_date": datetime(2022, 1, 1, 0, 0, 2).isoformat(),
            "trigger_pulse_id": 3,
        },
    ]

    @pytest_asyncio.fixture(autouse=True)
    async def _start_services(self, indexer_service, retriever_service):
        # Make sure the collector exists
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=TestCollector.test_collector
            ) as response:
                collector = json.loads(await response.content.read())
                self.test_dataset_1[0]["collector_id"] = collector["id"]
                self.test_dataset_2[0]["collector_id"] = collector["id"]
                self.test_dataset_2[1]["collector_id"] = collector["id"]

        # Remove the datasets in case they already exist
        query = {"query": {"match": {"collector_id": collector["id"]}}}
        requests.post(ELASTIC_URL + "/dataset/_delete_by_query", json=query)
        requests.post(ELASTIC_URL + "/dataset/_refresh")

        # Create the NeXus files
        for datasets in [self.test_dataset_1, self.test_dataset_2]:
            dataset_nexus = Dataset(
                collector_id=datasets[0]["collector_id"],
                collector_name=TestCollector.test_collector["name"],
                trigger_date=datetime.utcnow(),
                trigger_pulse_id=datasets[0]["trigger_pulse_id"],
                event_name=TestCollector.test_collector["event_name"],
                event_code=TestCollector.test_collector["event_code"],
            )
            for dataset in datasets:
                for i, pv in enumerate(TestCollector.test_collector["pvs"]):
                    new_event = Event(
                        pv_name=pv,
                        value=i,
                        timming_event_name=TestCollector.test_collector["event_name"],
                        timming_event_code=TestCollector.test_collector["event_code"],
                        data_date=datetime.utcnow(),
                        trigger_date=datetime.utcnow(),
                        pulse_id=dataset["trigger_pulse_id"],
                        trigger_pulse_id=dataset["trigger_pulse_id"],
                    )
                    dataset_nexus.update(new_event)

            await dataset_nexus.write()
            for dataset in datasets:
                dataset["path"] = str(dataset_nexus.path)

        # Create datasets to test queries
        for datasets in [self.test_dataset_1, self.test_dataset_2]:
            for dataset in datasets:
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
    async def test_query_existing_dataset_by_collector_id(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={"collector_id": self.test_dataset_1[0]["collector_id"]},
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 3

    @pytest.mark.asyncio
    async def test_query_non_existing_dataset_by_collector_id(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={"collector_id": "wrong_id"},
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 0

    @pytest.mark.asyncio
    async def test_query_existing_dataset_by_start(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "start": self.test_dataset_1[0]["trigger_date"],
                },
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 3

    @pytest.mark.asyncio
    async def test_query_existing_dataset_by_end(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "end": self.test_dataset_1[0]["trigger_date"],
                },
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 1

    @pytest.mark.asyncio
    async def test_query_no_dataset_by_end(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "end": datetime(2000, 1, 1, 0, 0, 1).isoformat(),
                },
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 0

    @pytest.mark.asyncio
    async def test_query_existing_dataset_by_trigger_id_start(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "trigger_pulse_id_start": self.test_dataset_1[0][
                        "trigger_pulse_id"
                    ],
                },
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 3

    @pytest.mark.asyncio
    async def test_query_existing_dataset_by_trigger_id_end(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "trigger_pulse_id_end": self.test_dataset_1[0]["trigger_pulse_id"],
                },
            ) as response:
                assert response.status == 200
                assert len(json.loads(await response.content.read())) == 1

    @pytest.mark.asyncio
    async def test_query_no_dataset_by_trigger_id_end(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "trigger_pulse_id_end": 0,
                },
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
                + self.test_dataset_1[0]["dataset_id"]
            ) as response:
                assert response.status == 200
                assert (
                    json.loads(await response.content.read())["id"]
                    == self.test_dataset_1[0]["dataset_id"]
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
                params={"collector_id": self.test_dataset_1[0]["collector_id"]},
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
                + self.test_dataset_1[0]["dataset_id"]
            ) as response:
                try:
                    schemas.Dataset.parse_obj(json.loads(await response.content.read()))
                except ValidationError:
                    assert False
                assert True

        # File endpoints

    @pytest.mark.asyncio
    async def test_get_existing_file_with_id(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL
                + FILES_ENDPOINT
                + "/dataset/"
                + self.test_dataset_1[0]["dataset_id"]
            ) as response:
                assert response.status == 200
                assert (
                    response.content_disposition.filename
                    == Path(self.test_dataset_1[0]["path"]).name
                )
                f = await aiofiles.open(
                    settings.storage_path / self.test_dataset_1[0]["path"], mode="rb"
                )
                assert await f.read() == await response.read()
                await f.close()

    @pytest.mark.asyncio
    async def test_get_removed_file_with_id(self):
        # Move file to somewhere else
        os.rename(
            settings.storage_path / self.test_dataset_1[0]["path"],
            settings.storage_path / (self.test_dataset_1[0]["path"] + ".bak"),
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL
                + FILES_ENDPOINT
                + "/dataset/"
                + self.test_dataset_1[0]["dataset_id"]
            ) as response:
                assert response.status == 404
        # Renaming the file back
        os.rename(
            settings.storage_path / (self.test_dataset_1[0]["path"] + ".bak"),
            settings.storage_path / self.test_dataset_1[0]["path"],
        )

    @pytest.mark.asyncio
    async def test_get_non_existing_file_with_id(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + FILES_ENDPOINT + "/dataset/wrong_id"
            ) as response:
                assert response.status == 404

    @pytest.mark.asyncio
    async def test_get_existing_file_with_path(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + FILES_ENDPOINT,
                params={"path": self.test_dataset_1[0]["path"]},
            ) as response:
                assert response.status == 200
                assert (
                    response.content_disposition.filename
                    == Path(self.test_dataset_1[0]["path"]).name
                )
                f = await aiofiles.open(
                    settings.storage_path / self.test_dataset_1[0]["path"], mode="rb"
                )
                assert await f.read() == await response.read()
                await f.close()

    @pytest.mark.asyncio
    async def test_get_non_existing_file_with_path(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + FILES_ENDPOINT, params={"path": "/wrong/path/file.h5"}
            ) as response:
                assert response.status == 404

    @pytest.mark.asyncio
    async def test_get_existing_file_with_query(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + FILES_ENDPOINT + DATASETS_ENDPOINT,
                params={"collector_id": self.test_dataset_1[0]["collector_id"]},
            ) as response:
                assert response.status == 200
                assert response.content_disposition.filename == "datasets.zip"
                with zipfile.ZipFile(BytesIO(await response.read())) as zip:
                    assert zip.namelist() == [
                        TestCollector.test_collector["name"] + ".h5"
                    ]

    @pytest.mark.asyncio
    async def test_get_non_existing_file_with_query(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + FILES_ENDPOINT + DATASETS_ENDPOINT,
                params={"collector_id": "wrong_id"},
            ) as response:
                assert response.status == 404

    @pytest.mark.asyncio
    async def test_get_existing_file_with_dataset_list_1(self):
        async with aiohttp.ClientSession() as session:
            dataset = dict(self.test_dataset_1[0])
            dataset.pop("dataset_id")
            async with session.post(
                RETRIEVER_URL + FILES_ENDPOINT + "/compile",
                json=[dataset],
            ) as response:
                assert response.status == 200
                assert (
                    response.content_disposition.filename
                    == Path(self.test_dataset_1[0]["path"]).name
                )
                f = await aiofiles.open(
                    settings.storage_path / self.test_dataset_1[0]["path"], mode="rb"
                )
                assert await f.read() == await response.read()
                await f.close()

    @pytest.mark.asyncio
    async def test_get_existing_file_with_dataset_list_2(self):
        async with aiohttp.ClientSession() as session:
            datasets = []
            for dataset in self.test_dataset_2:
                dataset = dict(dataset)
                dataset.pop("dataset_id")
                datasets.append(dataset)
            async with session.post(
                RETRIEVER_URL + FILES_ENDPOINT + "/compile",
                json=datasets,
            ) as response:
                assert response.status == 200
                assert (
                    response.content_disposition.filename
                    == Path(self.test_dataset_2[0]["path"]).name
                )
                f = await aiofiles.open(
                    settings.storage_path / self.test_dataset_2[0]["path"], mode="rb"
                )
                assert await f.read() == await response.read()
                await f.close()
