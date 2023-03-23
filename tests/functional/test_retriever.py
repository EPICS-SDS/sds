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
from pydantic import ValidationError
from esds.common import schemas
from esds.common.files import AcqEvent, AcqInfo, BeamInfo, Event, NexusFile
from esds.retriever.config import settings
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
        "host": "0.0.0.0",
    }

    test_collector_2 = {
        "name": "retriever_test_2",
        "event_name": "test_event_2",
        "event_code": 2,
        "pvs": ["PV:TEST:3", "PV:TEST:4"],
        "host": "0.0.0.0",
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
                    response_json = await response.json()
                    collector["collector_id"] = response_json["id"]

    @pytest.mark.asyncio
    async def test_query_existing_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT,
                params={"name": self.test_collector["name"]},
            ) as response:
                assert response.status == 200
                response_json = await response.json()
                assert response_json["total"] >= 1
                assert (
                    response_json["collectors"][0]["id"]
                    == self.test_collector["collector_id"]
                )

    @pytest.mark.asyncio
    async def test_query_non_existing_collector(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT, params={"name": "retriever_test2"}
            ) as response:
                assert response.status == 200
                assert (await response.json())["total"] == 0

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
                json_response = await response.json()
                assert json_response["total"] == 1
                assert len(json_response["collectors"]) == 1
                assert (
                    json_response["collectors"][0]["id"]
                    == self.test_collector["collector_id"]
                )

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
                assert len((await response.json())["collectors"]) == 2

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
                assert len((await response.json())["collectors"]) == 1

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
                assert (await response.json())["id"] == self.test_collector[
                    "collector_id"
                ]

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
                        (await response.json())["collectors"][0]
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
                    schemas.Collector.parse_obj(await response.json())
                except ValidationError:
                    assert False
                assert True


class TestDatasets:
    beam_info_dict = {
        "mode": "TestMode",
        "state": "ON",
        "present": "YES",
        "len": 2.86e-3,
        "energy": 2e9,
        "dest": "Target",
        "curr": 62.5e-3,
    }

    acq_info_dict = {
        "acq_type": "",
        "id": 0,
    }

    acq_event_dict = {
        "timestamp": datetime.utcnow().isoformat(),
        "name": "TestEvent",
        "delay": 0.0,
        "code": 0,
        "evr": "TestEVR",
    }

    test_dataset_1 = [
        {
            "sds_event_timestamp": datetime(2022, 1, 1, 0, 0, 0).isoformat(),
            "sds_event_pulse_id": 1,
            "data_timestamp": [datetime(2022, 1, 1, 0, 0, 0).isoformat()],
            "data_pulse_id": [1],
            "acq_info": acq_info_dict,
            "acq_event": acq_event_dict,
            "beam_info": beam_info_dict,
        }
    ]
    test_dataset_2 = [
        {
            "sds_event_timestamp": datetime(2022, 1, 1, 0, 0, 1).isoformat(),
            "sds_event_pulse_id": 2,
            "data_timestamp": [datetime(2022, 1, 1, 0, 0, 1).isoformat()],
            "data_pulse_id": [2],
            "acq_info": acq_info_dict,
            "acq_event": acq_event_dict,
            "beam_info": beam_info_dict,
        },
        {
            "sds_event_timestamp": datetime(2022, 1, 1, 0, 0, 2).isoformat(),
            "sds_event_pulse_id": 3,
            "data_timestamp": [datetime(2022, 1, 1, 0, 0, 2).isoformat()],
            "data_pulse_id": [3],
            "acq_info": acq_info_dict,
            "acq_event": acq_event_dict,
            "beam_info": beam_info_dict,
        },
    ]

    @pytest_asyncio.fixture(autouse=True)
    async def _start_services(self, indexer_service, retriever_service):
        # Make sure the collector exists
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INDEXER_URL + COLLECTORS_ENDPOINT, json=TestCollector.test_collector
            ) as response:
                collector = await response.json()
                self.test_dataset_1[0]["collector_id"] = collector["id"]
                self.test_dataset_2[0]["collector_id"] = collector["id"]
                self.test_dataset_2[1]["collector_id"] = collector["id"]

        # Remove the datasets in case they already exist
        query = {"query": {"match": {"collector_id": collector["id"]}}}
        requests.post(ELASTIC_URL + "/dataset/_delete_by_query", json=query)
        requests.post(ELASTIC_URL + "/dataset/_refresh")

        # Create the NeXus files
        for datasets in [self.test_dataset_1, self.test_dataset_2]:
            file_name: str = f'{TestCollector.test_collector["name"]}_{str(TestCollector.test_collector["event_code"])}_{str(datasets[0]["sds_event_pulse_id"])}'
            # Path is generated from date
            directory = Path(
                datetime.utcnow().strftime("%Y"),
                datetime.utcnow().strftime("%Y-%m-%d"),
            )

            nexus = NexusFile(
                collector_id=datasets[0]["collector_id"],
                collector_name=TestCollector.test_collector["name"],
                file_name=file_name,
                directory=settings.storage_path / directory,
            )

            acq_info = AcqInfo(**self.acq_info_dict)
            acq_event = AcqEvent(**self.acq_event_dict)
            beam_info = BeamInfo(**self.beam_info_dict)
            for dataset in datasets:
                for i, pv in enumerate(TestCollector.test_collector["pvs"]):
                    new_event = Event(
                        pv_name=pv,
                        value=i,
                        timing_event_code=TestCollector.test_collector["event_code"],
                        data_timestamp=datetime.utcnow(),
                        sds_event_timestamp=datetime.utcnow(),
                        pulse_id=dataset["sds_event_pulse_id"],
                        sds_event_pulse_id=dataset["sds_event_pulse_id"],
                        acq_info=acq_info,
                        acq_event=acq_event,
                        beam_info=beam_info,
                    )
                    nexus.add_event(new_event)

            nexus.write_from_events()
            for dataset in datasets:
                dataset["path"] = str(directory / f"{nexus.file_name}.h5")

        # Create datasets to test queries
        for datasets in [self.test_dataset_1, self.test_dataset_2]:
            for dataset in datasets:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        INDEXER_URL + DATASETS_ENDPOINT, json=dataset
                    ) as response:
                        assert response.status == 201
                        new_dataset = await response.json()
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
                assert len((await response.json())["datasets"]) == 3

    @pytest.mark.asyncio
    async def test_query_non_existing_dataset_by_collector_id(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={"collector_id": "wrong_id"},
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 0

    @pytest.mark.asyncio
    async def test_query_existing_dataset_by_start(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "start": self.test_dataset_1[0]["sds_event_timestamp"],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 3

    @pytest.mark.asyncio
    async def test_query_existing_dataset_by_end(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "end": self.test_dataset_1[0]["sds_event_timestamp"],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 1

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
                assert len((await response.json())["datasets"]) == 0

    @pytest.mark.asyncio
    async def test_query_existing_dataset_by_sds_event_id_start(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "sds_event_pulse_id_start": self.test_dataset_1[0][
                        "sds_event_pulse_id"
                    ],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 3

    @pytest.mark.asyncio
    async def test_query_existing_dataset_by_sds_event_id_end(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "sds_event_pulse_id_end": self.test_dataset_1[0][
                        "sds_event_pulse_id"
                    ],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 1

    @pytest.mark.asyncio
    async def test_query_no_dataset_by_sds_event_id_end(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": self.test_dataset_1[0]["collector_id"],
                    "sds_event_pulse_id_end": 0,
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 0

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
                assert (await response.json())["id"] == self.test_dataset_1[0][
                    "dataset_id"
                ]

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
                    schemas.Dataset.parse_obj((await response.json())["datasets"][0])
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
                    schemas.Dataset.parse_obj(await response.json())
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

    @pytest.mark.asyncio
    async def test_get_zip_file_with_datasets(self):
        async with aiohttp.ClientSession() as session:
            datasets = []
            for dataset in self.test_dataset_1:
                dataset = dict(dataset)
                dataset.pop("dataset_id")
                datasets.append(dataset)
            for dataset in self.test_dataset_2:
                dataset = dict(dataset)
                dataset.pop("dataset_id")
                datasets.append(dataset)
            async with session.post(
                RETRIEVER_URL + FILES_ENDPOINT + "/compile",
                json=datasets,
            ) as response:
                assert response.status == 200
                assert response.content_disposition.filename == "datasets.zip"
                zip_io = BytesIO()
                zip_io.write(await response.read())
                with zipfile.ZipFile(
                    zip_io, mode="r", compression=zipfile.ZIP_DEFLATED
                ) as zip:
                    print(zip.filelist)
                    print(TestCollector.test_collector["name"] + ".h5")
                    assert [f.filename for f in zip.filelist].count(
                        TestCollector.test_collector["name"] + ".h5"
                    ) == 1
