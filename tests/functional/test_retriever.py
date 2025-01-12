import os
import zipfile
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import aiofiles
import aiohttp
import pytest
import pytest_asyncio
import requests
from pydantic import ValidationError
from tests.functional.service_loader import (
    INDEXER_PORT,
    RETRIEVER_PORT,
    indexer_service,
    retriever_service,
)

from esds.common import schemas
from esds.common.files import Event, NexusFile
from esds.retriever.config import settings

ELASTIC_URL = "http://elasticsearch:9200"
INDEXER_URL = "http://0.0.0.0:" + str(INDEXER_PORT)
RETRIEVER_URL = "http://0.0.0.0:" + str(RETRIEVER_PORT)

COLLECTORS_ENDPOINT = "/collectors"
DATASETS_ENDPOINT = "/datasets"
NEXUS_ENDPOINT = "/nexus"
SEARCH_ENDPOINT = "/query"

timeout = aiohttp.ClientTimeout(total=10)

test_collector = {
    "name": "retriever_test",
    "parent_path": "/",
    "event_code": 1,
    "pvs": ["PV:TEST:1", "PV:TEST:2", "PV:TEST:3"],
}

test_collector_2 = {
    "name": "retriever_test_2",
    "parent_path": "/",
    "event_code": 2,
    "pvs": ["PV:TEST:3", "PV:TEST:4"],
}


@pytest.mark.usefixtures("indexer_service", "retriever_service")
class TestCollector:
    async def test_add_collectors(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for collector in [
                test_collector,
                test_collector_2,
            ]:
                async with session.post(
                    INDEXER_URL + COLLECTORS_ENDPOINT, json=collector
                ) as response:
                    response_json = await response.json()
                    collector["collector_id"] = response_json["collector_id"]

    async def test_query_existing_collector(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT,
                params={"name": test_collector["name"]},
            ) as response:
                assert response.status == 200
                response_json = await response.json()
                assert response_json["total"] >= 1
                assert (
                    response_json["collectors"][0]["collector_id"]
                    == test_collector["collector_id"]
                )

    async def test_query_non_existing_collector(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT, params={"name": "retriever_test2"}
            ) as response:
                assert response.status == 200
                assert (await response.json())["total"] == 0

    async def test_query_collector_all_filters(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT,
                params={
                    "name": test_collector["name"],
                    "event_code": test_collector["event_code"],
                    "parent_path": test_collector["parent_path"],
                    "pv": ["PV:TEST:1", "PV:TEST:2"],
                },
            ) as response:
                assert response.status == 200
                json_response = await response.json()
                assert json_response["total"] == 1
                assert len(json_response["collectors"]) == 1
                assert (
                    json_response["collectors"][0]["collector_id"]
                    == test_collector["collector_id"]
                )

    async def test_query_collector_pv_filter_overlap(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT,
                params={
                    "name": "retriever_test*",
                    "pv": ["PV:TEST:3"],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["collectors"]) == 2

    async def test_query_collector_pv_filter_wildcard(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT,
                params={
                    "name": "retriever_test*",
                    "pv": ["PV:*:1"],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["collectors"]) == 1

    async def test_get_existing_collector(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL
                + COLLECTORS_ENDPOINT
                + "/"
                + test_collector["collector_id"]
            ) as response:
                assert response.status == 200
                print(await response.json())
                assert (await response.json())["collectors"][0][
                    "collector_id"
                ] == test_collector["collector_id"]

    async def test_get_non_existing_collector(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT + "/wrong_id"
            ) as response:
                assert response.status == 404

    async def test_validate_schema_query_collector(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + COLLECTORS_ENDPOINT, params={"name": "retriever_test"}
            ) as response:
                try:
                    schemas.Collector.model_validate(
                        (await response.json())["collectors"][0]
                    )
                except ValidationError:
                    assert False
                assert True

    async def test_validate_schema_get_collector(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL
                + COLLECTORS_ENDPOINT
                + "/"
                + test_collector["collector_id"]
            ) as response:
                try:
                    schemas.Collector.model_validate(
                        (await response.json())["collectors"][0]
                    )
                except ValidationError:
                    assert False
                assert True


beam_info_dict = {
    "mode": "TestMode",
    "state": "ON",
    "present": "YES",
    "len": 2.86e-3,
    "energy": 2e9,
    "dest": "Target",
    "curr": 62.5e-3,
}

acq_event_dict = {
    "timestamp": datetime.now(UTC).isoformat(),
    "name": "TestEvent",
    "delay": 0.0,
    "code": 0,
    "evr": "TestEVR",
}

# 1 dataset in one file
test_dataset_1 = [
    {
        "sds_event_timestamp": datetime(2022, 1, 1, 0, 0, 0).isoformat(),
        "sds_cycle_start_timestamp": datetime(2022, 1, 1, 0, 0, 0).isoformat(),
        "sds_event_cycle_id": 1,
        "cycle_id_timestamp": datetime(2022, 1, 1, 0, 0, 0).isoformat(),
        "data_timestamp": [datetime(2022, 1, 1, 0, 0, 0).isoformat()],
        "data_cycle_id": [1],
        "acq_event": acq_event_dict,
        "beam_info": beam_info_dict,
    }
]
# several datasets in another file
test_dataset_2 = [
    {
        "sds_event_timestamp": datetime(2022, 1, 1, 0, 0, i).isoformat(),
        "sds_cycle_start_timestamp": datetime(2022, 1, 1, 0, 0, 0).isoformat(),
        "sds_event_cycle_id": i + 1,
        "cycle_id_timestamp": datetime(2022, 1, 1, 0, 0, i).isoformat(),
        "data_timestamp": [datetime(2022, 1, 1, 0, 0, i).isoformat()],
        "data_cycle_id": [i + 1],
        "acq_event": acq_event_dict,
        "beam_info": beam_info_dict,
    }
    for i in range(1, 6)
]


@pytest_asyncio.fixture(loop_scope="class", scope="class")
async def _start_services(indexer_service, retriever_service):
    # Make sure the collector exists
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(
            INDEXER_URL + COLLECTORS_ENDPOINT, json=test_collector
        ) as response:
            collector = await response.json()
            for datasets in [test_dataset_1, test_dataset_2]:
                for dataset in datasets:
                    dataset["collector_id"] = collector["collector_id"]

    # Remove the datasets in case they already exist
    query = {"query": {"match": {"collector_id": collector["collector_id"]}}}
    requests.post(ELASTIC_URL + "/dataset/_delete_by_query", json=query)
    requests.post(ELASTIC_URL + "/dataset/_refresh")

    # Create the NeXus files
    for datasets in [test_dataset_1, test_dataset_2]:
        file_name: str = f'{test_collector["parent_path"].lstrip("/").replace("/","_")}_{test_collector["name"]}_{str(test_collector["event_code"])}_{str(datasets[0]["sds_event_cycle_id"])}'
        # Path is generated from date
        directory = Path(
            datetime.now(UTC).strftime("%Y"),
            datetime.now(UTC).strftime("%Y-%m-%d"),
        )

        nexus = NexusFile(
            collector_id=datasets[0]["collector_id"],
            parent_path=test_collector["parent_path"],
            collector_name=test_collector["name"],
            file_name=file_name,
            directory=settings.storage_path / directory,
        )

        for dataset in datasets:
            for i, pv in enumerate(test_collector["pvs"]):
                new_event = Event(
                    pv_name=pv,
                    value=i,
                    type=None,
                    timing_event_code=test_collector["event_code"],
                    data_timestamp=datetime.now(UTC),
                    sds_event_timestamp=datetime.now(UTC),
                    cycle_id_timestamp=dataset["cycle_id_timestamp"],
                    cycle_id=dataset["sds_event_cycle_id"],
                    sds_event_cycle_id=dataset["sds_event_cycle_id"],
                    attributes=dict(
                        acq_event=acq_event_dict,
                        beam_info=beam_info_dict,
                    ),
                )
                nexus.add_event(new_event)

        nexus.write_from_events()
        for dataset in datasets:
            dataset["path"] = str(directory / f"{nexus.file_name}.h5")

    # Create datasets to test queries
    for datasets in [test_dataset_1, test_dataset_2]:
        for dataset in datasets:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    INDEXER_URL + DATASETS_ENDPOINT, json=dataset
                ) as response:
                    assert response.status == 201
                    new_dataset = await response.json()
                    dataset["dataset_id"] = new_dataset["id"]

    # Make sure the index is refreshed
    requests.post(ELASTIC_URL + "/dataset/_refresh")


@pytest.mark.usefixtures("_start_services")
class TestDatasets:
    async def test_query_existing_dataset_by_collector_id(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={"collector_id": test_dataset_1[0]["collector_id"]},
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 6

    async def test_query_non_existing_dataset_by_collector_id(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={"collector_id": "wrong_id"},
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 0

    async def test_query_existing_dataset_by_start(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": test_dataset_1[0]["collector_id"],
                    "start": test_dataset_2[1]["sds_event_timestamp"],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 4

    async def test_query_existing_dataset_by_end(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": test_dataset_1[0]["collector_id"],
                    "end": test_dataset_1[0]["sds_event_timestamp"],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 1

    async def test_query_no_dataset_by_end(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": test_dataset_1[0]["collector_id"],
                    "end": datetime(2000, 1, 1, 0, 0, 1).isoformat(),
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 0

    async def test_query_existing_dataset_by_sds_event_id_start(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": test_dataset_1[0]["collector_id"],
                    "sds_event_cycle_id_start": test_dataset_2[2]["sds_event_cycle_id"],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 3

    async def test_query_existing_dataset_by_sds_event_id_end(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": test_dataset_1[0]["collector_id"],
                    "sds_event_cycle_id_end": test_dataset_1[0]["sds_event_cycle_id"],
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 1

    async def test_query_no_dataset_by_sds_event_id_end(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": test_dataset_1[0]["collector_id"],
                    "sds_event_cycle_id_end": 0,
                },
            ) as response:
                assert response.status == 200
                assert len((await response.json())["datasets"]) == 0

    async def test_query_latest_dataset_by_collector_id(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": test_dataset_1[0]["collector_id"],
                    "size": 1,
                },
            ) as response:
                assert response.status == 200
                datasets = (await response.json())["datasets"]
                assert len(datasets) == 1
                assert (
                    datasets[0]["sds_event_timestamp"]
                    == test_dataset_2[-1]["sds_event_timestamp"]
                )

    async def test_query_latest_3_datasets_by_collector_id(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": test_dataset_1[0]["collector_id"],
                    "size": 3,
                },
            ) as response:
                assert response.status == 200
                datasets = (await response.json())["datasets"]
                assert len(datasets) == 3
                for i in range(3):
                    assert (
                        datasets[i]["sds_event_timestamp"]
                        == test_dataset_2[-1 - i]["sds_event_timestamp"]
                    )

    async def test_query_first_dataset_by_collector_id(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={
                    "collector_id": test_dataset_1[0]["collector_id"],
                    "size": 1,
                    "sort": "asc",
                },
            ) as response:
                assert response.status == 200
                datasets = (await response.json())["datasets"]
                assert len(datasets) == 1
                assert (
                    datasets[0]["sds_event_timestamp"]
                    == test_dataset_1[0]["sds_event_timestamp"]
                )

    async def test_get_existing_dataset(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL
                + DATASETS_ENDPOINT
                + "/"
                + test_dataset_1[0]["dataset_id"]
            ) as response:
                assert response.status == 200
                assert (await response.json())["id"] == test_dataset_1[0]["dataset_id"]

    async def test_get_non_existing_dataset(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT + "/wrong_id"
            ) as response:
                assert response.status == 404

    async def test_validate_schema_query_dataset(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + DATASETS_ENDPOINT,
                params={"collector_id": test_dataset_1[0]["collector_id"]},
            ) as response:
                try:
                    schemas.Dataset.model_validate(
                        (await response.json())["datasets"][0]
                    )
                except ValidationError:
                    assert False
                assert True

    async def test_validate_schema_get_dataset(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL
                + DATASETS_ENDPOINT
                + "/"
                + test_dataset_1[0]["dataset_id"]
            ) as response:
                try:
                    schemas.Dataset.model_validate(await response.json())
                except ValidationError:
                    assert False
                assert True

        # File endpoints

    async def test_get_existing_file_with_id(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL
                + NEXUS_ENDPOINT
                + "/dataset/"
                + test_dataset_1[0]["dataset_id"]
            ) as response:
                assert response.status == 200
                assert (
                    response.content_disposition.filename
                    == Path(test_dataset_1[0]["path"]).name
                )
                f = await aiofiles.open(
                    settings.storage_path / test_dataset_1[0]["path"], mode="rb"
                )
                assert await f.read() == await response.read()
                await f.close()

    async def test_get_removed_file_with_id(self):
        # Move file to somewhere else
        os.rename(
            settings.storage_path / test_dataset_1[0]["path"],
            settings.storage_path / (test_dataset_1[0]["path"] + ".bak"),
        )
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL
                + NEXUS_ENDPOINT
                + "/dataset/"
                + test_dataset_1[0]["dataset_id"]
            ) as response:
                assert response.status == 404
        # Renaming the file back
        os.rename(
            settings.storage_path / (test_dataset_1[0]["path"] + ".bak"),
            settings.storage_path / test_dataset_1[0]["path"],
        )

    async def test_get_non_existing_file_with_id(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + NEXUS_ENDPOINT + "/dataset/wrong_id"
            ) as response:
                assert response.status == 404

    async def test_get_existing_file_with_path(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + NEXUS_ENDPOINT,
                params={"path": test_dataset_1[0]["path"]},
            ) as response:
                assert response.status == 200
                assert (
                    response.content_disposition.filename
                    == Path(test_dataset_1[0]["path"]).name
                )
                f = await aiofiles.open(
                    settings.storage_path / test_dataset_1[0]["path"], mode="rb"
                )
                assert await f.read() == await response.read()
                await f.close()

    async def test_get_non_existing_file_with_path(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + NEXUS_ENDPOINT, params={"path": "/wrong/path/file.h5"}
            ) as response:
                assert response.status == 404

    async def test_get_existing_file_with_query(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + NEXUS_ENDPOINT + SEARCH_ENDPOINT,
                params={"collector_id": test_dataset_1[0]["collector_id"]},
            ) as response:
                assert response.status == 200
                assert response.content_disposition.filename == "datasets.zip"
                with zipfile.ZipFile(BytesIO(await response.read())) as zip:
                    assert zip.namelist() == [test_collector["name"] + ".h5"]

    async def test_get_non_existing_file_with_query(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                RETRIEVER_URL + NEXUS_ENDPOINT + SEARCH_ENDPOINT,
                params={"collector_id": "wrong_id"},
            ) as response:
                assert response.status == 404

    async def test_get_existing_file_with_dataset_list_1(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            dataset = dict(test_dataset_1[0])
            dataset.pop("dataset_id")
            async with session.post(
                RETRIEVER_URL + NEXUS_ENDPOINT + DATASETS_ENDPOINT,
                json=[dataset],
            ) as response:
                assert response.status == 200
                assert (
                    response.content_disposition.filename
                    == Path(test_dataset_1[0]["path"]).name
                )
                f = await aiofiles.open(
                    settings.storage_path / test_dataset_1[0]["path"], mode="rb"
                )
                assert await f.read() == await response.read()
                await f.close()

    async def test_get_existing_file_with_dataset_list_2(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            datasets = []
            for dataset in test_dataset_2:
                dataset = dict(dataset)
                dataset.pop("dataset_id")
                datasets.append(dataset)
            async with session.post(
                RETRIEVER_URL + NEXUS_ENDPOINT + DATASETS_ENDPOINT,
                json=datasets,
            ) as response:
                assert response.status == 200
                assert (
                    response.content_disposition.filename
                    == Path(test_dataset_2[0]["path"]).name
                )
                f = await aiofiles.open(
                    settings.storage_path / test_dataset_2[0]["path"], mode="rb"
                )
                assert await f.read() == await response.read()
                await f.close()

    async def test_get_zip_file_with_datasets(self):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            datasets = []
            for dataset in test_dataset_1:
                dataset = dict(dataset)
                dataset.pop("dataset_id")
                datasets.append(dataset)
            for dataset in test_dataset_2:
                dataset = dict(dataset)
                dataset.pop("dataset_id")
                datasets.append(dataset)
            async with session.post(
                RETRIEVER_URL + NEXUS_ENDPOINT + DATASETS_ENDPOINT,
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
                    print(test_collector["name"] + ".h5")
                    assert [f.filename for f in zip.filelist].count(
                        test_collector["name"] + ".h5"
                    ) == 1
