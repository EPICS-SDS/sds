import requests
import json
from datetime import datetime

INDEXER_URL = "http://sds_indexer:8000"
RETRIEVER_URL = "http://sds_retriever:8001"
ELASTIC_URL = "http://elasticsearch:9200"

COLLECTORS_ENDPOINT = "/collectors"
DATASETS_ENDPOINT = "/datasets"


class TestCollector:
    test_collector = {
        "name": "retriever_test",
        "event_name": "test_event",
        "event_code": 1,
        "pvs": ["PV:TEST:1", "PV:TEST:2", "PV:TEST:3"],
    }

    @classmethod
    def setup_class(cls):
        """
        Setups the database
        """
        response = requests.post(
            INDEXER_URL + COLLECTORS_ENDPOINT, json=TestCollector.test_collector
        )
        collector = json.loads(response.content)
        cls.test_collector["collector_id"] = collector["id"]

    def test_query_existing_collector(self):
        response = requests.get(
            RETRIEVER_URL + COLLECTORS_ENDPOINT, params={"name": "retriever_test"}
        )
        assert response.status_code == 200
        assert (
            json.loads(response.content)[0]["id"] == self.test_collector["collector_id"]
        )

    def test_query_non_existing_collector(self):
        response = requests.get(
            RETRIEVER_URL + COLLECTORS_ENDPOINT, params={"name": "retriever_test2"}
        )
        assert response.status_code == 200
        assert json.loads(response.content) == []

    def test_get_existing_collector(self):
        response = requests.get(
            RETRIEVER_URL + "/collectors/" + self.test_collector["collector_id"]
        )
        assert response.status_code == 200
        assert json.loads(response.content)["id"] == self.test_collector["collector_id"]

    def test_get_non_existing_collector(self):
        response = requests.get(RETRIEVER_URL + "/collectors/wrong_id")
        assert response.status_code == 404


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

    @classmethod
    def setup_class(cls):
        # Make sure the collector exists
        response = requests.post(
            INDEXER_URL + COLLECTORS_ENDPOINT, json=TestCollector.test_collector
        )
        collector = json.loads(response.content)
        cls.test_dataset_1["collector_id"] = collector["id"]
        cls.test_dataset_2["collector_id"] = collector["id"]

        # Remove the datasets in case they already exist
        query = {"query": {"match": {"collector_id": collector["id"]}}}
        requests.post(ELASTIC_URL + "/dataset/_delete_by_query", json=query)
        requests.post(ELASTIC_URL + "/dataset/_refresh")

        # Create a dataset to test queries
        for dataset in [cls.test_dataset_1, cls.test_dataset_2]:
            response = requests.post(INDEXER_URL + DATASETS_ENDPOINT, json=dataset)
            assert response.status_code == 201
            new_dataset = json.loads(response.content)
            dataset["dataset_id"] = new_dataset["id"]

        # Make sure the index is refreshed
        requests.post(ELASTIC_URL + "/dataset/_refresh")

    def test_query_existing_dataset(self):
        response = requests.get(
            RETRIEVER_URL + DATASETS_ENDPOINT,
            params={"collector_id": self.test_dataset_1["collector_id"]},
        )
        assert response.status_code == 200
        assert len(json.loads(response.content)) == 2

    def test_query_non_existing_dataset(self):
        response = requests.get(
            RETRIEVER_URL + DATASETS_ENDPOINT,
            params={"collector_id": "wrong_id"},
        )
        assert response.status_code == 200
        assert len(json.loads(response.content)) == 0

    def test_get_existing_dataset(self):
        response = requests.get(
            RETRIEVER_URL + "/datasets/" + self.test_dataset_1["dataset_id"]
        )
        assert response.status_code == 200
        assert json.loads(response.content)["id"] == self.test_dataset_1["dataset_id"]

    def test_get_non_existing_dataset(self):
        response = requests.get(RETRIEVER_URL + "/datasets/wrong_id")
        assert response.status_code == 404
