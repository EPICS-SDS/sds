import requests
import json
from datetime import datetime

from pydantic import ValidationError
from common import schemas

INDEXER_URL = "http://sds_indexer:8000"
ELASTIC_URL = "http://elasticsearch:9200"
COLLECTORS_ENDPOINT = "/collectors"
DATASETS_ENDPOINT = "/datasets"


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

    @classmethod
    def setup_class(cls):
        # Make sure there is no collector that matches the collector that is created in the test_create test
        query = {"query": {"match": {"name": "indexer_test"}}}
        requests.post(ELASTIC_URL + "/collector/_delete_by_query", json=query)
        requests.post(ELASTIC_URL + "/collector/_refresh")

    def test_create(self):
        response = requests.post(
            INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector
        )
        assert response.status_code == 201

    def test_create_bad_schema(self):
        response = requests.post(
            INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector_bad_schema
        )
        assert response.status_code == 422

    def test_get(self):
        response = requests.post(
            INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector
        )
        assert response.status_code == 200

    def test_validate_schema(self):
        response = requests.post(
            INDEXER_URL + COLLECTORS_ENDPOINT, json=self.test_collector
        )
        try:
            schemas.Collector.parse_obj(json.loads(response.content))
        except ValidationError:
            assert False
        assert True


class TestDatasets:
    test_dataset = {
        "trigger_date": datetime.utcnow().isoformat(),
        "trigger_pulse_id": 0,
        "path": "/directory/file.h5",
    }
    test_dataset_bad_schema = {
        "trigger_date": "not a timestamp",
        "trigger_pulse_id": 0,
        "path": "/directory/file.h5",
    }

    @classmethod
    def setup_class(cls):
        response = requests.post(
            INDEXER_URL + COLLECTORS_ENDPOINT, json=TestCollector.test_collector
        )
        collector = json.loads(response.content)
        cls.test_dataset["collector_id"] = collector["id"]

    def test_create(self):
        response = requests.post(
            INDEXER_URL + DATASETS_ENDPOINT, json=self.test_dataset
        )
        assert response.status_code == 201

    def test_create_ttl(self):
        response = requests.post(
            INDEXER_URL + DATASETS_ENDPOINT, params={"ttl": 10}, json=self.test_dataset
        )
        assert response.status_code == 201

    def test_create_bad_schema(self):
        response = requests.post(
            INDEXER_URL + DATASETS_ENDPOINT, json=self.test_dataset_bad_schema
        )
        assert response.status_code == 422

    def test_validate_schema(self):
        response = requests.post(
            INDEXER_URL + DATASETS_ENDPOINT, json=self.test_dataset
        )
        try:
            schemas.Dataset.parse_obj(json.loads(response.content))
        except ValidationError:
            assert False
        assert True
