import requests
import json

indexer_url = "http://sds_indexer:8000"
retriever_url = "http://sds_retriever:8001"


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
            indexer_url + "/collectors", json=TestCollector.test_collector
        )
        collector = json.loads(response.content)
        cls.test_collector["collector_id"] = collector["id"]

    def test_query_existing_collector(self):
        response = requests.get(
            retriever_url + "/collectors", params={"name": "retriever_test"}
        )
        assert response.status_code == 200
        assert (
            json.loads(response.content)[0]["id"] == self.test_collector["collector_id"]
        )

    def test_query_non_existing_collector(self):
        response = requests.get(
            retriever_url + "/collectors", params={"name": "retriever_test2"}
        )
        assert response.status_code == 200
        assert json.loads(response.content) == []

    def test_get_existing_collector(self):
        response = requests.get(
            retriever_url + "/collectors/" + self.test_collector["collector_id"]
        )
        assert response.status_code == 200
        assert json.loads(response.content)["id"] == self.test_collector["collector_id"]

    def test_get_non_existing_collector(self):
        response = requests.get(retriever_url + "/collectors/wrong_id")
        assert response.status_code == 404
