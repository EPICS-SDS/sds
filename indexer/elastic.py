import asyncio
import logging
import sys
import time

from elasticsearch import Elasticsearch

# ES indexes
CO_INDEX = "collectors"
DS_INDEX = "datasets"
EX_INDEX = "expiring_by"
logger = logging.getLogger("indexer")


class IndexerClient(object):
    def __init__(self):
        # Create the client instance
        self.client = Elasticsearch(
            "http://elasticsearch:9200"
        )

        self.lock = asyncio.Lock()

    def wait_for_connection(self, timeout):
        trials = 0
        while not self.client.ping():
            if trials > timeout:
                logger.error("Could not connect to the ElasticSearch server!")
                sys.exit(1)

            trials += 1
            time.sleep(1)

    def check_indices(self):
        # Initialize indices
        indices = self.client.indices
        for index in [CO_INDEX, DS_INDEX, EX_INDEX]:
            if not indices.exists(index=index):
                indices.create(index=index)

    def search_collector_id(self, collector_name, method_name, method_type, pv_list):
        response = self.client.search(index=CO_INDEX, q=(
            f"name: {collector_name} AND "
            f"event: {method_name} AND eventType: {method_type}"
        ))

        if response["hits"]["total"]["value"] == 0:
            return None

        ds_id = None
        for hit in response["hits"]["hits"]:
            if len(pv_list) == len(hit["_source"]["listOfPvs"]):
                ds_id = hit["_id"]
                for pv in hit["_source"]["listOfPvs"]:
                    if pv_list.count(pv) != 1:
                        ds_id = None
                        break

        return ds_id

    async def get_collector_id(self, collector_name, method_name, method_type, pv_list):
        async with self.lock:
            # First search in case it already exists...
            ds_id = self.get_collector_id(
                collector_name, method_name, method_type, pv_list)
            if ds_id is not None:
                return ds_id

            # If it doesn"t, then create it
            document = {"name": collector_name, "event": method_name,
                        "eventType": method_type, "listOfPvs": pv_list}
            response = self.client.index(index=CO_INDEX, document=document)
            return response["_id"]

    def add_dataset(self, collector_id, ev_timestamp, tg_pulse_id, path):
        document = {"collectorId": collector_id, "timestamp": ev_timestamp,
                    "tgPulseId": tg_pulse_id, "path": path}
        response = self.client.index(index=DS_INDEX, document=document)

        return response["_id"]

    def add_expire_by(self, dataset_id, expire_by):
        document = {"datasetId": dataset_id, "expireBy": expire_by}
        self.client.index(index=EX_INDEX, document=document)
