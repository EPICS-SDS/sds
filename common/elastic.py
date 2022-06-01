import asyncio
import fnmatch
import logging
import os
import sys
import time

from elasticsearch import Elasticsearch

# ES indexes
DS_INDEX = "datasets"
EV_INDEX = "events"
EX_INDEX = "expiring_by"

ES_QUERY_SIZE = 1000
logger = logging.getLogger("storage")


class IndicesNotAvailableException(Exception):
    """Raised when the indices don't exist in elasticsearch"""

    pass


class TooManyHitsException(Exception):
    """Raised when the number of hits exceeds the limit"""

    pass


class ElasticClient(object):
    def __init__(self):
        ELASTIC_URL = os.environ.get("ELASTIC_URL", "http://127.0.0.1:9200")
        # Password for the 'elastic' user and certificate generated by Elasticsearch
        ELASTIC_PASSWORD = os.environ.get("ELASTIC_PASSWORD", None)
        ELASTIC_CERT = os.environ.get("ELASTIC_CERT", None)

        # Create the client instance
        if ELASTIC_CERT is not None and ELASTIC_PASSWORD is not None:
            self.client = Elasticsearch(
                ELASTIC_URL,
                ca_certs=ELASTIC_CERT,
                basic_auth=("elastic", ELASTIC_PASSWORD),
            )
        else:
            self.client = Elasticsearch(ELASTIC_URL)

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
        for index in [DS_INDEX, EV_INDEX, EX_INDEX]:
            if indices.exists(index=index) == False:
                raise IndicesNotAvailableException()

    def create_mising_indices(self):
        # Initialize indices
        indices = self.client.indices
        for index in [DS_INDEX, EV_INDEX, EX_INDEX]:
            if indices.exists(index=index) == False:
                indices.create(index=index)

    def query_paginated(self, query, sort):
        """
        Make as many queries as neccessary on elasticsearch to retrieve all the hits 
        """
        response = self.client.search(
            index=EV_INDEX, q=query, size=ES_QUERY_SIZE, sort=sort
        )

        n_total = response["hits"]["total"]["value"]

        if n_total == 0:
            return None

        results = []
        n_received = len(response["hits"]["hits"])
        for hit in response["hits"]["hits"]:
            results.append(hit["_source"])

        # If more than ES_QUERY_SIZE hits received, continue to retrieve more pages.
        while n_received < n_total:
            response = self.client.search(
                index=EV_INDEX,
                q=query,
                size=ES_QUERY_SIZE,
                sort=sort,
                search_after=hit["sort"],
            )

            for hit in response["hits"]["hits"]:
                results.append(hit["_source"])
            n_received += len(response["hits"]["hits"])

        return results

    async def get_dataset_id(self, ds_name, ev_name, ev_type, pv_list):
        """
        Get the ID for a dataset that matches all the parameters.
        If no dataset is found, a new one is created.
        """
        async with self.lock:
            # First search in case it already exists...
            ds_id = self.search_dataset_id(ds_name, ev_name, ev_type, pv_list)
            if ds_id is not None:
                return ds_id

            # If it doesn't, then create it
            response = self.client.index(
                index=DS_INDEX,
                document={
                    "name": ds_name,
                    "event": ev_name,
                    "eventType": ev_type,
                    "listOfPvs": pv_list,
                    "created": time.time(),
                },
            )
            return response["_id"]

    def add_event(self, ds_id, ev_timestamp, tg_pulse_id, path):
        """
        Add a new event to the event index. There is no check for the dataset id, so 
        """
        response = self.client.index(
            index=EV_INDEX,
            document={
                "datasetId": ds_id,
                "timestamp": ev_timestamp,
                "tgPulseId": tg_pulse_id,
                "path": path,
            },
        )

        return response["_id"]

    def add_expire_by(self, ev_id, expire_by):
        self.client.index(
            index=EX_INDEX, document={"eventId": ev_id, "expireBy": expire_by}
        )

    def search_dataset_id(self, ds_name, ev_name, ev_type, pv_list):
        response = self.client.search(
            index=DS_INDEX,
            size=ES_QUERY_SIZE,
            q="name: {0} AND event: {1} AND eventType: {2}".format(
                ds_name, ev_name, ev_type
            ),
        )

        n_total = response["hits"]["total"]["value"]

        if n_total == 0:
            return None

        if n_total > ES_QUERY_SIZE:
            raise TooManyHitsException()

        ds_id = None
        for hit in response["hits"]["hits"]:
            if len(pv_list) == len(hit["_source"]["listOfPvs"]):
                ds_id = hit["_id"]
                for pv in hit["_source"]["listOfPvs"]:
                    if pv_list.count(pv) != 1:
                        ds_id = None
                        break

        return ds_id

    def search_datasets(self, dataset_ids, ds_name, ev_name, ev_type, pv_list):
        """
        Search for datasets that contain **at least** the PVs given as a parameter.
        The dataset can contain more PVs than the ones defined, it does not need to be a perfect match.
        All the parameters except the id can contain wildcards, including PV names.
        """
        if dataset_ids is None:
            dataset_ids = "*"
        if ds_name is None:
            ds_name = "*"
        if ev_name is None:
            ev_name = "*"
        if ev_type is None:
            ev_type = "*"

        if len(dataset_ids) > 1:
            dataset_ids = "(" + " OR ".join(dataset_ids) + ")"

        response = self.client.search(
            index=DS_INDEX,
            size=ES_QUERY_SIZE,
            q="_id: {0} AND name: {1} AND event: {2} AND eventType: {3}".format(
                dataset_ids, ds_name, ev_name, ev_type
            ),
        )

        n_total = response["hits"]["total"]["value"]

        if n_total == 0:
            return None

        if n_total > ES_QUERY_SIZE:
            raise TooManyHitsException()

        dataset_list = []
        for hit in response["hits"]["hits"]:
            # Check if the hit contains all PVs in the list
            missing = False
            if pv_list is not None:
                for pv in pv_list:
                    filtered = fnmatch.filter(hit["_source"]["listOfPvs"], pv)
                    if len(filtered) == 0:
                        missing = True

            if not missing:
                dataset = hit["_source"]
                dataset["id"] = hit["_id"]
                dataset_list.append(dataset)

        return dataset_list

    def search_events(
        self, dataset_ids, start, end, tg_pulse_id_start, tg_pulse_id_end
    ):
        if dataset_ids is None:
            dataset_ids = "*"
        elif len(dataset_ids) > 1:
            dataset_ids = "(" + " OR ".join(dataset_ids) + ")"
        else:
            dataset_ids = dataset_ids[0]

        query = "datasetId: {0}".format(dataset_ids)

        if start is not None:
            query += " AND timestamp:>=" + str(start)
        if end is not None:
            query += " AND timestamp:<=" + str(end)
        if tg_pulse_id_start is not None:
            query += " AND tgPulseId:>=" + str(tg_pulse_id_start)
        if tg_pulse_id_end is not None:
            query += " AND tgPulseId:<=" + str(tg_pulse_id_end)

        sort = {"timestamp": {"order": "desc"}}

        return self.query_paginated(query, sort)

    def get_file_events(self, filepath):
        response = self.client.search(
            index=EV_INDEX, size=ES_QUERY_SIZE, q="path: " + filepath
        )

        if response["hits"]["total"]["value"] == 0:
            return None

        if response["hits"]["total"]["value"] > ES_QUERY_SIZE:
            raise TooManyHitsException()

        event_list = []
        for hit in response["hits"]["hits"]:
            event_list.append(hit["_source"])

        return event_list
