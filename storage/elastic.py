import asyncio
import logging
import sys
import time

from elasticsearch import Elasticsearch

# ES indexes
DS_INDEX = 'datasets'
EV_INDEX = 'events'
EX_INDEX = 'expiring_by'
logger = logging.getLogger("storage")

class StorageClient(object):
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
                logger.error('Could not connect to the ElasticSearch server!')
                sys.exit(1)
            
            trials += 1
            time.sleep(1)

    def check_indices(self):
        # Initialize indices
        indices = self.client.indices
        for index in [DS_INDEX, EV_INDEX, EX_INDEX]:
            if indices.exists(index=index)==False:
                indices.create(index=index)

    def search_dataset_id(self, ds_name, ev_name, ev_type, pv_list):
        response = self.client.search(index=DS_INDEX, q='name: {0} AND event: {1} AND eventType: {2}'.format(ds_name, ev_name, ev_type))

        if response['hits']['total']['value'] == 0:
            return None
        
        ds_id = None
        for hit in response['hits']['hits']:
            if len(pv_list) == len(hit['_source']['listOfPvs']):
                ds_id = hit['_id']
                for pv in hit['_source']['listOfPvs']:
                    if pv_list.count(pv) != 1:
                        ds_id = None
                        break
        
        return ds_id

    async def get_dataset_id(self, ds_name, ev_name, ev_type, pv_list):
        async with self.lock:
            # First search in case it already exists...
            ds_id = self.search_dataset_id(ds_name, ev_name, ev_type, pv_list)
            if ds_id is not None:
                return ds_id
            
            # If it doesn't, then create it
            response = self.client.index(index=DS_INDEX, document={'name': ds_name, 'event': ev_name, 'eventType': ev_type, 'listOfPvs': pv_list})
            return response['_id']

    def add_event(self, ds_id, ev_timestamp, tg_pulse_id, path):
        response = self.client.index(index=EV_INDEX, document={'datasetId': ds_id,'timestamp': ev_timestamp, 'tgPulseId': tg_pulse_id, 'path': path})
       
        return response['_id']

    def add_expire_by(self, ev_id, expire_by):
        self.client.index(index=EX_INDEX, document={'eventId': ev_id, 'expireBy': expire_by})
