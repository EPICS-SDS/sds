import aiohttp
from common.files import Dataset, DatasetSchema

from collector.config import settings


class IndexableDataset(Dataset):
    async def index(self):
        """
        Publish metadata into the indexer service
        """
        try:
            print(repr(self), "indexing...")
            url = settings.indexer_url + "/datasets"
            data = DatasetSchema.parse_obj(self).json()
            headers = {"Content-Type": "application/json"}
            async with aiohttp.ClientSession(headers=headers) as client:
                async with client.post(url, data=data) as response:
                    response.raise_for_status()
            print(repr(self), "indexing done.")
        except Exception as e:
            print(repr(self), "indexing failed!")
            print(e)
