from .collector import Collector
from .config import settings
from .dataset import Dataset, DatasetSchema
from .event import Event

from nexusformat.nexus import nxsetcompression

# Disabling HDF5 compression to improve performance
nxsetcompression("None")
