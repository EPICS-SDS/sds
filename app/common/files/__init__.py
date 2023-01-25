from .collector import Collector
from .config import settings
from .dataset import Dataset
from .event import Event
from .nexus_file import NexusFile

from nexusformat.nexus import nxsetcompression

# Disabling HDF5 compression to improve performance
nxsetcompression("None")
