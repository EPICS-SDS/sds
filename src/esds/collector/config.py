import logging

from pydantic import AnyHttpUrl, Field, FilePath
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    log_level: int | str = logging.INFO
    collector_host: str = "0.0.0.0"
    collector_definitions: FilePath
    flush_file_delay: float = Field(
        2, description="Maximum delay to flush data from a collector task to file."
    )
    collector_timeout: float = Field(
        2,
        description="Timeout for a collector task in seconds. It restarts every time a new update is received for a given SDS event ID.",
    )
    http_connection_timeout: float = 5
    events_per_file: int = Field(
        1,
        description="Maximum number of SDS events to be stored in a single NeXus file. The file is kept open for more events until the collector tasks time out.",
    )
    indexer_url: AnyHttpUrl
    wait_for_indexer: bool = True
    indexer_timeout_min: int = 1
    indexer_timeout_max: int = 30
    collector_api_enabled: bool = True
    collector_api_port: int = 8000
    collector_ioc_enabled: bool = False
    autostart_collectors: bool = True
    autosave_collectors_definition: bool = True
    status_queue_length: int = Field(
        14,
        description="Number of samples for the moving average used to calculate status values.",
    )


settings = Settings()
