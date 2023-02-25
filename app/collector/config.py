from pydantic import BaseSettings, FilePath, AnyHttpUrl


class Settings(BaseSettings):
    collector_host: str = "0.0.0.0"
    collector_definitions: FilePath
    collector_timeout: int = 2
    events_per_file: int = 1
    indexer_url: AnyHttpUrl
    wait_for_indexer: bool = True
    indexer_timeout_min: int = 1
    indexer_timeout_max: int = 30
    collector_api_enabled: bool = True
    collector_api_port: int = 8000
    collector_ioc_enabled: bool = False
    autostart_collectors: bool = True
    autosave_collectors_definition: bool = True
    status_queue_length: int = 14


settings = Settings()
