from pydantic import BaseSettings, FilePath, AnyHttpUrl


class Settings(BaseSettings):
    collector_definitions: FilePath
    collector_timeout: int = 2
    indexer_url: AnyHttpUrl
    wait_for_indexer: bool = True
    indexer_timeout: int = 30


settings = Settings()
