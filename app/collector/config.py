from pydantic import BaseSettings, FilePath, AnyHttpUrl


class Settings(BaseSettings):
    collector_definitions: FilePath
    indexer_url: AnyHttpUrl
    collector_timeout: int = 2


settings = Settings()
