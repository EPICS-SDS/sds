from pydantic import BaseSettings, DirectoryPath, FilePath, AnyHttpUrl


class Settings(BaseSettings):
    storage_path: DirectoryPath
    collector_definitions: FilePath
    indexer_url: AnyHttpUrl
    collector_timeout: int = 2


settings = Settings()
