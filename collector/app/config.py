from pydantic import BaseSettings, DirectoryPath, FilePath, AnyHttpUrl


class Settings(BaseSettings):
    output_dir: DirectoryPath
    collector_definitions: FilePath
    indexer_url: AnyHttpUrl
    collector_timeout: int = 2


settings = Settings()
