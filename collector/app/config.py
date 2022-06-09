from pydantic import BaseSettings, DirectoryPath, FilePath


class Settings(BaseSettings):
    output_dir: DirectoryPath
    collector_definitions: FilePath
    collector_timeout: int = 2


settings = Settings()
