from pydantic import BaseSettings, DirectoryPath


class Settings(BaseSettings):
    storage_path: DirectoryPath
    compression: bool = True
    compression_level: int = 4


settings = Settings()
