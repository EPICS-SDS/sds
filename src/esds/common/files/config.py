from pydantic import DirectoryPath
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    storage_path: DirectoryPath
    compression: bool = True
    compression_level: int = 4


settings = Settings()
