import logging
from pydantic_settings import BaseSettings
from pydantic import DirectoryPath


class Settings(BaseSettings):
    log_level: int | str = logging.INFO
    storage_path: DirectoryPath


settings = Settings()
