import logging

from pydantic import DirectoryPath
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    log_level: int | str = logging.INFO
    storage_path: DirectoryPath


settings = Settings()
