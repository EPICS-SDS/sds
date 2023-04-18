import logging
from pydantic import BaseSettings, DirectoryPath


class Settings(BaseSettings):
    log_level: int | str = logging.INFO
    storage_path: DirectoryPath


settings = Settings()
