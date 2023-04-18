import logging
from pydantic import BaseSettings


class Settings(BaseSettings):
    log_level: int | str = logging.INFO


settings = Settings()
