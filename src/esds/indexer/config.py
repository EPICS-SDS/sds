import logging

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    log_level: int | str = logging.INFO


settings = Settings()
