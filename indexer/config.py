from pydantic import BaseSettings


class Settings(BaseSettings):
    log_level: str = "INFO"


settings = Settings()
