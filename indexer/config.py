from pydantic import BaseSettings, FilePath


class Settings(BaseSettings):
    log_level: str = "INFO"
    collector_definitions: FilePath


settings = Settings()
