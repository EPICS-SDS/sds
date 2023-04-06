from pydantic import BaseSettings, DirectoryPath


class Settings(BaseSettings):
    log_level: str = "INFO"
    storage_path: DirectoryPath


settings = Settings()
