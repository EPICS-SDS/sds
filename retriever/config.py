from pydantic import BaseSettings, DirectoryPath


class Settings(BaseSettings):
    storage_path: DirectoryPath


settings = Settings()
