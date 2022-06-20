from pydantic import BaseSettings, AnyHttpUrl, FilePath


class Settings(BaseSettings):
    collector_definitions: FilePath


settings = Settings()
