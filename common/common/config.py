from pydantic import BaseSettings, AnyHttpUrl


class Settings(BaseSettings):
    elastic_url: AnyHttpUrl
    max_query_size: int = 1000
    # Wait in case elastic is starting in parallel
    retry_connection: int = 120


settings = Settings()
