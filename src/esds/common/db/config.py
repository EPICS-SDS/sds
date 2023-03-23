from typing import Optional
from pydantic import BaseSettings, AnyHttpUrl


class Settings(BaseSettings):
    elastic_url: AnyHttpUrl
    elastic_password: Optional[str] = None
    max_query_size: int = 1000
    # Wait in case elastic is starting in parallel
    retry_connection: int = 120
    ilm_policy_max_docs: int = 10000000
    ilm_policy_max_primary_shard_size: str = "50gb"


settings = Settings()
