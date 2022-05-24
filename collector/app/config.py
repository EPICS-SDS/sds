from pydantic import BaseSettings


class Settings(BaseSettings):
    output_dir: str


settings = Settings()
