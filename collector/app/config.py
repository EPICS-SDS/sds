from pydantic import BaseSettings, DirectoryPath


class Settings(BaseSettings):
    output_dir: DirectoryPath
    monitor_timeout: int = 2


settings = Settings()
