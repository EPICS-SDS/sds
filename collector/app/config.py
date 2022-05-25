from pydantic import BaseSettings, DirectoryPath, FilePath


class Settings(BaseSettings):
    output_dir: DirectoryPath
    events_file_path: FilePath
    monitor_timeout: int = 2


settings = Settings()
