from pydantic import BaseSettings, DirectoryPath


class Settings(BaseSettings):
    storage_path: DirectoryPath
    compression: bool = True
    compression_level: int = 4
    # Time after which no more data can be appended to the file (from last modification)
    file_appendable_window: float = 60


settings = Settings()
