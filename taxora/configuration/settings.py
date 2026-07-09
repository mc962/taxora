import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from constants import PROJECT_ROOT


@lru_cache
def _read_pyproject_version() -> str:
    """Read version from pyproject.toml (cached)."""
    try:
        pyproject_path = Path(PROJECT_ROOT) / "pyproject.toml"
        with open(pyproject_path, "rb") as f:
            import tomllib

            data = tomllib.load(f)
        return str(data["project"]["version"])
    except Exception:
        return "0.0.0-dev"


class DatabaseSettings(BaseModel):
    host: str = "localhost"
    port: int = 5432
    user: str = "taxora"
    password: str = ""
    database: str = "taxora"

    def connection_string(self) -> str:
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.getenv("ENV_FILE_PATH", ".env"),
        env_nested_delimiter="__",
        extra="ignore",
    )

    host: str = "0.0.0.0"
    port: int = 8000

    # Temporal
    temporal_address: str = "temporal-internal.florence.place:7233"
    temporal_task_queue: str = "taxora-task-queue"

    # GBIF
    gbif_base_url: str = "https://api.gbif.org/v1"
    gbif_page_size: int = 300
    gbif_max_offset: int = 100_000

    # Database (Postgres only)
    echo_sql: bool = False
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)

    @property
    def application_version(self) -> str:
        return _read_pyproject_version()

    @property
    def database_connection_string(self) -> str:
        return self.database.connection_string()


application_settings = Settings()
