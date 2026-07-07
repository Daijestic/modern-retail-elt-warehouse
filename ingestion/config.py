from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class AppConfig:
    project_dir: Path
    source_data_dir: Path
    db_init_path: Path
    raw_schema: str
    metadata_schema: str
    schema_version: str
    log_level: str
    dbt_project_dir: Path
    dbt_profiles_dir: Path
    docker_compose_file: Path
    postgres_service: str


def get_database_config() -> DatabaseConfig:
    return DatabaseConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        database=os.getenv("POSTGRES_DB", "retail_dw"),
        user=os.getenv("POSTGRES_USER", "retail_user"),
        password=os.getenv("POSTGRES_PASSWORD", "retail_password"),
    )


def get_app_config() -> AppConfig:
    return AppConfig(
        project_dir=BASE_DIR,
        source_data_dir=Path(os.getenv("SOURCE_DATA_DIR", BASE_DIR / "data" / "raw")).resolve(),
        db_init_path=Path(os.getenv("DB_INIT_PATH", BASE_DIR / "db" / "init.sql")).resolve(),
        raw_schema=os.getenv("RAW_SCHEMA", "raw"),
        metadata_schema=os.getenv("METADATA_SCHEMA", "metadata"),
        schema_version=os.getenv("SOURCE_SCHEMA_VERSION", "v1"),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        dbt_project_dir=Path(os.getenv("DBT_PROJECT_DIR", BASE_DIR / "dbt")).resolve(),
        dbt_profiles_dir=Path(os.getenv("DBT_PROFILES_DIR", BASE_DIR / "dbt")).resolve(),
        docker_compose_file=Path(os.getenv("DOCKER_COMPOSE_FILE", BASE_DIR / "docker-compose.yml")).resolve(),
        postgres_service=os.getenv("POSTGRES_SERVICE_NAME", "postgres"),
    )
