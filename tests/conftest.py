from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from ingestion.config import get_database_config
from ingestion.db import ensure_database_objects, get_engine, rebuild_database_objects, wait_for_database
from tests.helpers import build_valid_dataset, run_command, write_dataset


def _is_ci_environment() -> bool:
    return os.getenv("CI", "").strip().lower() in {"1", "true", "yes"}


def _skip_or_fail_for_missing_dependency(message: str) -> None:
    if _is_ci_environment():
        pytest.fail(message, pytrace=False)
    pytest.skip(message)


@pytest.fixture()
def source_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    source_dir = write_dataset(tmp_path / "raw", build_valid_dataset())
    monkeypatch.setenv("SOURCE_DATA_DIR", str(source_dir))
    return source_dir


@pytest.fixture(scope="session")
def postgres_engine():
    engine = get_engine()
    try:
        wait_for_database(engine, timeout_seconds=5, interval_seconds=1)
        ensure_database_objects(engine)
    except Exception as exc:  # pragma: no cover - exercised only when DB is missing
        _skip_or_fail_for_missing_dependency(
            f"PostgreSQL is not available for integration tests: {exc}"
        )
    return engine


@pytest.fixture()
def clean_database(postgres_engine):
    rebuild_database_objects(postgres_engine)
    yield postgres_engine
    rebuild_database_objects(postgres_engine)


@pytest.fixture(scope="session")
def dbt_cli():
    dbt_path = shutil.which("dbt")
    if dbt_path is None:
        _skip_or_fail_for_missing_dependency("dbt CLI is not installed in this environment.")
    return dbt_path


@pytest.fixture()
def db_env(monkeypatch: pytest.MonkeyPatch):
    database_config = get_database_config()
    monkeypatch.setenv("POSTGRES_HOST", database_config.host)
    monkeypatch.setenv("POSTGRES_PORT", str(database_config.port))
    monkeypatch.setenv("POSTGRES_DB", database_config.database)
    monkeypatch.setenv("POSTGRES_USER", database_config.user)
    monkeypatch.setenv("POSTGRES_PASSWORD", database_config.password)
    return os.environ.copy()
