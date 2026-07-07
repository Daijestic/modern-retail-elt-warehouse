from __future__ import annotations

import re
import time
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ingestion.config import get_app_config, get_database_config
from ingestion.table_config import TableConfig, get_enabled_table_configs

_SAFE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def get_engine(database_config=None) -> Engine:
    config = database_config or get_database_config()
    return create_engine(
        config.sqlalchemy_url,
        future=True,
        pool_pre_ping=True,
    )


def quote_identifier(identifier: str) -> str:
    if not _SAFE_IDENTIFIER_PATTERN.match(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier}")
    return f'"{identifier}"'


def qualified_name(schema_name: str, table_name: str) -> str:
    return f"{quote_identifier(schema_name)}.{quote_identifier(table_name)}"


def ensure_database_objects(engine: Engine, init_sql_path: Path | None = None) -> None:
    app_config = get_app_config()
    sql_path = init_sql_path or app_config.db_init_path
    sql_text = sql_path.read_text(encoding="utf-8")

    with engine.begin() as connection:
        cursor = connection.connection.cursor()
        try:
            cursor.execute(sql_text)
        finally:
            cursor.close()


def wait_for_database(engine: Engine, timeout_seconds: int = 60, interval_seconds: int = 2) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except SQLAlchemyError as exc:
            last_error = exc
            time.sleep(interval_seconds)

    raise TimeoutError("PostgreSQL did not become ready before the timeout.") from last_error


def fetch_scalar(engine: Engine, sql: str, parameters: dict | None = None):
    with engine.connect() as connection:
        return connection.execute(text(sql), parameters or {}).scalar_one()


def table_row_count(engine: Engine, schema_name: str, table_name: str) -> int:
    return int(fetch_scalar(engine, f"SELECT COUNT(*) FROM {qualified_name(schema_name, table_name)}"))


def cleanup_stage_tables(engine: Engine, raw_schema: str | None = None) -> None:
    schema_name = raw_schema or get_app_config().raw_schema
    cleanup_sql = f"""
    DO $$
    DECLARE
        stage_table RECORD;
    BEGIN
        FOR stage_table IN
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = '{schema_name}'
              AND tablename LIKE '%__stg_%'
        LOOP
            EXECUTE format('DROP TABLE IF EXISTS %I.%I', '{schema_name}', stage_table.tablename);
        END LOOP;
    END $$;
    """
    with engine.begin() as connection:
        cursor = connection.connection.cursor()
        try:
            cursor.execute(cleanup_sql)
        finally:
            cursor.close()


def reset_database_state(engine: Engine, table_configs: list[TableConfig] | None = None) -> None:
    app_config = get_app_config()
    tables = table_configs or get_enabled_table_configs()

    cleanup_stage_tables(engine, app_config.raw_schema)

    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {qualified_name(app_config.metadata_schema, 'ingestion_rejections')}"))
        connection.execute(text(f"TRUNCATE TABLE {qualified_name(app_config.metadata_schema, 'ingestion_runs')}"))
        connection.execute(text(f"TRUNCATE TABLE {qualified_name(app_config.metadata_schema, 'pipeline_runs')}"))

        for table_config in tables:
            connection.execute(
                text(f"TRUNCATE TABLE {qualified_name(app_config.raw_schema, table_config.target_table)}")
            )


def rebuild_database_objects(engine: Engine) -> None:
    app_config = get_app_config()
    with engine.begin() as connection:
        connection.execute(text(f"DROP SCHEMA IF EXISTS {quote_identifier(app_config.raw_schema)} CASCADE"))
        connection.execute(text(f"DROP SCHEMA IF EXISTS {quote_identifier(app_config.metadata_schema)} CASCADE"))
    ensure_database_objects(engine)
