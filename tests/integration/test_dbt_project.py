from __future__ import annotations

import pytest

from ingestion.pipeline import run_pipeline

from tests.helpers import query_scalar, run_command


@pytest.mark.integration
@pytest.mark.dbt
def test_dbt_build_creates_expected_marts(clean_database, source_data_dir, db_env, dbt_cli):
    summary = run_pipeline(engine=clean_database)
    assert summary.status == "SUCCESS"

    deps_result = run_command(
        [dbt_cli, "deps", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        env=db_env,
    )
    assert deps_result.returncode == 0, deps_result.stderr

    parse_result = run_command(
        [dbt_cli, "parse", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        env=db_env,
    )
    assert parse_result.returncode == 0, parse_result.stderr

    build_result = run_command(
        [dbt_cli, "build", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        env=db_env,
    )
    assert build_result.returncode == 0, build_result.stderr

    assert query_scalar(clean_database, "SELECT COUNT(*) FROM analytics_marts.fact_orders") == 2
    assert query_scalar(clean_database, "SELECT COUNT(*) FROM analytics_marts.fact_order_items") == 3
    assert query_scalar(clean_database, "SELECT COUNT(*) FROM analytics_marts.mart_daily_revenue") == 2
    assert (
        float(
            query_scalar(
                clean_database,
                "SELECT SUM(gross_order_value) FROM analytics_marts.mart_daily_revenue",
            )
        )
        == 202.0
    )
