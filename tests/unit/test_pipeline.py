from __future__ import annotations

from datetime import datetime

from ingestion.models import PipelineSummary, TableLoadResult
from ingestion.pipeline import determine_pipeline_status, pipeline_exit_code


def _table_result(status: str) -> TableLoadResult:
    now = datetime(2025, 1, 1, 0, 0, 0)
    return TableLoadResult(
        run_id="run-1",
        table_run_id=f"table-{status}",
        source_name="customers",
        source_file="customers.csv",
        target_schema="raw",
        target_table="raw_customers",
        file_size_bytes=1,
        file_checksum="abc",
        schema_version="v1",
        load_strategy="atomic_full_refresh_via_stage_table",
        status=status,
        rows_read=1,
        rows_loaded=1 if status != "FAILED" else 0,
        rows_rejected=0,
        duplicate_count=0,
        started_at=now,
        finished_at=now,
        duration_seconds=0.1,
    )


def test_determine_pipeline_status_returns_success():
    assert determine_pipeline_status([_table_result("SUCCESS")]) == "SUCCESS"


def test_determine_pipeline_status_returns_partial_success():
    assert determine_pipeline_status([_table_result("PARTIAL_SUCCESS")]) == "PARTIAL_SUCCESS"


def test_determine_pipeline_status_returns_failed_when_any_table_failed():
    assert determine_pipeline_status([
        _table_result("SUCCESS"),
        _table_result("FAILED"),
    ]) == "FAILED"


def test_pipeline_exit_code_is_non_zero_only_for_failed_summary():
    now = datetime(2025, 1, 1, 0, 0, 0)
    success_summary = PipelineSummary(
        run_id="run-1",
        status="PARTIAL_SUCCESS",
        started_at=now,
        finished_at=now,
        duration_seconds=0.2,
        table_results=[_table_result("PARTIAL_SUCCESS")],
    )
    failed_summary = PipelineSummary(
        run_id="run-2",
        status="FAILED",
        started_at=now,
        finished_at=now,
        duration_seconds=0.2,
        table_results=[_table_result("FAILED")],
    )

    assert pipeline_exit_code(success_summary) == 0
    assert pipeline_exit_code(failed_summary) == 1
