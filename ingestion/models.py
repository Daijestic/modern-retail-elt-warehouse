from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ValidationIssue:
    reason_code: str
    detail: str


@dataclass(frozen=True)
class RowRejection:
    source_row_number: int
    reason_code: str
    reason_detail: str
    raw_record: dict[str, Any]


@dataclass(frozen=True)
class ValidationMetrics:
    rows_read: int = 0
    rows_valid: int = 0
    rows_rejected: int = 0
    duplicate_count: int = 0


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    normalized_dataframe: pd.DataFrame
    accepted_dataframe: pd.DataFrame
    errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[ValidationIssue] = field(default_factory=list)
    row_rejections: list[RowRejection] = field(default_factory=list)
    metrics: ValidationMetrics = field(default_factory=ValidationMetrics)


@dataclass(frozen=True)
class TableLoadResult:
    run_id: str
    table_run_id: str
    source_name: str
    source_file: str
    target_schema: str
    target_table: str
    file_size_bytes: int
    file_checksum: str
    schema_version: str
    load_strategy: str
    status: str
    rows_read: int
    rows_loaded: int
    rows_rejected: int
    duplicate_count: int
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class PipelineSummary:
    run_id: str
    status: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    table_results: list[TableLoadResult]

    @property
    def succeeded(self) -> int:
        return sum(result.status == "SUCCESS" for result in self.table_results)

    @property
    def partial(self) -> int:
        return sum(result.status == "PARTIAL_SUCCESS" for result in self.table_results)

    @property
    def failed(self) -> int:
        return sum(result.status == "FAILED" for result in self.table_results)


@dataclass(frozen=True)
class ExecutionHooks:
    before_target_replacement: Any | None = None
    after_target_truncate: Any | None = None


class TableLoadError(RuntimeError):
    def __init__(self, result: TableLoadResult, original_exception: Exception):
        super().__init__(str(original_exception))
        self.result = result
        self.original_exception = original_exception


class StageValidationError(RuntimeError):
    """Raised when the staged data does not match the validated load expectation."""

