from __future__ import annotations

import logging

from ingestion.config import get_app_config

LOG_CONTEXT_FIELDS = [
    "run_id",
    "table_run_id",
    "source_file",
    "target_table",
    "status",
    "rows_read",
    "rows_loaded",
    "rows_rejected",
    "duplicate_count",
    "duration_seconds",
    "error_type",
    "error_message",
]


class _ContextDefaultsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for field_name in LOG_CONTEXT_FIELDS:
            if not hasattr(record, field_name):
                setattr(record, field_name, "-")
        return True


def configure_logging() -> None:
    app_config = get_app_config()
    root_logger = logging.getLogger()

    if root_logger.handlers:
        for handler in root_logger.handlers:
            handler.addFilter(_ContextDefaultsFilter())
        root_logger.setLevel(app_config.log_level)
        return

    handler = logging.StreamHandler()
    handler.addFilter(_ContextDefaultsFilter())
    handler.setFormatter(
        logging.Formatter(
            (
                "%(asctime)s %(levelname)s %(name)s %(message)s | "
                "run_id=%(run_id)s table_run_id=%(table_run_id)s "
                "source_file=%(source_file)s target_table=%(target_table)s status=%(status)s "
                "rows_read=%(rows_read)s rows_loaded=%(rows_loaded)s rows_rejected=%(rows_rejected)s "
                "duplicate_count=%(duplicate_count)s duration_seconds=%(duration_seconds)s "
                "error_type=%(error_type)s error_message=%(error_message)s"
            )
        )
    )

    root_logger.setLevel(app_config.log_level)
    root_logger.addHandler(handler)


def get_logger(name: str = "ingestion") -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)
