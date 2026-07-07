from __future__ import annotations

import sys

from ingestion.pipeline import pipeline_exit_code, run_pipeline


def main() -> int:
    summary = run_pipeline()
    return pipeline_exit_code(summary)


if __name__ == "__main__":
    sys.exit(main())
