from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.config import get_app_config
from ingestion.db import ensure_database_objects, get_engine, wait_for_database
from ingestion.pipeline import pipeline_exit_code, run_pipeline
from ingestion.table_config import get_enabled_table_configs

load_dotenv()

DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
GENERATOR_SCRIPT = PROJECT_ROOT / "scripts" / "generate_sample_retail_data.py"


def run_subprocess(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    completed_process = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=False,
        check=False,
        env=os.environ.copy(),
    )
    if check and completed_process.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {completed_process.returncode}: {' '.join(command)}"
        )
    return completed_process


def require_command(command_name: str) -> None:
    if shutil.which(command_name) is None:
        raise RuntimeError(f"Required command '{command_name}' is not available on PATH.")


def compose_base_command() -> list[str]:
    require_command("docker")
    return ["docker", "compose", "-f", str(get_app_config().docker_compose_file)]


def dbt_base_command() -> list[str]:
    require_command("dbt")
    return ["dbt"]


def has_dbt_package_manifest() -> bool:
    return any(
        (DBT_PROJECT_DIR / manifest_name).exists()
        for manifest_name in ("packages.yml", "dependencies.yml")
    )


def run_dbt(command_parts: str | list[str]) -> None:
    app_config = get_app_config()
    normalized_parts = [command_parts] if isinstance(command_parts, str) else command_parts
    run_subprocess(
        [
            *dbt_base_command(),
            *normalized_parts,
            "--project-dir",
            str(app_config.dbt_project_dir),
            "--profiles-dir",
            str(app_config.dbt_profiles_dir),
        ]
    )


def get_dbt_schema_name(custom_schema: str) -> str:
    target_name = os.getenv("DBT_TARGET", "dev")
    target_schema = "analytics"
    if target_name in {"prod", "production"}:
        return custom_schema
    return f"{target_schema}_{custom_schema}"


def command_compose_config(_: argparse.Namespace) -> int:
    run_subprocess(compose_base_command() + ["config"])
    return 0


def command_up(_: argparse.Namespace) -> int:
    run_subprocess(compose_base_command() + ["up", "-d"])
    return 0


def command_down(_: argparse.Namespace) -> int:
    run_subprocess(compose_base_command() + ["down"])
    return 0


def command_reset(_: argparse.Namespace) -> int:
    run_subprocess(compose_base_command() + ["down", "-v"])
    return 0


def command_logs(args: argparse.Namespace) -> int:
    command = compose_base_command() + ["logs"]
    if args.follow:
        command.append("-f")
    run_subprocess(command, check=False)
    return 0


def command_wait_for_postgres(_: argparse.Namespace) -> int:
    engine = get_engine()
    wait_for_database(engine)
    return 0


def command_init_db(_: argparse.Namespace) -> int:
    engine = get_engine()
    ensure_database_objects(engine)
    return 0


def command_prepare_sample_data(_: argparse.Namespace) -> int:
    run_subprocess([sys.executable, str(GENERATOR_SCRIPT)])
    return 0


def command_load(_: argparse.Namespace) -> int:
    summary = run_pipeline()
    return pipeline_exit_code(summary)


def command_dbt_deps(_: argparse.Namespace) -> int:
    if not has_dbt_package_manifest():
        print("No dbt package manifest found; skipping `dbt deps`.")
        return 0
    run_dbt("deps")
    return 0


def command_dbt_parse(_: argparse.Namespace) -> int:
    run_dbt("parse")
    return 0


def command_dbt_build(_: argparse.Namespace) -> int:
    run_dbt("build")
    return 0


def command_dbt_docs(_: argparse.Namespace) -> int:
    run_dbt(["docs", "generate"])
    return 0


def command_test(_: argparse.Namespace) -> int:
    run_subprocess([sys.executable, "-m", "pytest"])
    return 0


def command_check(_: argparse.Namespace) -> int:
    run_subprocess([sys.executable, "-m", "compileall", "ingestion", "scripts", "tests"])
    run_subprocess(compose_base_command() + ["config"])
    return 0


def command_clean(_: argparse.Namespace) -> int:
    for relative_path in [
        PROJECT_ROOT / ".pytest_cache",
        DBT_PROJECT_DIR / "logs",
        DBT_PROJECT_DIR / "target",
        DBT_PROJECT_DIR / "dbt_packages",
    ]:
        if relative_path.exists():
            shutil.rmtree(relative_path, ignore_errors=True)
    return 0


def command_verify(_: argparse.Namespace) -> int:
    from sqlalchemy import text

    engine = get_engine()
    table_names = [table.target_table for table in get_enabled_table_configs()]
    marts_schema = get_dbt_schema_name("marts")
    expected_marts = [
        "fact_orders",
        "fact_order_items",
        "mart_daily_revenue",
        "mart_product_performance",
        "mart_delivery_performance",
    ]

    with engine.connect() as connection:
        for table_name in table_names:
            count = connection.execute(text(f"SELECT COUNT(*) FROM raw.{table_name}")).scalar_one()
            print(f"raw.{table_name}: {count}")

        for table_name in expected_marts:
            count = connection.execute(
                text(f"SELECT COUNT(*) FROM {marts_schema}.{table_name}")
            ).scalar_one()
            print(f"{marts_schema}.{table_name}: {count}")

        gross_order_value = connection.execute(
            text(f"SELECT COALESCE(SUM(gross_order_value), 0) FROM {marts_schema}.mart_daily_revenue")
        ).scalar_one()
        print(f"{marts_schema}.mart_daily_revenue gross_order_value: {gross_order_value}")

    return 0


def command_demo(_: argparse.Namespace) -> int:
    require_command("python")
    require_command("docker")
    require_command("dbt")

    command_compose_config(argparse.Namespace())
    command_up(argparse.Namespace())
    command_wait_for_postgres(argparse.Namespace())
    command_init_db(argparse.Namespace())
    command_prepare_sample_data(argparse.Namespace())

    load_exit_code = command_load(argparse.Namespace())
    if load_exit_code != 0:
        return load_exit_code

    command_dbt_deps(argparse.Namespace())
    command_dbt_build(argparse.Namespace())
    command_verify(argparse.Namespace())
    return 0


def command_ci_local(_: argparse.Namespace) -> int:
    command_check(argparse.Namespace())
    command_up(argparse.Namespace())
    command_wait_for_postgres(argparse.Namespace())
    command_init_db(argparse.Namespace())
    command_test(argparse.Namespace())
    command_dbt_deps(argparse.Namespace())
    command_dbt_parse(argparse.Namespace())
    command_dbt_build(argparse.Namespace())
    command_verify(argparse.Namespace())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local project task runner.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    commands = {
        "compose-config": command_compose_config,
        "up": command_up,
        "down": command_down,
        "reset": command_reset,
        "wait-for-postgres": command_wait_for_postgres,
        "init-db": command_init_db,
        "prepare-sample-data": command_prepare_sample_data,
        "load": command_load,
        "dbt-deps": command_dbt_deps,
        "dbt-parse": command_dbt_parse,
        "dbt-build": command_dbt_build,
        "dbt-docs": command_dbt_docs,
        "test": command_test,
        "check": command_check,
        "lint": command_check,
        "clean": command_clean,
        "verify": command_verify,
        "demo": command_demo,
        "ci-local": command_ci_local,
    }

    for command_name, handler in commands.items():
        subparser = subparsers.add_parser(command_name)
        subparser.set_defaults(handler=handler)

    logs_parser = subparsers.add_parser("logs")
    logs_parser.add_argument("--follow", action="store_true")
    logs_parser.set_defaults(handler=command_logs)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
