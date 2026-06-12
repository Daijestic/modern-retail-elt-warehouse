PYTHON ?= python
PROJECT_CLI = $(PYTHON) scripts/project_cli.py

compose-config:
	$(PROJECT_CLI) compose-config

up:
	$(PROJECT_CLI) up

down:
	$(PROJECT_CLI) down

reset:
	$(PROJECT_CLI) reset

logs:
	$(PROJECT_CLI) logs --follow

install:
	$(PYTHON) -m pip install -r requirements.txt

prepare-sample-data:
	$(PROJECT_CLI) prepare-sample-data

init-db:
	$(PROJECT_CLI) init-db

wait-for-postgres:
	$(PROJECT_CLI) wait-for-postgres

load:
	$(PROJECT_CLI) load

dbt-deps:
	$(PROJECT_CLI) dbt-deps

dbt-parse:
	$(PROJECT_CLI) dbt-parse

dbt-build:
	$(PROJECT_CLI) dbt-build

dbt-docs:
	$(PROJECT_CLI) dbt-docs

verify:
	$(PROJECT_CLI) verify

demo:
	$(PROJECT_CLI) demo

test:
	$(PROJECT_CLI) test

check:
	$(PROJECT_CLI) check

lint:
	$(PROJECT_CLI) lint

clean:
	$(PROJECT_CLI) clean

ci-local:
	$(PROJECT_CLI) ci-local
