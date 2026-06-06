up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v
	docker compose up -d

logs:
	docker compose logs -f

ps:
	docker compose ps

install:
	pip install -r requirements.txt

load:
	python -m ingestion.load_csv_to_postgres

test:
	pytest

sql:
	docker exec -it retail_postgres psql -U retail_user -d retail_dw

run-sql:
	docker exec -i retail_postgres psql -U retail_user -d retail_dw < $(FILE)

sql-practice:
	docker exec -i retail_postgres psql -U retail_user -d retail_dw < sql_practice/01_basic_select.sql
	docker exec -i retail_postgres psql -U retail_user -d retail_dw < sql_practice/02_joins.sql
	docker exec -i retail_postgres psql -U retail_user -d retail_dw < sql_practice/03_cte.sql
	docker exec -i retail_postgres psql -U retail_user -d retail_dw < sql_practice/04_window_functions.sql
	docker exec -i retail_postgres psql -U retail_user -d retail_dw < sql_practice/05_data_quality_checks.sql
	docker exec -i retail_postgres psql -U retail_user -d retail_dw < sql_practice/06_business_analysis.sql

sample-data:
	cp data/sample/customers.csv data/raw/customers.csv
	cp data/sample/orders.csv data/raw/orders.csv

dbt-debug:
	powershell -ExecutionPolicy Bypass -File scripts/dbt.ps1 -Command debug

dbt-run:
	powershell -ExecutionPolicy Bypass -File scripts/dbt.ps1 -Command run

dbt-test:
	powershell -ExecutionPolicy Bypass -File scripts/dbt.ps1 -Command test

dbt-run-staging:
	powershell -ExecutionPolicy Bypass -File scripts/dbt.ps1 -Command run -Select staging

dbt-test-staging:
	powershell -ExecutionPolicy Bypass -File scripts/dbt.ps1 -Command test -Select staging

dbt-run-marts:
	powershell -ExecutionPolicy Bypass -File scripts/dbt.ps1 -Command run -Select marts

dbt-test-marts:
	powershell -ExecutionPolicy Bypass -File scripts/dbt.ps1 -Command test -Select marts

dbt-freshness:
	powershell -ExecutionPolicy Bypass -File scripts/dbt.ps1 -Command freshness
	
dbt-docs:
	dbt docs generate --project-dir dbt --profiles-dir dbt