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

sample-data:
	cp data/sample/customers.csv data/raw/customers.csv
	cp data/sample/orders.csv data/raw/orders.csv

dbt-debug:
	dbt debug --project-dir dbt --profiles-dir dbt

dbt-run:
	dbt run --project-dir dbt --profiles-dir dbt

dbt-run-staging:
	dbt run --project-dir dbt --profiles-dir dbt --select staging

dbt-test:
	dbt test --project-dir dbt --profiles-dir dbt

dbt-test-staging:
	dbt test --project-dir dbt --profiles-dir dbt --select staging

dbt-freshness:
	dbt source freshness --project-dir dbt --profiles-dir dbt

dbt-docs:
	dbt docs generate --project-dir dbt --profiles-dir dbt