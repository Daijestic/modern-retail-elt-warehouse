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
	python ingestion/load_csv_to_postgres.py

test:
	pytest

sql:
	docker exec -it retail_postgres psql -U retail_user -d retail_dw