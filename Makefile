.PHONY: init up down restart clean

init:
	@echo "AIRFLOW_UID=$(shell id -u)" > .env
	docker compose up airflow-init

up:
	docker compose up -d

down:
	docker compose down

restart:
	down up

clean:
	docker compose down --volumes --rmi all
