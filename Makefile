.PHONY: help setup up down restart logs clean test lint format check-format type-check

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E "^[a-zA-Z_-]+:.*?## .*$$" $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup:
	@echo "Running initial setup..."
	@./setup.sh

# Service management

up:
	@echo "Starting Docker services..."
	@docker-compose up -d
	@echo "Services started"
	@docker-compose ps

down:
	@echo "Stopping Docker services..."
	@docker-compose down
	@echo "Services stopped"

restart: down up

logs:
	@docker-compose logs -f

logs-airflow:
	@docker-compose logs -f airflow-webserver

logs-scheduler:
	@docker-compose logs -f airflow-scheduler

clean:
	@echo "This will delete all data. Press Ctrl+C to cancel..."
	@sleep 5
	@docker-compose down -v
	@rm -rf logs/* data/raw/* data/processed/*
	@echo "Cleaned up"

ps:
	@docker-compose ps

shell-airflow:
	@docker-compose exec airflow-webserver bash

shell-source:
	@docker-compose exec postgres-source psql -U user -d source_db

shell-dwh:
	@docker-compose exec postgres-dwh psql -U user -d warehouse_db

# Database Seeding

seed:
	@echo "Seeding source database (local)..."
	@uv run python -m scripts.seed_source_db

# Data Extraction

extract-locally:
	@echo "Running extraction locally for date: $(DATE)"
	@uv run python -m scripts.run_extract_locally --execution-date $(DATE)

# Data Quality Validation

validate-locally:
	@echo "Running data quality validation locally for date: $(DATE)"
	@uv run python -m scripts.run_validate_locally --execution-date $(DATE)

# Data Loading to Staging

load-locally:
	@echo "Running data loading locally for date: $(DATE)"
	@uv run python -m scripts.run_load_locally --execution-date $(DATE)

# Development

install:
	@echo "Installing dependencies..."
	@uv sync
	@echo "Dependencies installed"

test:
	@echo "Running tests..."
	@uv run python -m pytest -v

test-cov:
	@echo "Running tests with coverage..."
	@uv run python -m pytest --cov=scripts --cov-report=html --cov-report=term

lint:
	@echo "Running linter..."
	@uv run ruff check .

lint-fix:
	@echo "Running linter with auto-fix..."
	@uv run ruff check --fix .

format:
	@echo "Formatting code..."
	@uv run ruff format .

check-format:
	@echo "Checking code format..."
	@uv run ruff format --check .

type-check:
	@echo "Running type checker..."
	@uv run ty check

qa: lint type-check test

# Airflow

airflow-dags-list:
	@docker-compose exec airflow-webserver airflow dags list

airflow-dags-trigger:
	@docker-compose exec airflow-webserver airflow dags trigger $(DAG_ID)

airflow-tasks-test:
	@docker-compose exec airflow-webserver airflow tasks test $(DAG_ID) $(TASK_ID) $(DATE)

# dbt

dbt-debug:
	@cd /opt/airflow/dbt_project && dbt debug

dbt-deps:
	@cd /opt/airflow/dbt_project && dbt deps

dbt-run:
	@cd /opt/airflow/dbt_project && dbt run

dbt-test:
	@cd /opt/airflow/dbt_project && dbt test

dbt-build:
	@cd /opt/airflow/dbt_project && dbt build

dbt-docs:
	@cd /opt/airflow/dbt_project && dbt docs generate && dbt docs serve
