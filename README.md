# E-Commerce ELT Data Platform

A simple simulation of end-to-end ELT (Extract, Load, Transform) pipeline for e-commerce data processing. Built with Apache Airflow 3.x, dbt Core, PostgreSQL 17, and Great Expectations. The system implements a three-layer data architecture with SCD Type 2 support and circuit breaker data quality validation.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation and Setup](#installation-and-setup)
- [Usage](#usage)
- [Data Schema](#data-schema)
- [Backfill Strategy](#backfill-strategy)
- [Development](#development)
- [Testing](#testing)
- [Important Notes](#important-notes)
- [License](#license)

## Overview

This project implements an end-to-end data pipeline that simulates an e-commerce environment. The pipeline extracts data from a source OLTP database, validates it using Great Expectations, loads it into a data warehouse, and transforms it into analytics-ready tables using dbt.

### Goals

- Historical fidelity through SCD Type 2 implementation for dimension tables
- Operational resilience with circuit breaker pattern for data quality validation
- Scalable transformation using incremental processing
- Full idempotency and backfill support using execution date partitioning
- Deterministic environments with containerized infrastructure

### Scope Limitations

- Batch processing only (no real-time streaming)
- Local or on-premise Docker deployment (no public cloud)
- Ends at validated analytics schema tables (no front-end analytics)

## Architecture

### High-Level Data Flow

```
Source OLTP (PostgreSQL)
    |
    v
[Extraction] --> Parquet Files (Data Lake)
    |
    v
[Validation] --> Great Expectations (Circuit Breaker)
    |
    v
[Load] --> raw_ingest schema (Daily Snapshots)
    |
    v
[Transform] --> raw_current schema (Latest State)
    |
    v
[Transform] --> analytics schema (Facts and Dimensions with SCD2)
```

### Three-Layer dbt Architecture

1. **Daily Snapshot Layer (raw_ingest)**: Stores daily snapshots with batch_id for replayability
2. **Latest Snapshot State Layer (raw_current)**: Maintains the current state of each record
3. **Analytics Layer (analytics)**: Star schema with fact tables and SCD Type 2 dimension tables

### Infrastructure Components

| Service | Port | Purpose |
|---------|------|---------|
| postgres-source | 5433 | Source OLTP database with simulated e-commerce data |
| postgres-dwh | 5434 | Data warehouse (raw_ingest, raw_current, analytics schemas) |
| airflow | 8080 | Orchestration and scheduling |

## Technology Stack

| Category | Technology | Version |
|----------|------------|---------|
| Orchestration | Apache Airflow | 3.x |
| Transformation | dbt Core | 1.10+ |
| Database | PostgreSQL | 17 |
| Data Quality | Great Expectations | 1.x |
| Containerization | Docker and Docker Compose | Latest |
| Package Manager | uv | Latest |
| Linting | Ruff | Latest |
| Testing | Pytest | Latest |
| Language | Python | 3.12+ |

## Project Structure

```
.
├── dags/                           # Airflow DAG definitions
│   ├── dag_users.py                # Users ingestion DAG
│   ├── dag_products.py             # Products ingestion DAG
│   ├── dag_orders.py               # Orders and order_items ingestion DAG
│   └── dag_transformation.py       # dbt transformation DAG
│
├── dbt_project/                    # dbt project
│   ├── models/
│   │   ├── raw_ingest/             # Source definitions for raw_ingest schema
│   │   ├── raw_current/            # Incremental models for latest state
│   │   ├── facts/                  # Fact table models
│   │   └── dimensions/             # Dimension snapshots (SCD Type 2)
│   ├── macros/                     # Custom dbt macros
│   ├── dbt_project.yml             # dbt project configuration
│   └── profiles.yml                # dbt connection profiles
│
├── scripts/                        # Python modules
│   ├── core/
│   │   ├── extractor.py            # Data extraction logic
│   │   ├── loader.py               # Data loading logic
│   │   └── data_quality_validator.py  # Great Expectations validation
│   ├── utils/
│   │   ├── database.py             # Database connection utilities
│   │   └── file.py                 # File handling utilities
│   ├── seed_source_db.py           # Source database seeding script
│   ├── run_extract_locally.py      # Local extraction runner
│   ├── run_validate_locally.py     # Local validation runner
│   └── run_load_locally.py         # Local loading runner
│
├── tests/                          # Pytest unit tests
│   ├── test_extract.py             # Extraction tests
│   ├── test_loader.py              # Loader tests
│   └── test_data_quality_validator.py  # Validation tests
│
├── data/                           # Data lake (Parquet files)
├── logs/                           # Airflow logs
│
├── compose.yaml                    # Docker Compose configuration
├── Makefile                        # Development commands
├── pyproject.toml                  # Python project configuration
├── setup.sh                        # Initial setup script
└── README.md                       # This file
```

## Prerequisites

- Docker and Docker Compose
- Python 3.12 or higher
- uv package manager (recommended)

## Installation and Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd etl-pipeline-airflow
```

### 2. Run Setup

The setup script will check Docker, create necessary directories, configure environment variables, and start all services.

```bash
make setup
```

This command will:
- Verify Docker is running
- Create `.env` file from template
- Create required directories
- Set Airflow UID for proper permissions
- Start all Docker services

### 3. Install Python Dependencies (for local development)

```bash
# Using uv (recommended)
make install

# Or manually
uv sync
```

### 4. Seed the Source Database

```bash
make seed
```

### 5. Access Airflow UI

URL: http://localhost:8080

## Usage

### Running the Pipeline

The pipeline consists of two types of DAGs:

1. **Ingestion DAGs**: Extract data from source, validate, and load to raw_ingest
   - `ingest_users`
   - `ingest_products`
   - `ingest_orders`

2. **Transformation DAG**: Run dbt models
   - `transformation`

DAGs are scheduled to run daily at midnight UTC. The Transformation DAG waits for all Ingestion DAGs to complete before running.

### Manual Execution

```bash
# Extract data for a specific date
make extract-locally DATE=2025-01-01

# Validate extracted data
make validate-locally DATE=2025-01-01

# Load data to raw_ingest
make load-locally DATE=2025-01-01
```

### Database Access

```bash
# Connect to source database
make shell-source

# Connect to data warehouse
make shell-dwh
```

### dbt Commands

```bash
# Run dbt models
make dbt-run

# Run dbt tests
make dbt-test

# Run dbt build (run + test)
make dbt-build

# Generate and serve dbt documentation
make dbt-docs

# Debug dbt connection
make dbt-debug
```

## Data Schema

### Source Tables (OLTP)

| Table | Description |
|-------|-------------|
| users | User information with soft delete support |
| products | Product catalog with pricing |
| orders | Order transactions with status tracking |
| order_items | Order line items linking orders to products |

### Analytics Tables

**Dimensions (SCD Type 2)**:
- `dim_users`: Historical tracking of user information changes
- `dim_products`: Historical tracking of product and pricing changes
- `dim_orders`: Historical tracking of order status changes

**Facts**:
- `fct_orders`: Order-level metrics including totals and item counts
- `fct_order_items`: Line-item level details with calculated subtotals
- `fct_daily_order_statistics`: Daily aggregated metrics for orders and GMV

## Backfill Strategy

The pipeline supports several backfill scenarios:

1. **First Release**: Backfill all historical data into raw_ingest, then run transformation with full-refresh
2. **New Table Addition**: Backfill only the new table using the select flag
3. **Failed Task Recovery**: Clear and retry within the same day, or backfill for later recovery
4. **New Fact Table**: Backfill transformation only with full-refresh for the new fact table

### Data Quality Validation

Great Expectations validates all extracted data before loading. If validation fails:
- The Airflow task is marked as FAILED
- The invalid Parquet file is quarantined by renaming it with an `.invalid.{timestamp}` suffix
- Downstream tasks are blocked
- The original data is preserved for investigation

## Development

### Code Quality

```bash
# Run linter
make lint

# Auto-fix linting issues
make lint-fix

# Format code
make format

# Type checking
make type-check

# Run all quality checks
make qa
```

### Testing

```bash
# Run all tests
make test

# Run tests with coverage report
make test-cov
```

### Useful Make Commands

| Command | Description |
|---------|-------------|
| `make setup` | Run initial setup (create .env, directories, start Docker) |
| `make install` | Install Python dependencies |
| `make up` | Start all Docker services |
| `make down` | Stop all Docker services |
| `make restart` | Restart all services |
| `make logs` | View Docker logs |
| `make clean` | Remove all data and volumes |
| `make ps` | Show running containers |
| `make shell-airflow` | Open shell in Airflow container |
| `make shell-source` | Connect to source database |
| `make shell-dwh` | Connect to data warehouse |
| `make seed` | Seed source database with sample data |
| `make qa` | Run lint, type-check, and tests |

## Testing

The project uses Pytest for unit testing with coverage reporting.

```bash
# Run all tests
uv run pytest -v

# Run specific test file
uv run pytest tests/test_extract.py -v

# Run with coverage
uv run pytest --cov=scripts --cov-report=html
```

### Test Coverage

Tests cover:
- Data extraction logic
- Data loading logic
- Data quality validation including file quarantine functionality

## Important Notes

### Assumptions

- The source database simulates a typical e-commerce OLTP system
- All timestamps are stored in UTC
- The pipeline processes data for the previous day (T-1 processing)
- Parquet files in the data lake are immutable after creation

### Idempotency

- Extraction: Files are not re-extracted if they already exist for a given date
- Loading: Uses truncate-insert per batch_id to ensure clean state
- Transformation: dbt incremental models handle updates correctly

### Data Quality

- Validation happens before loading to prevent bad data from entering the warehouse
- Invalid files are quarantined, not deleted, for audit purposes
- Recovery requires fixing the source issue and re-running the extraction

## License

MIT License. See LICENSE file for details.

## Author

Malik Fajar Mubarok
