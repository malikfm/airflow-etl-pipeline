#!/bin/bash

set -e

echo "Running database seeding script..."

# Check if running inside Docker or locally
if [ -f "/.dockerenv" ]; then
    echo "Running inside Docker container"
    python /opt/airflow/scripts/seed_source_db.py
else
    echo "Running locally using uv to run script"
    uv run python scripts/seed_source_db.py
fi

echo "Seeding completed!"
