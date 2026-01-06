#!/bin/bash

set -e

echo "Setting up E-Commerce ELT Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker and try again."
    exit 1
fi

echo "Docker is running"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo ".env file created"
else
    echo ".env file already exists"
fi

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p dags dbt_project scripts/core scripts/utils tests data logs

# Set proper permissions for Airflow
echo "Setting up permissions for Airflow..."
echo "AIRFLOW_UID=$(id -u)" >> .env

echo "Starting Docker services..."
docker-compose up -d

echo "Waiting for services to be healthy..."
sleep 10

echo "Service Status:"
docker-compose ps

echo "Setup complete!"
