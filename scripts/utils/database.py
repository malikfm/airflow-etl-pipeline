import os

import psycopg2
from psycopg2.extensions import connection


def get_source_db_connection() -> connection:
    """Create connection to postgres-source database."""
    return psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "localhost"),
        port=int(os.getenv("SOURCE_DB_PORT", "5433")),
        database=os.getenv("SOURCE_DB_NAME", "source_db"),
        user=os.getenv("SOURCE_DB_USER", "user"),
        password=os.getenv("SOURCE_DB_PASSWORD", "password"),
    )


def get_dwh_db_connection() -> connection:
    """Create connection to postgres-dwh database."""
    return psycopg2.connect(
        host=os.getenv("DWH_DB_HOST", "localhost"),
        port=int(os.getenv("DWH_DB_PORT", "5434")),
        database=os.getenv("DWH_DB_NAME", "warehouse_db"),
        user=os.getenv("DWH_DB_USER", "user"),
        password=os.getenv("DWH_DB_PASSWORD", "password"),
    )
