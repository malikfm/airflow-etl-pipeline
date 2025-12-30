import os

import psycopg2
from psycopg2.extensions import connection


def get_source_db_connection() -> connection:
    """Create connection to postgres-source database."""
    conn = psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "localhost"),
        port=int(os.getenv("SOURCE_DB_PORT", "5433")),
        database=os.getenv("SOURCE_DB_NAME", "source_db"),
        user=os.getenv("SOURCE_DB_USER", "user"),
        password=os.getenv("SOURCE_DB_PASSWORD", "password"),
    )
    return conn


def get_dw_db_connection() -> connection:
    """Create connection to postgres-dw database."""
    conn = psycopg2.connect(
        host=os.getenv("DW_DB_HOST", "localhost"),
        port=int(os.getenv("DW_DB_PORT", "5434")),
        database=os.getenv("DW_DB_NAME", "warehouse_db"),
        user=os.getenv("DW_DB_USER", "user"),
        password=os.getenv("DW_DB_PASSWORD", "password"),
    )
    return conn
