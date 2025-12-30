import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from scripts.utils.database import get_dw_db_connection

SCHEMA_NAME = "staging"

def _create_staging_schema(cur):
    """Create staging schema if it doesn't exist."""
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
    print(f"Staging schema created/verified: {SCHEMA_NAME}")


def _create_staging_table(cur, table_name: str, df: pd.DataFrame):
    """Create staging table if it doesn't exist based on DataFrame schema."""
    staging_table = f"{SCHEMA_NAME}.{table_name}"
    
    # Map pandas dtypes to PostgreSQL types
    type_mapping = {
        "int64": "BIGINT",
        "float64": "DOUBLE PRECISION",
        "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP",
        "bool": "BOOLEAN",
    }
    
    columns = []
    for col_name, dtype in df.dtypes.items():
        pg_type = type_mapping.get(str(dtype), "TEXT")
        columns.append(f"{col_name} {pg_type}")
    
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {staging_table} (
            {', '.join(columns)}
        )
    """
    
    cur.execute(create_table_sql)
    print(f"Ensured {staging_table} exists")


def truncate_and_load(
    table_name: str,
    parquet_path: Path,
) -> int:
    """
    Truncate staging table and load data from Parquet file.
    
    This implements the truncate-insert pattern for staging tables.
    Staging is temporary - dbt will handle history.
    
    Args:
        table_name: Name of the staging table (without stg_ prefix)
        parquet_path: Path to Parquet file
        
    Returns:
        Number of rows loaded
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    # Read parquet file
    df = pd.read_parquet(parquet_path)
    
    if len(df) == 0:
        print(f"Warning: No data in {parquet_path}")
        return 0

    # Get database connection
    conn = get_dw_db_connection()
    
    try:
        staging_table = f"{SCHEMA_NAME}.{table_name}"
        
        with conn.cursor() as cur:
            # Create schema and table if not exists (for first run)
            _create_staging_schema(cur)
            _create_staging_table(cur, table_name, df)
            
            conn.commit()
        
        # Load data using pandas to_sql (more efficient for bulk insert)
        engine = create_engine(
            f"postgresql://{os.getenv('DW_DB_USER', 'user')}:"
            f"{os.getenv('DW_DB_PASSWORD', 'password')}@"
            f"{os.getenv('DW_DB_HOST', 'localhost')}:"
            f"{os.getenv('DW_DB_PORT', '5434')}/"
            f"{os.getenv('DW_DB_NAME', 'warehouse_db')}"
        )
        
        df.to_sql(
            table_name,
            engine,
            schema=SCHEMA_NAME,
            if_exists="replace",  # truncate and insert
            index=False,
            method="multi",
        )
        
        row_count = len(df)
        print(f"Loaded {row_count} rows into {staging_table}")
        
        return row_count
        
    finally:
        conn.close()
