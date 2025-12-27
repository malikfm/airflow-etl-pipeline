from pathlib import Path

import pandas as pd
from sqlalchemy import text

from scripts.common.db_utils import get_dw_db_connection


def create_staging_schema():
    """Create staging schema if it doesn't exist."""
    conn = get_dw_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
            conn.commit()
            print("Staging schema created/verified")
    finally:
        conn.close()


def truncate_and_load(
    table_name: str,
    parquet_path: Path,
    execution_date: str,
) -> int:
    """
    Truncate staging table and load data from Parquet file.
    
    This implements the truncate-insert pattern for staging tables.
    Staging is temporary - dbt will handle history.
    
    Args:
        table_name: Name of the staging table (without stg_ prefix)
        parquet_path: Path to Parquet file
        execution_date: Date in YYYY-MM-DD format
        
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
        staging_table = f"staging.stg_{table_name}"
        
        with conn.cursor() as cur:
            # Truncate staging table
            cur.execute(f"TRUNCATE TABLE {staging_table}")
            print(f"Truncated {staging_table}")
            
            # Create table if not exists (for first run)
            _create_staging_table(cur, table_name, df)
            
            conn.commit()
        
        # Load data using pandas to_sql (more efficient for bulk insert)
        from sqlalchemy import create_engine
        import os
        
        engine = create_engine(
            f"postgresql://{os.getenv('DW_DB_USER', 'user')}:"
            f"{os.getenv('DW_DB_PASSWORD', 'password')}@"
            f"{os.getenv('DW_DB_HOST', 'localhost')}:"
            f"{os.getenv('DW_DB_PORT', '5434')}/"
            f"{os.getenv('DW_DB_NAME', 'warehouse_db')}"
        )
        
        df.to_sql(
            f"stg_{table_name}",
            engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
        )
        
        row_count = len(df)
        print(f"Loaded {row_count} rows into {staging_table}")
        
        return row_count
        
    finally:
        conn.close()


def _create_staging_table(cur, table_name: str, df: pd.DataFrame):
    """Create staging table if it doesn't exist based on DataFrame schema."""
    staging_table = f"staging.stg_{table_name}"
    
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


def load_orders(parquet_path: Path, execution_date: str) -> int:
    """Load orders data to staging.stg_orders."""
    return truncate_and_load("orders", parquet_path, execution_date)


def load_order_items(parquet_path: Path, execution_date: str) -> int:
    """Load order items data to staging.stg_order_items."""
    return truncate_and_load("order_items", parquet_path, execution_date)


def load_users(parquet_path: Path, execution_date: str) -> int:
    """Load users data to staging.stg_users."""
    return truncate_and_load("users", parquet_path, execution_date)


def load_products(parquet_path: Path, execution_date: str) -> int:
    """Load products data to staging.stg_products."""
    return truncate_and_load("products", parquet_path, execution_date)


def load_marketing(parquet_path: Path, execution_date: str) -> int:
    """Load marketing data to staging.stg_marketing."""
    return truncate_and_load("marketing", parquet_path, execution_date)
