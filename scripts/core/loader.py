import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from scripts.utils.database import get_dw_db_connection
from scripts.utils.file import check_file_exists

SCHEMA_NAME = "staging"


def truncate_and_load(
    table_name: str,
    parquet_path: Path,
) -> int:
    """
    Truncate staging table and load data from Parquet file.
    
    This implements the truncate-insert pattern for staging tables.
    Staging is temporary, dbt will handle history.
    
    Args:
        table_name: Name of the staging table
        parquet_path: Path to Parquet file
        
    Returns:
        Number of rows loaded
    """
    if not check_file_exists(parquet_path):
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    # Read parquet file
    df = pd.read_parquet(parquet_path)
    
    if len(df) == 0:
        print(f"Warning: No data in {parquet_path}")
        return 0

    staging_table = f"{SCHEMA_NAME}.{table_name}"
    
    # Get database connection for truncate
    conn = get_dw_db_connection()
    
    try:
        with conn.cursor() as cur:
            # Truncate table (preserves structure and data types)
            cur.execute(f"TRUNCATE TABLE {staging_table}")
        conn.commit()
    finally:
        conn.close()
    
    # Load data using pandas to_sql with append
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
        if_exists="append",  # append to preserve table structure
        index=False,
        method="multi",
    )
    
    row_count = len(df)
    print(f"Loaded {row_count} rows into {staging_table}")
    
    return row_count
