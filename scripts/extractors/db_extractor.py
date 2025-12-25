from pathlib import Path

import pandas as pd

from scripts.common.db_utils import get_source_db_connection
from scripts.common.file_utils import get_data_lake_path


def extract_table_by_date(
    table_name: str,
    execution_date: str,
    date_column: str = "created_at",
) -> pd.DataFrame:
    """
    Extract data from a table filtered by execution date.
    
    Args:
        table_name: Name of the table to extract
        execution_date: Date in YYYY-MM-DD format
        date_column: Column name to filter by date
        
    Returns:
        DataFrame containing the extracted data
    """
    conn = get_source_db_connection()
    
    try:
        query = f"""
            SELECT * 
            FROM {table_name} 
            WHERE {date_column}::DATE = %s
        """
        
        df = pd.read_sql_query(query, conn, params=(execution_date,))
        print(f"Extracted {len(df)} rows from {table_name} for {execution_date}")
        
        return df
    finally:
        conn.close()


def extract_orders(execution_date: str) -> Path:
    """
    Extract orders data for a specific date and save as Parquet.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        Path to the saved Parquet file
    """
    df = extract_table_by_date("orders", execution_date, "created_at")
    
    output_path = get_data_lake_path("orders", execution_date)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    
    print(f"Saved {len(df)} orders to {output_path}")
    return output_path


def extract_order_items_by_orders(execution_date: str) -> Path:
    """
    Extract order items for orders created on a specific date.
    
    This joins with orders table to get items for orders created on execution_date.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        Path to the saved Parquet file
    """
    conn = get_source_db_connection()
    
    try:
        query = """
            SELECT oi.* 
            FROM order_items oi
            INNER JOIN orders o ON oi.order_id = o.id
            WHERE o.created_at::DATE = %s
        """
        
        df = pd.read_sql_query(query, conn, params=(execution_date,))
        print(f"Extracted {len(df)} order items for {execution_date}")
        
        output_path = get_data_lake_path("order_items", execution_date)
        df.to_parquet(output_path, index=False, engine="pyarrow")
        
        print(f"Saved {len(df)} order items to {output_path}")
        return output_path
    finally:
        conn.close()


def extract_full_table(table_name: str, execution_date: str) -> Path:
    """
    Extract full table snapshot (for dimension tables like users, products).
    
    Args:
        table_name: Name of the table to extract
        execution_date: Date in YYYY-MM-DD format (used for file naming)
        
    Returns:
        Path to the saved Parquet file
    """
    conn = get_source_db_connection()
    
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        
        print(f"Extracted {len(df)} rows from {table_name}")
        
        output_path = get_data_lake_path(table_name, execution_date)
        df.to_parquet(output_path, index=False, engine="pyarrow")
        
        print(f"Saved {len(df)} {table_name} to {output_path}")
        return output_path
    finally:
        conn.close()
