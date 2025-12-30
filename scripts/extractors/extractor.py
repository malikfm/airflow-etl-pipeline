from pathlib import Path

import pandas as pd

from scripts.common.db_utils import get_source_db_connection
from scripts.common.file_utils import get_data_lake_path


def _check_file_exists(file_path: Path) -> bool:
    if file_path.exists():
        print(f"File {file_path} already exists. Skipping extraction.")
        return True
    return False


def extract_table_by_date(
    table_name: str,
    execution_date: str,
    date_column: str = "updated_at",
) -> Path:
    """Extract data from a table filtered by date_column.
    
    Args:
        table_name: Name of the table to extract
        execution_date: Date in YYYY-MM-DD format
        date_column: Column name to filter by date
        
    Returns:
        Path to the saved Parquet file
    """
    output_path = get_data_lake_path(table_name, execution_date)
    if _check_file_exists(output_path):
        return output_path
    
    conn = get_source_db_connection()
    
    try:
        query = f"""
            SELECT * 
            FROM {table_name} 
            WHERE {date_column}::DATE = %s
        """
        
        df = pd.read_sql_query(query, conn, params=(execution_date,))
        print(f"Extracted {len(df)} rows from {table_name} for {execution_date}")
        
        df.to_parquet(output_path, index=False, engine="pyarrow")
        
        print(f"Saved {len(df)} {table_name} to {output_path}")
        return output_path
    finally:
        conn.close()


def extract_child_table_by_parent_table(
    parent_table_name: str,
    child_table_name: str,
    child_table_join_column: str,
    execution_date: str,
    date_column: str = "updated_at",
) -> Path:
    """Extract child table data based on parent table.
    
    Join child table with parent table to get child table data based on parent table date_column.
    
    Args:
        parent_table_name: Name of the parent table
        child_table_name: Name of the child table
        child_table_join_column: Column name to join child table with parent table
        execution_date: Date in YYYY-MM-DD format
        date_column: Column name to filter by date
        
    Returns:
        Path to the saved Parquet file
    """
    output_path = get_data_lake_path(child_table_name, execution_date)
    if _check_file_exists(output_path):
        return output_path
    
    conn = get_source_db_connection()
    
    try:
        query = f"""
            SELECT c.* 
            FROM {child_table_name} c
            JOIN {parent_table_name} p ON c.{child_table_join_column} = p.id
            WHERE p.{date_column}::DATE = %s
        """
        
        df = pd.read_sql_query(query, conn, params=(execution_date,))
        print(f"Extracted {len(df)} {child_table_name} for {execution_date}")
        
        df.to_parquet(output_path, index=False, engine="pyarrow")
        
        print(f"Saved {len(df)} {child_table_name} to {output_path}")
        return output_path
    finally:
        conn.close()
