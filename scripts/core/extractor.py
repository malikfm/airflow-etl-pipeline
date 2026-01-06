from pathlib import Path

import pandas as pd

from scripts.utils.database import get_source_db_connection
from scripts.utils.file import check_file_exists, get_data_lake_path


def extract_table_by_date(
    table_name: str,
    execution_date: str
) -> Path | None:
    """Extract data from a table filtered by date_column.
    
    Args:
        table_name: Name of the table to extract
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        Path to the saved Parquet file
    """
    output_path = get_data_lake_path(table_name, execution_date)
    if check_file_exists(output_path):
        print(f"File {output_path} already exists. Skipping extraction.")
        return output_path

    conn = get_source_db_connection()

    try:
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE created_at::DATE = %s
            OR updated_at::DATE = %s
        """

        df = pd.read_sql_query(query, conn, params=[execution_date, execution_date])
        print(f"Extracted {len(df)} rows from {table_name} for {execution_date}")

        if df.empty:
            print(f"No new data for {table_name} for {execution_date}")
            return None

        df.to_parquet(output_path, index=False, engine="pyarrow")
        print(f"Saved {len(df)} {table_name} to {output_path}")
        return output_path
    finally:
        conn.close()


def extract_child_table_by_parent_table(
    parent_table_name: str,
    child_table_name: str,
    primary_key_in_parent: str,
    foreign_key_in_child: str,
    execution_date: str
) -> Path | None:
    """Extract child table data based on parent table.
    
    Child table is a table that doesn't have a date_column,
    but it has a foreign key to a table that has a date_column.
    
    Args:
        parent_table_name: Name of the parent table
        child_table_name: Name of the child table
        primary_key_in_parent: Primary key in the parent table
        foreign_key_in_child: Parent table foreign key in the child table
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        Path to the saved Parquet file
    """
    parent_parquet_path = get_data_lake_path(parent_table_name, execution_date)
    output_path = get_data_lake_path(child_table_name, execution_date)
    if check_file_exists(output_path):
        print(f"File {output_path} already exists. Skipping extraction.")
        return output_path

    # Get parent ids from parent parquet file
    parent_ids = pd.read_parquet(parent_parquet_path)[primary_key_in_parent].tolist()

    if not parent_ids:
        print(f"No new data for {parent_table_name} for {execution_date}")
        return None

    conn = get_source_db_connection()

    try:
        query = f"""
            SELECT *
            FROM {child_table_name}
            WHERE {foreign_key_in_child} IN %s
        """

        df = pd.read_sql_query(query, conn, params=[tuple(parent_ids)])
        print(f"Extracted {len(df)} {child_table_name} for {execution_date}")

        df.to_parquet(output_path, index=False, engine="pyarrow")
        print(f"Saved {len(df)} {child_table_name} to {output_path}")
        return output_path
    finally:
        conn.close()
