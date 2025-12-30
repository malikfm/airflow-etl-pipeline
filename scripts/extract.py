"""Execute extraction locally."""
import argparse
from datetime import datetime

from scripts.extractors.extractor import (
    extract_child_table_by_parent_table,
    extract_table_by_date,
)


def extract_all(execution_date: str) -> None:
    """Extract all data sources for a given execution date.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
    """
    print(f"\nStarting extraction for {execution_date}")
    
    # Extract transactional data (incremental)
    print("\n1. Extracting orders...")
    extract_table_by_date("orders", execution_date)
    
    print("\n2. Extracting order items...")
    extract_child_table_by_parent_table("orders", "order_items", "order_id", execution_date)
    
    # Extract dimension tables (full snapshot)
    print("\n3. Extracting users (full snapshot)...")
    extract_table_by_date("users", execution_date)
    
    print("\n4. Extracting products (full snapshot)...")
    extract_table_by_date("products", execution_date)
    
    print(f"\nExtraction completed for {execution_date}\n")


def main():
    parser = argparse.ArgumentParser(description="Extract data sources for a given execution date.")
    parser.add_argument("--execution-date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    execution_date = args.execution_date
    
    # Validate date format
    try:
        datetime.strptime(execution_date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{execution_date}'. Use YYYY-MM-DD.")
        return 1
    
    extract_all(execution_date)


if __name__ == "__main__":
    raise SystemExit(main())
