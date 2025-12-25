import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.extractors.api_extractor import extract_marketing_data
from scripts.extractors.db_extractor import (
    extract_full_table,
    extract_order_items_by_orders,
    extract_orders,
)


def extract_all(execution_date: str) -> None:
    """
    Extract all data sources for a given execution date.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
    """
    print(f"\nStarting extraction for {execution_date}")
    print("=" * 50)
    
    # Extract transactional data (incremental)
    print("\n1. Extracting orders...")
    extract_orders(execution_date)
    
    print("\n2. Extracting order items...")
    extract_order_items_by_orders(execution_date)
    
    # Extract dimension tables (full snapshot)
    print("\n3. Extracting users (full snapshot)...")
    extract_full_table("users", execution_date)
    
    print("\n4. Extracting products (full snapshot)...")
    extract_full_table("products", execution_date)
    
    # Extract marketing data from API
    print("\n5. Extracting marketing data from API...")
    extract_marketing_data(execution_date)
    
    print("\n" + "=" * 50)
    print(f"Extraction completed for {execution_date}\n")


def main():
    """Main entry point for extraction script."""
    if len(sys.argv) < 2:
        print("Usage: python scripts/extract.py YYYY-MM-DD")
        sys.exit(1)
    
    execution_date = sys.argv[1]
    
    # Validate date format
    try:
        datetime.strptime(execution_date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{execution_date}'. Use YYYY-MM-DD")
        sys.exit(1)
    
    extract_all(execution_date)


if __name__ == "__main__":
    main()
