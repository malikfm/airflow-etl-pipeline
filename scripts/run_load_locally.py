"""Execute loading to staging locally."""
import argparse
from datetime import datetime

from scripts.core.loader import truncate_and_load
from scripts.utils.file import get_data_lake_path


def load_all_to_staging(execution_date: str) -> None:
    """Load all data from data lake to staging tables.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
    """
    print(f"\nLoading data to staging for {execution_date}")
    
    # Load orders
    print("\n1. Loading orders...")
    orders_path = get_data_lake_path("orders", execution_date)
    orders_count = truncate_and_load("orders", orders_path)
    print(f"Loaded {orders_count} orders")
    
    # Load order items
    print("\n2. Loading order items...")
    order_items_path = get_data_lake_path("order_items", execution_date)
    order_items_count = truncate_and_load("order_items", order_items_path)
    print(f"Loaded {order_items_count} order items")
    
    # Load users
    print("\n3. Loading users...")
    users_path = get_data_lake_path("users", execution_date)
    users_count = truncate_and_load("users", users_path)
    print(f"Loaded {users_count} users")
    
    # Load products
    print("\n4. Loading products...")
    products_path = get_data_lake_path("products", execution_date)
    products_count = truncate_and_load("products", products_path)
    print(f"Loaded {products_count} products")
    
    print(f"\nLoading completed for {execution_date}")
    print(f"Total rows loaded: {orders_count + order_items_count + users_count + products_count}")


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
    
    load_all_to_staging(execution_date)


if __name__ == "__main__":
    raise SystemExit(main())
