"""Execute loading to staging locally."""
import argparse
from datetime import datetime

from scripts.core.loader import truncate_and_load
from scripts.utils.file import check_file_exists, get_data_lake_path


def load_all(execution_date: str) -> None:
    """Load all data from data lake to raw_ingest tables.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
    """
    print(f"\nLoading data to raw_ingest for {execution_date}")
    
    # Load orders
    print("\n1. Loading orders...")
    orders_path = get_data_lake_path("orders", execution_date)
    if check_file_exists(orders_path):
        orders_count = truncate_and_load("orders", orders_path)
        print(f"Loaded {orders_count} orders")
    else:
        print(f"No data found for orders for {execution_date}. Skipping load.")
        orders_count = 0
    
    # Load order items
    print("\n2. Loading order items...")
    order_items_path = get_data_lake_path("order_items", execution_date)
    if check_file_exists(order_items_path):
        order_items_count = truncate_and_load("order_items", order_items_path)
        print(f"Loaded {order_items_count} order items")
    else:
        print(f"No data found for order items for {execution_date}. Skipping load.")
        order_items_count = 0
    
    # Load users
    print("\n3. Loading users...")
    users_path = get_data_lake_path("users", execution_date)
    if check_file_exists(users_path):
        users_count = truncate_and_load("users", users_path)
        print(f"Loaded {users_count} users")
    else:
        print(f"No data found for users for {execution_date}. Skipping load.")
        users_count = 0
    
    # Load products
    print("\n4. Loading products...")
    products_path = get_data_lake_path("products", execution_date)
    if check_file_exists(products_path):
        products_count = truncate_and_load("products", products_path)
        print(f"Loaded {products_count} products")
    else:
        print(f"No data found for products for {execution_date}. Skipping load.")
        products_count = 0
    
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
    
    load_all(execution_date)


if __name__ == "__main__":
    raise SystemExit(main())
