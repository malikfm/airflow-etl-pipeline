import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.common.file_utils import get_data_lake_path
from scripts.loaders.staging_loader import (
    create_staging_schema,
    load_marketing,
    load_order_items,
    load_orders,
    load_products,
    load_users,
)


def load_all_to_staging(execution_date: str) -> None:
    """
    Load all data from data lake to staging tables.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
    """
    print(f"\nLoading data to staging for {execution_date}")
    
    # Ensure staging schema exists
    print("\n1. Creating staging schema...")
    create_staging_schema()
    
    # Load orders
    print("\n2. Loading orders...")
    orders_path = get_data_lake_path("orders", execution_date)
    orders_count = load_orders(orders_path, execution_date)
    print(f"   Loaded {orders_count} orders")
    
    # Load order items
    print("\n3. Loading order items...")
    order_items_path = get_data_lake_path("order_items", execution_date)
    order_items_count = load_order_items(order_items_path, execution_date)
    print(f"   Loaded {order_items_count} order items")
    
    # Load users
    print("\n4. Loading users...")
    users_path = get_data_lake_path("users", execution_date)
    users_count = load_users(users_path, execution_date)
    print(f"   Loaded {users_count} users")
    
    # Load products
    print("\n5. Loading products...")
    products_path = get_data_lake_path("products", execution_date)
    products_count = load_products(products_path, execution_date)
    print(f"   Loaded {products_count} products")
    
    # Load marketing
    print("\n6. Loading marketing...")
    marketing_path = get_data_lake_path("marketing", execution_date)
    marketing_count = load_marketing(marketing_path, execution_date)
    print(f"   Loaded {marketing_count} marketing records")
    
    print(f"\nLoading completed for {execution_date}")
    print(f"Total rows loaded: {orders_count + order_items_count + users_count + products_count + marketing_count}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/load.py YYYY-MM-DD")
        sys.exit(1)
    
    execution_date = sys.argv[1]
    
    # Validate date format
    try:
        datetime.strptime(execution_date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{execution_date}'. Use YYYY-MM-DD")
        sys.exit(1)
    
    load_all_to_staging(execution_date)


if __name__ == "__main__":
    main()
