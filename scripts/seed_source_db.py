import os
import random
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import psycopg2
from faker import Faker
from psycopg2 import sql
from psycopg2.extensions import connection

# Init faker
fake = Faker()
Faker.seed(42)  # For reproducibility
random.seed(42)


def get_db_connection() -> connection:
    """Create connection to postgres-source database."""
    conn = psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "localhost"),
        port=os.getenv("SOURCE_DB_PORT", "5433"),
        database=os.getenv("SOURCE_DB_NAME", "source_db"),
        user=os.getenv("SOURCE_DB_USER", "user"),
        password=os.getenv("SOURCE_DB_PASSWORD", "password"),
    )
    return conn


def create_tables(conn: connection) -> None:
    """Create tables in source database if they don't exist."""
    with conn.cursor() as cur:
        # Drop tables if exist (for clean slate)
        cur.execute("""
            DROP TABLE IF EXISTS order_items CASCADE;
            DROP TABLE IF EXISTS orders CASCADE;
            DROP TABLE IF EXISTS products CASCADE;
            DROP TABLE IF EXISTS users CASCADE;
        """)

        # Create users table
        cur.execute("""
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                address TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create products table
        cur.execute("""
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(100),
                price DECIMAL(10, 2) NOT NULL
            );
        """)

        # Create orders table
        cur.execute("""
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                status VARCHAR(50) NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
        """)

        # Create order_items table
        cur.execute("""
            CREATE TABLE order_items (
                id SERIAL PRIMARY KEY,
                order_id INTEGER NOT NULL REFERENCES orders(id),
                product_id INTEGER NOT NULL REFERENCES products(id),
                quantity INTEGER NOT NULL
            );
        """)

        conn.commit()
        print("Tables created successfully")


def generate_users(num_users: int = 100) -> pd.DataFrame:
    """Generate user data."""
    users = []
    for _ in range(num_users):
        users.append({
            "name": fake.name(),
            "email": fake.unique.email(),
            "address": fake.address().replace("\n", ", "),
            "updated_at": fake.date_time_between(start_date="-90d", end_date="now"),
        })
    return pd.DataFrame(users)


def generate_products(num_products: int = 50) -> pd.DataFrame:
    """Generate product data with various categories."""
    categories = [
        "Electronics",
        "Clothing",
        "Books",
        "Home & Garden",
        "Sports",
        "Toys",
        "Food & Beverage",
        "Beauty",
    ]

    products = []
    for _ in range(num_products):
        category = random.choice(categories)
        products.append({
            "name": fake.catch_phrase(),
            "category": category,
            "price": round(random.uniform(5.0, 500.0), 2),
        })
    return pd.DataFrame(products)


def generate_orders(user_ids: List[int], num_orders: int = 500) -> pd.DataFrame:
    """Generate orders spanning the last 3 months, this allows for backfill testing."""
    statuses = ["pending", "shipped", "completed", "cancelled"]
    orders = []

    # Calculate date range (last 3 months)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)

    for _ in range(num_orders):
        # Generate random date within the last 3 months
        random_days = random.randint(0, 90)
        created_at = start_date + timedelta(days=random_days)

        # Add some randomness to hours/minutes
        created_at = created_at.replace(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

        orders.append({
            "user_id": random.choice(user_ids),
            "status": random.choice(statuses),
            "created_at": created_at,
        })

    return pd.DataFrame(orders)


def generate_order_items(order_ids: List[int], product_ids: List[int]) -> pd.DataFrame:
    """Generate order items (1-5 items per order)."""
    order_items = []

    for order_id in order_ids:
        # Each order has 1-5 items
        num_items = random.randint(1, 5)
        selected_products = random.sample(product_ids, min(num_items, len(product_ids)))

        for product_id in selected_products:
            order_items.append({
                "order_id": order_id,
                "product_id": product_id,
                "quantity": random.randint(1, 10),
            })

    return pd.DataFrame(order_items)


def insert_data(conn: connection, table_name: str, df: pd.DataFrame) -> List[int]:
    """
    Insert data into table and return list of inserted IDs.

    Args:
        conn: Database connection
        table_name: Name of the table
        df: DataFrame containing data to insert

    Returns:
        List of inserted IDs
    """
    with conn.cursor() as cur:
        # Prepare column names and placeholders
        columns = df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(placeholders),
        )

        # Insert rows and collect IDs
        ids = []
        for _, row in df.iterrows():
            cur.execute(insert_query, tuple(row))
            ids.append(cur.fetchone()[0])

        conn.commit()
        print(f"Inserted {len(ids)} rows into {table_name}")
        return ids


def simulate_user_address_changes(conn: connection, user_ids: List[int], num_changes: int = 20) -> None:
    """
    Simulate address changes for some users (for SCD Type 2 testing).

    This updates the address and updated_at timestamp for
    random users at different points in time.
    """
    with conn.cursor() as cur:
        for _ in range(num_changes):
            user_id = random.choice(user_ids)
            new_address = fake.address().replace("\n", ", ")
            updated_at = fake.date_time_between(start_date="-60d", end_date="now")

            cur.execute(
                """
                UPDATE users
                SET address = %s, updated_at = %s
                WHERE id = %s
                """,
                (new_address, updated_at, user_id),
            )

        conn.commit()
        print(f"Simulated {num_changes} address changes for SCD Type 2 testing")


def print_summary(conn: connection) -> None:
    """Print summary of seeded data."""
    with conn.cursor() as cur:
        tables = ["users", "products", "orders", "order_items"]
        print("\n" + "=" * 50)
        print("DATABASE SEEDING SUMMARY")
        print("=" * 50)

        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"{table.upper():<20}: {count:>6} rows")

        # Show date range for orders
        cur.execute("SELECT MIN(created_at), MAX(created_at) FROM orders")
        min_date, max_date = cur.fetchone()
        print("\n" + "-" * 50)
        print(f"Orders date range:")
        print(f"  From: {min_date}")
        print(f"  To:   {max_date}")
        print("=" * 50 + "\n")


def main():
    print("\nStarting database seeding process...")
    print("\nConnecting to postgres-source...")
    conn = get_db_connection()
    print("Connected successfully")

    try:
        print("\nCreating tables...")
        create_tables(conn)

        print("\nGenerating users...")
        users_df = generate_users(num_users=100)
        user_ids = insert_data(conn, "users", users_df)

        # Simulate address changes for SCD Type 2
        print("Simulating user address changes...")
        simulate_user_address_changes(conn, user_ids, num_changes=20)

        print("Generating products...")
        products_df = generate_products(num_products=50)
        product_ids = insert_data(conn, "products", products_df)

        print("Generating orders (spanning 3 months)...")
        orders_df = generate_orders(user_ids, num_orders=500)
        order_ids = insert_data(conn, "orders", orders_df)

        print("Generating order items...")
        order_items_df = generate_order_items(order_ids, product_ids)
        insert_data(conn, "order_items", order_items_df)

        print("\nSummary:")
        print_summary(conn)

        print("\nDatabase seeding completed successfully!")

    except Exception as e:
        print(f"\nError during seeding: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
        print("\nDatabase connection closed.\n")


if __name__ == "__main__":
    main()
