"""Execute seeding locally."""
import os
import random
from datetime import UTC, datetime, timedelta

import pandas as pd
import psycopg2
from faker import Faker
from psycopg2 import sql
from psycopg2.extensions import connection

# Init faker
fake = Faker()
Faker.seed(42)  # For reproducibility
random.seed(42)


def get_src_db_connection() -> connection:
    """Create connection to postgres-source database."""
    conn = psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "localhost"),
        port=os.getenv("SOURCE_DB_PORT", "5433"),
        database=os.getenv("SOURCE_DB_NAME", "source_db"),
        user=os.getenv("SOURCE_DB_USER", "user"),
        password=os.getenv("SOURCE_DB_PASSWORD", "password"),
    )
    return conn


def get_dwh_db_connection() -> connection:
    """Create connection to postgres-dwh (warehouse) database."""
    conn = psycopg2.connect(
        host=os.getenv("DWH_DB_HOST", "localhost"),
        port=os.getenv("DWH_DB_PORT", "5434"),
        database=os.getenv("DWH_DB_NAME", "warehouse_db"),
        user=os.getenv("DWH_DB_USER", "user"),
        password=os.getenv("DWH_DB_PASSWORD", "password"),
    )
    return conn


def create_source_tables(conn: connection) -> None:
    """Create tables in source database."""
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
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at TIMESTAMP WITH TIME ZONE
            );
        """)

        # Create products table
        cur.execute("""
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(100),
                price DECIMAL(10, 2) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at TIMESTAMP WITH TIME ZONE
            );
        """)

        # Create orders table
        cur.execute("""
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                status VARCHAR(50) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
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


def create_warehouse_tables(conn: connection) -> None:
    """Create tables in warehouse database for staging and raw schemas.
    
    Tables are created without constraints.
    Schema and data types match source database.
    """
    with conn.cursor() as cur:
        # Create schemas
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_ingest")
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_current")

        # Drop existing tables in raw_ingest
        cur.execute("""
            DROP TABLE IF EXISTS raw_ingest.order_items CASCADE;
            DROP TABLE IF EXISTS raw_ingest.orders CASCADE;
            DROP TABLE IF EXISTS raw_ingest.products CASCADE;
            DROP TABLE IF EXISTS raw_ingest.users CASCADE;
        """)

        # Drop existing tables in raw_current
        cur.execute("""
            DROP TABLE IF EXISTS raw_current.order_items CASCADE;
            DROP TABLE IF EXISTS raw_current.orders CASCADE;
            DROP TABLE IF EXISTS raw_current.products CASCADE;
            DROP TABLE IF EXISTS raw_current.users CASCADE;
        """)

        # Create raw_ingest tables (no constraints, no defaults)
        cur.execute("""
            CREATE TABLE raw_ingest.users (
                batch_id CHAR(8),
                id INTEGER,
                name VARCHAR(255),
                email VARCHAR(255),
                address TEXT,
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                deleted_at TIMESTAMP WITH TIME ZONE
            );
        """)

        cur.execute("""
            CREATE TABLE raw_ingest.products (
                batch_id CHAR(8),
                id INTEGER,
                name VARCHAR(255),
                category VARCHAR(100),
                price DECIMAL(10, 2),
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                deleted_at TIMESTAMP WITH TIME ZONE
            );
        """)

        cur.execute("""
            CREATE TABLE raw_ingest.orders (
                batch_id CHAR(8),
                id INTEGER,
                user_id INTEGER,
                status VARCHAR(50),
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE
            );
        """)

        cur.execute("""
            CREATE TABLE raw_ingest.order_items (
                batch_id CHAR(8),
                id INTEGER,
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER
            );
        """)

        # Create raw_current tables (same structure as raw_ingest)
        cur.execute("""
            CREATE TABLE raw_current.users (
                id INTEGER,
                name VARCHAR(255),
                email VARCHAR(255),
                address TEXT,
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                deleted_at TIMESTAMP WITH TIME ZONE
            );
        """)

        cur.execute("""
            CREATE TABLE raw_current.products (
                id INTEGER,
                name VARCHAR(255),
                category VARCHAR(100),
                price DECIMAL(10, 2),
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                deleted_at TIMESTAMP WITH TIME ZONE
            );
        """)

        cur.execute("""
            CREATE TABLE raw_current.orders (
                id INTEGER,
                user_id INTEGER,
                status VARCHAR(50),
                created_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE
            );
        """)

        cur.execute("""
            CREATE TABLE raw_current.order_items (
                id INTEGER,
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER
            );
        """)

        conn.commit()
        print("Warehouse tables created successfully (staging and raw schemas)")


def generate_users(num_users: int = 100) -> pd.DataFrame:
    """Generate user data."""
    users = []
    base_time = datetime.now(UTC) - timedelta(days=90)

    for i in range(num_users):
        deleted_at = None
        timedelta_created_at = random.randint(0, 90)
        created_at = base_time + timedelta(days=timedelta_created_at)

        # Add some randomness to hours/minutes
        created_at = created_at.replace(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

        if timedelta_created_at == 90:
            updated_at = created_at
        else:
            timedelta_updated_at = min(random.randint(0, 89), 90 - timedelta_created_at)
            updated_at = created_at + timedelta(days=timedelta_updated_at)

            # Add some randomness to hours/minutes
            updated_at = updated_at.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )

            # 5% of users are soft deleted
            if random.random() < 0.05:
                deleted_at = updated_at

        users.append({
            "name": fake.name(),
            "email": fake.unique.email(),
            "address": fake.address().replace("\n", ", "),
            "created_at": created_at,
            "updated_at": updated_at,
            "deleted_at": deleted_at,
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
    base_time = datetime.now(UTC) - timedelta(days=90)

    for i in range(num_products):
        deleted_at = None
        category = random.choice(categories)
        timedelta_created_at = random.randint(0, 90)
        created_at = base_time + timedelta(days=timedelta_created_at)

        # Add some randomness to hours/minutes
        created_at = created_at.replace(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

        if timedelta_created_at == 90:
            updated_at = created_at
        else:
            timedelta_updated_at = min(random.randint(0, 89), 90 - timedelta_created_at)
            updated_at = created_at + timedelta(days=timedelta_updated_at)

            # Add some randomness to hours/minutes
            updated_at = updated_at.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )

            # 3% of products are soft deleted
            if random.random() < 0.03:
                deleted_at = updated_at

        products.append({
            "name": fake.catch_phrase(),
            "category": category,
            "price": round(random.uniform(5.0, 500.0), 2),
            "created_at": created_at,
            "updated_at": updated_at,
            "deleted_at": deleted_at,
        })
    return pd.DataFrame(products)


def generate_orders(users_df: pd.DataFrame, num_orders: int = 500) -> pd.DataFrame:
    """Generate orders spanning the last 3 months.
    
    Orders are created after the user exists (order.created_at > user.created_at).
    """
    statuses = ["pending", "shipped", "completed", "cancelled"]
    orders = []

    base_time = datetime.now(UTC) - timedelta(days=90)

    for _ in range(num_orders):
        status = random.choice(statuses)

        if status == "pending":
            # Create user lookup for created_at
            filtered_users_df = users_df[users_df["created_at"] < pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)]
            user_created_at = filtered_users_df.set_index("id")["created_at"].to_dict()
            user_ids = list(user_created_at.keys())

            # Select a random user
            user_id = random.choice(user_ids)
            user_created = user_created_at[user_id]

            # Ensure order is created after user exists
            # Calculate minimum days after user creation
            days_since_base = (user_created - base_time).days

            # Pending orders are recent (last 2 weeks)
            created_at = base_time + timedelta(days=random.randint(max(days_since_base, 76), 90))

            # Add some randomness to hours/minutes
            created_at = created_at.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )

            # There might be a case where the day is the same but the hour is before user created
            # Ensure order is after user
            if created_at <= user_created:
                created_at = user_created + timedelta(hours=random.randint(1, 24))

            updated_at = created_at

        elif status == "shipped":
            # Create user lookup for created_at
            filtered_users_df = users_df[users_df["created_at"] < pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=5)]
            user_created_at = filtered_users_df.set_index("id")["created_at"].to_dict()
            user_ids = list(user_created_at.keys())

            # Select a random user
            user_id = random.choice(user_ids)
            user_created = user_created_at[user_id]

            # Ensure order is created after user exists
            # Calculate minimum days after user creation
            days_since_base = (user_created - base_time).days

            # Shipped orders are 1-4 weeks old
            created_at = base_time + timedelta(days=random.randint(max(days_since_base, 61), 85))

            # Add some randomness to hours/minutes
            created_at = created_at.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )

            # There might be a case where the day is the same but the hour is before user created
            # Ensure order is after user
            if created_at <= user_created:
                created_at = user_created + timedelta(hours=random.randint(1, 24))

            updated_at = created_at + timedelta(days=random.randint(1, 5))

            # Add some randomness to hours/minutes
            updated_at = updated_at.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )

        else:  # completed or cancelled
            # Create user lookup for created_at
            filtered_users_df = users_df[users_df["created_at"] < pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=15)]
            user_created_at = filtered_users_df.set_index("id")["created_at"].to_dict()
            user_ids = list(user_created_at.keys())

            # Select a random user
            user_id = random.choice(user_ids)
            user_created = user_created_at[user_id]

            # Ensure order is created after user exists
            # Calculate minimum days after user creation
            days_since_base = (user_created - base_time).days

            # Older orders (1-11 weeks old)
            created_at = base_time + timedelta(days=random.randint(max(days_since_base, 11), 75))

            # Add some randomness to hours/minutes
            created_at = created_at.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )

            # There might be a case where the day is the same but the hour is before user created
            # Ensure order is after user
            if created_at <= user_created:
                created_at = user_created + timedelta(hours=random.randint(1, 24))

            updated_at = created_at + timedelta(days=random.randint(5, 15))

            # Add some randomness to hours/minutes
            updated_at = updated_at.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )

        orders.append({
            "user_id": user_id,
            "status": status,
            "created_at": created_at,
            "updated_at": updated_at,
        })

    return pd.DataFrame(orders)


def generate_order_items(orders_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """Generate order items (1-5 items per order).
    
    Products in order items must exist before the order was created
    (product.created_at < order.created_at).
    """
    order_items = []

    # Create product lookup for created_at
    product_created_at = products_df.set_index("id")["created_at"].to_dict()
    product_ids = list(product_created_at.keys())

    for _, order in orders_df.iterrows():
        order_id = order["id"]
        order_created = order["created_at"]

        # Filter products that existed before this order was created
        available_products = [
            pid for pid in product_ids
            if product_created_at[pid] < order_created
        ]

        # If no products available (shouldn't happen with our data), skip
        if not available_products:
            continue

        # Each order has 1-5 items
        num_items = random.randint(1, min(5, len(available_products)))
        selected_products = random.sample(available_products, num_items)

        for product_id in selected_products:
            order_items.append({
                "order_id": order_id,
                "product_id": product_id,
                "quantity": random.randint(1, 10),
            })

    return pd.DataFrame(order_items)


def insert_data(conn: connection, table_name: str, df: pd.DataFrame) -> list[int]:
    """
    Insert data into table and return list of inserted IDs.

    Args:
        conn: Database connection
        table_name: Name of the table
        df: DataFrame containing data to insert

    Returns:
        List of inserted IDs
    """
    # Replace NaT with None for PostgreSQL compatibility
    df = df.replace({pd.NaT: None})

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
        print("Orders date range:")
        print(f"  From: {min_date}")
        print(f"  To:   {max_date}")
        print("=" * 50 + "\n")


def main():
    print("\nStarting database seeding process...")

    # Seed source database
    print("\nConnecting to postgres-source...")
    conn = get_src_db_connection()
    print("Connected successfully")

    try:
        print("\nCreating tables...")
        create_source_tables(conn)

        print("\nGenerating users...")
        users_df = generate_users(num_users=100)
        user_ids = insert_data(conn, "users", users_df)
        # Add IDs to DataFrame for later use
        users_df["id"] = user_ids

        print("Generating products...")
        products_df = generate_products(num_products=50)
        product_ids = insert_data(conn, "products", products_df)
        # Add IDs to DataFrame for later use
        products_df["id"] = product_ids

        print("Generating orders (spanning 3 months)...")
        orders_df = generate_orders(users_df, num_orders=500)
        order_ids = insert_data(conn, "orders", orders_df)
        # Add IDs to DataFrame for later use
        orders_df["id"] = order_ids

        print("Generating order items...")
        order_items_df = generate_order_items(orders_df, products_df)
        insert_data(conn, "order_items", order_items_df)

        print("\nSummary:")
        print_summary(conn)

        print("\nSource database seeding completed successfully!")

    except Exception as e:
        print(f"\nError during source seeding: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
        print("Source database connection closed.")

    # Setup warehouse database
    print("\nConnecting to postgres-dw (warehouse)...")
    dw_conn = get_dwh_db_connection()
    print("Connected successfully")

    try:
        print("\nCreating warehouse tables (staging and raw schemas)...")
        create_warehouse_tables(dw_conn)
        print("\nWarehouse database setup completed successfully!")

    except Exception as e:
        print(f"\nError during warehouse setup: {e}")
        dw_conn.rollback()
        raise
    finally:
        dw_conn.close()
        print("Warehouse database connection closed.\n")


if __name__ == "__main__":
    main()
