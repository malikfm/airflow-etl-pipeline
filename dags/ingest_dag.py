import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.extractors.api_extractor import extract_marketing_data
from scripts.extractors.db_extractor import (
    extract_full_table,
    extract_order_items_by_orders,
    extract_orders,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ingest_ecommerce_data",
    default_args=default_args,
    description="Extract e-commerce data from source DB and API to data lake",
    schedule="@daily",
    start_date=datetime(2025, 9, 25),
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "extract", "data-lake"],
)


def extract_orders_task(**context):
    """Extract orders for execution date."""
    execution_date = context["ds"]
    extract_orders(execution_date)


def extract_order_items_task(**context):
    """Extract order items for execution date."""
    execution_date = context["ds"]
    extract_order_items_by_orders(execution_date)


def extract_users_task(**context):
    """Extract full users snapshot."""
    execution_date = context["ds"]
    extract_full_table("users", execution_date)


def extract_products_task(**context):
    """Extract full products snapshot."""
    execution_date = context["ds"]
    extract_full_table("products", execution_date)


def extract_marketing_task(**context):
    """Extract marketing data from API."""
    execution_date = context["ds"]
    extract_marketing_data(execution_date)


def validate_data_quality_task(**context):
    """
    Validate extracted data quality.
    
    This implements the circuit breaker pattern - if validation fails,
    the task will fail and stop the pipeline.
    """
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))

    from scripts.common.data_quality import DataQualityValidator
    from scripts.common.file_utils import get_data_lake_path

    execution_date = context["ds"]
    validator = DataQualityValidator()

    print(f"Validating data for {execution_date}")

    # Validate all extracted files
    validations = {
        "orders": validator.validate_orders,
        "order_items": validator.validate_order_items,
        "users": validator.validate_users,
        "products": validator.validate_products,
        "marketing": validator.validate_marketing,
    }

    all_passed = True
    for table_name, validate_func in validations.items():
        try:
            file_path = get_data_lake_path(table_name, execution_date)
            result = validate_func(table_name, file_path)

            if result["success"]:
                print(f"✓ {table_name} validation passed")
            else:
                print(f"✗ {table_name} validation failed")
                all_passed = False
        except Exception as e:
            print(f"✗ {table_name} validation error: {e}")
            all_passed = False

    if not all_passed:
        raise ValueError("Data quality validation failed - circuit breaker activated")


def load_to_staging_task(**context):
    """Load validated data from data lake to staging tables."""
    import sys
    from pathlib import Path

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

    execution_date = context["ds"]
    
    print(f"Loading data to staging for {execution_date}")
    
    # Create staging schema
    create_staging_schema()
    
    # Load all tables
    tables = {
        "orders": load_orders,
        "order_items": load_order_items,
        "users": load_users,
        "products": load_products,
        "marketing": load_marketing,
    }
    
    total_rows = 0
    for table_name, load_func in tables.items():
        file_path = get_data_lake_path(table_name, execution_date)
        row_count = load_func(file_path, execution_date)
        print(f"Loaded {row_count} rows to staging.stg_{table_name}")
        total_rows += row_count
    
    print(f"Total rows loaded to staging: {total_rows}")


# Define tasks
task_extract_orders = PythonOperator(
    task_id="extract_orders",
    python_callable=extract_orders_task,
    dag=dag,
)

task_extract_order_items = PythonOperator(
    task_id="extract_order_items",
    python_callable=extract_order_items_task,
    dag=dag,
)

task_extract_users = PythonOperator(
    task_id="extract_users",
    python_callable=extract_users_task,
    dag=dag,
)

task_extract_products = PythonOperator(
    task_id="extract_products",
    python_callable=extract_products_task,
    dag=dag,
)

task_extract_marketing = PythonOperator(
    task_id="extract_marketing",
    python_callable=extract_marketing_task,
    dag=dag,
)

task_validate_quality = PythonOperator(
    task_id="validate_data_quality",
    python_callable=validate_data_quality_task,
    dag=dag,
)

task_load_to_staging = PythonOperator(
    task_id="load_to_staging",
    python_callable=load_to_staging_task,
    dag=dag,
)

# Set dependencies
# All extraction tasks run in parallel
extraction_tasks = [
    task_extract_orders,
    task_extract_order_items,
    task_extract_users,
    task_extract_products,
    task_extract_marketing,
]

# Validation runs after all extractions complete
# Loading runs after validation passes
extraction_tasks >> task_validate_quality >> task_load_to_staging
