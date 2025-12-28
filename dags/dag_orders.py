import sys
from pathlib import Path

import pendulum
from airflow.sdk import DAG, task

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.common.data_quality import DataQualityValidator
from scripts.common.file_utils import get_data_lake_path
from scripts.extractors.db_extractor import (
    extract_child_table_by_parent_table,
    extract_table_by_date
)
from scripts.loaders.staging_loader import truncate_and_load

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5)
}

PARENT_TABLE_NAME = "orders"
CHILD_TABLES = [
    {
        "table_name": "order_items",
        "join_column": "order_id"
    }
]

with DAG(
    dag_id="ingest_orders",
    default_args=default_args,
    description="Extract orders table from source DB to data warehouse",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 9, 25, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "data-warehouse"]
) as dag:


    @task
    def extract_data_to_lake_by_date(table_name: str, **context):
        """Extract table from source DB to data lake."""
        execution_date = context["ds"]
        extract_table_by_date(table_name, execution_date)
    

    @task
    def extract_data_to_lake_by_parent(
        parent_table_name: str,
        child_table_name: str,
        child_table_join_column: str,
        **context
    ):
        """Extract table from source DB to data lake."""
        execution_date = context["ds"]
        extract_child_table_by_parent_table(
            parent_table_name, child_table_name, child_table_join_column, execution_date
        )


    @task
    def validate_extracted_data(table_name: str, **context):
        """Validate extracted data quality.
        
        This implements the circuit breaker pattern if validation fails,
        the task will fail and stop the pipeline.
        """
        execution_date = context["ds"]
        validator = DataQualityValidator()

        print(f"Validating data for {execution_date}")

        file_path = get_data_lake_path(table_name, execution_date)
        result = validator.validate_parquet_file(table_name, file_path)

        if result["success"]:
            print(f"{table_name} validation passed")
        else:
            print(f"{table_name} validation failed")
            raise ValueError("Data quality validation failed, stopping pipeline.")


    @task
    def load_data_to_staging(table_name: str, **context):
        """Load validated data from data lake to staging tables."""
        execution_date = context["ds"]
        
        print(f"Loading data to staging for {execution_date}")
        
        file_path = get_data_lake_path(table_name, execution_date)
        row_count = truncate_and_load(table_name, file_path)
        
        print(f"Loaded {row_count} rows to staging.stg_{table_name}")


    # Set task dependencies
    # Parent
    extract_parent_to_lake = extract_data_to_lake_by_date.override(
        task_id=f"extract_{PARENT_TABLE_NAME}_to_lake"
    )(PARENT_TABLE_NAME)
    
    validate_extracted_parent = validate_extracted_data.override(
        task_id=f"validate_extracted_{PARENT_TABLE_NAME}"
    )(PARENT_TABLE_NAME)
    
    load_parent_to_staging = load_data_to_staging.override(
        task_id=f"load_{PARENT_TABLE_NAME}_to_staging"
    )(PARENT_TABLE_NAME)
    
    # Child
    extract_child_to_lake = []
    validate_extracted_child = []
    load_child_to_staging = []
    for child_table in CHILD_TABLES:
        extract_child_to_lake.append(
            extract_data_to_lake_by_parent.override(
                task_id=f"extract_{child_table['table_name']}_to_lake"
            )(PARENT_TABLE_NAME, child_table["table_name"], child_table["join_column"])
        )

        validate_extracted_child.append(
            validate_extracted_data.override(
                task_id=f"validate_extracted_{child_table['table_name']}"
            )(child_table["table_name"])
        )

        load_child_to_staging.append(
            load_data_to_staging.override(
                task_id=f"load_{child_table['table_name']}_to_staging"
            )(child_table["table_name"])
        )

    extract_parent_to_lake >> extract_child_to_lake >> validate_extracted_parent >> validate_extracted_child >> load_parent_to_staging >> load_child_to_staging
