import sys
from pathlib import Path

import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.sdk import DAG, task

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.core.data_quality_validator import DataQualityValidator
from scripts.core.extractor import extract_table_by_date
from scripts.core.loader import truncate_and_load
from scripts.utils.file import check_file_exists, get_data_lake_path

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5)
}

TABLE_NAME = "users"

with DAG(
    dag_id="ingest_users",
    default_args=default_args,
    description="Extract users table from source DB to data warehouse",
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


    @task.branch(task_id="any_data_for_this_date")
    def validate_extracted_file_exists(table_name: str, **context):
        """Check if a file exists."""
        execution_date = context["ds"]
        file_path = get_data_lake_path(table_name, execution_date)
        
        if not check_file_exists(file_path):
            print(f"No data for {execution_date}")
            return "end_pipeline"
        
        print(f"Data for {execution_date} exists")
        return f"validate_extracted_{table_name}"


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
    start_pipeline = EmptyOperator(task_id="start_pipeline", dag=dag)

    extract_to_lake = extract_data_to_lake_by_date.override(
        task_id=f"extract_{TABLE_NAME}_to_lake"
    )(TABLE_NAME)

    branch = validate_extracted_file_exists(TABLE_NAME)
    
    validate = validate_extracted_data.override(
        task_id=f"validate_extracted_{TABLE_NAME}"
    )(TABLE_NAME)
    
    load_to_staging = load_data_to_staging.override(
        task_id=f"load_{TABLE_NAME}_to_staging"
    )(TABLE_NAME)

    end_pipeline = EmptyOperator(task_id="end_pipeline", dag=dag)
    
    start_pipeline >> extract_to_lake >> branch
    branch >> validate >> load_to_staging >> end_pipeline
    branch >> end_pipeline
