import sys
from pathlib import Path

import pendulum
from airflow.sdk import DAG, task

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.common.data_quality import DataQualityValidator
from scripts.common.file_utils import get_data_lake_path
from scripts.extractors.api_extractor import extract_marketing_data
from scripts.loaders.staging_loader import truncate_and_load

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5)
}

with DAG(
    dag_id="ingest_marketing_api",
    default_args=default_args,
    description="Extract marketing data from API",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 9, 25, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "data-warehouse"]
) as dag:


    @task
    def extract_marketing_data(**context):
        """Extract marketing data from API."""
        execution_date = context["ds"]
        extract_marketing_data(execution_date)


    @task
    def validate_extracted_data(**context):
        """Validate extracted data quality.
        
        This implements the circuit breaker pattern if validation fails,
        the task will fail and stop the pipeline.
        """
        execution_date = context["ds"]
        validator = DataQualityValidator()

        print(f"Validating data for {execution_date}")

        file_path = get_data_lake_path("marketing", execution_date)
        result = validator.validate_parquet_file("marketing", file_path)

        if result["success"]:
            print(f"Marketing validation passed")
        else:
            print(f"Marketing validation failed")
            raise ValueError("Data quality validation failed, stopping pipeline.")


    @task
    def load_data_to_staging(**context):
        """Load validated data from data lake to staging tables."""
        execution_date = context["ds"]
        
        print(f"Loading data to staging for {execution_date}")
        
        file_path = get_data_lake_path("marketing", execution_date)
        row_count = truncate_and_load("marketing", file_path)
        
        print(f"Loaded {row_count} rows to staging.stg_marketing")


    # Set task dependencies
    extract_to_lake = extract_marketing_data.override(
        task_id="extract_marketing_to_lake"
    )()
    
    validate = validate_extracted_data.override(
        task_id="validate_extracted_marketing"
    )()
    
    load_to_staging = load_data_to_staging.override(
        task_id="load_marketing_to_staging"
    )()
    
    extract_to_lake >> validate >> load_to_staging
