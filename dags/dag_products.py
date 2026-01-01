import sys
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, task, TriggerRule

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.core.data_quality_validator import DataQualityValidator
from scripts.core.extractor import extract_child_table_by_parent_table, extract_table_by_date
from scripts.core.loader import truncate_and_load
from scripts.utils.file import check_file_exists, get_data_lake_path

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=3)
}

PARENT_TABLE = {
    "name": "products",
    "pk": "id"
}
CHILD_TABLES = [
    # {
    #     "name": "product_metadata",
    #     "p_fk": "product_id"
    # }
]

with DAG(
    dag_id="ingest_products",
    default_args=default_args,
    description="Extract products table from source DB to data warehouse",
    schedule="@daily",
    start_date=pendulum.today(tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "data-warehouse"],
) as dag:

    @task
    def extract_parent(table_name: str, **kwargs):
        """Extract parent table from source DB to data lake."""
        execution_date = kwargs["ds"]
        extract_table_by_date(table_name, execution_date)
    

    @task
    def extract_child(
        parent_table_name: str,
        child_table_name: str,
        primary_key_in_parent: str,
        foreign_key_in_child: str,
        **kwargs
    ):
        """Extract child table from source DB to data lake."""
        execution_date = kwargs["ds"]
        extract_child_table_by_parent_table(
            parent_table_name,
            child_table_name,
            primary_key_in_parent,
            foreign_key_in_child,
            execution_date
        )


    @task.branch(task_display_name="Any data for this date?")
    def validate_data_exists(table_name: str, **kwargs):
        """Check if a file exists."""
        execution_date = kwargs["ds"]
        
        file_path = get_data_lake_path(table_name, execution_date)
        if not check_file_exists(file_path):
            print(f"No data for {execution_date}")
            return "end"
        
        print(f"Data for {execution_date} exists")
        return f"validate_data_quality_{table_name}"


    @task
    def validate_data_quality(table_name: str, **kwargs):
        """Validate extracted data quality.
        
        This implements the circuit breaker pattern if validation fails,
        the task will fail and stop the pipeline.
        """
        execution_date = kwargs["ds"]
        validator = DataQualityValidator()

        print(f"Validating {table_name} data quality for {execution_date}")

        file_path = get_data_lake_path(table_name, execution_date)
        result = validator.validate_parquet_file(table_name, file_path)

        if result["success"]:
            print(f"{table_name} validation passed")
        else:
            print(f"{table_name} validation failed")
            raise ValueError("Data quality validation failed, stopping pipeline.")


    @task
    def load(table_name: str, **kwargs):
        """Load validated data from data lake to raw_ingest tables."""
        execution_date = kwargs["ds"]
        batch_id = execution_date.replace("-", "")
        
        print(f"Loading {table_name} to raw_ingest for {execution_date}")
        
        file_path = get_data_lake_path(table_name, execution_date)
        truncate_and_load(table_name, file_path, batch_id)


    # Start and end tasks
    start = EmptyOperator(task_id="start", task_display_name="Start")
    end = EmptyOperator(task_id="end", task_display_name="End", trigger_rule=TriggerRule.NONE_FAILED)
    
    # Branch
    branch = validate_data_exists(PARENT_TABLE['name'])

    # Parent tasks
    parent_extraction_task = extract_parent.override(
        task_id=f"extract_{PARENT_TABLE['name']}_to_lake",
        task_display_name=f"Extract \"{PARENT_TABLE['name']}\" to data lake"
    )(PARENT_TABLE['name'])
    
    parent_data_quality_validation_task = validate_data_quality.override(
        task_id=f"validate_data_quality_{PARENT_TABLE['name']}",
        task_display_name=f"Validate \"{PARENT_TABLE['name']}\" data quality"
    )(PARENT_TABLE['name'])
    
    parent_load_task = load.override(
        task_id=f"load_{PARENT_TABLE['name']}_to_raw_ingest",
        task_display_name=f"Load \"{PARENT_TABLE['name']}\" to raw_ingest"
    )(PARENT_TABLE['name'])
    
    # Set task dependencies
    start >> parent_extraction_task >> branch

    if CHILD_TABLES:
        # Child Tasks
        for idx, child_table in enumerate(CHILD_TABLES):
            child_extraction_task = extract_child.override(
                task_id=f"extract_{child_table['name']}_to_lake",
                task_display_name=f"Extract \"{child_table['name']}\" to data lake"
            )(PARENT_TABLE['name'], child_table['name'], PARENT_TABLE['pk'], child_table['p_fk'])

            child_data_quality_validation_task = validate_data_quality.override(
                task_id=f"validate_data_quality_{child_table['name']}",
                task_display_name=f"Validate \"{child_table['name']}\" data quality"
            )(child_table['name'])

            child_load_task = load.override(
                task_id=f"load_{child_table['name']}_to_raw_ingest",
                task_display_name=f"Load \"{child_table['name']}\" to raw_ingest"
            )(child_table['name'])

            # Child dependencies
            parent_load_task >> child_extraction_task >> child_data_quality_validation_task >> child_load_task >> end
    else:
        branch >> parent_data_quality_validation_task >> parent_load_task >> end
    
    branch >> end
