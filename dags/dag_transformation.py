import sys
from pathlib import Path

import pendulum
from airflow.models import Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG, task, TriggerRule

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=3),
}

# Ingestion DAGs to wait for
INGESTION_DAGS = ["ingest_orders", "ingest_users", "ingest_products"]

with DAG(
    dag_id="transform_dbt",
    default_args=default_args,
    description="Run dbt transformations: raw_ingest -> raw_current -> analytics",
    schedule="@daily",
    start_date=pendulum.today(tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["transformation", "dbt", "data-warehouse"],
    params={
        "is_backfill": Param(
            default=False,
            type="boolean",
            description="Set this True to do backfill or manual trigger.",
        ),
        "is_full_refresh": Param(
            default=False,
            type="boolean",
            description="This parameter is only used for backfill or manual trigger. If True, run full refresh.",
        ),
        "models": Param(
            default="raw_current.*",
            type="string",
            description=(
                "This parameter is only used for backfill or manual trigger."
                " dbt --select argument to backfill specific models."
                " Examples: 'raw_current.*' (all), 'analytics.daily_order_statistics' (specific table),"
                " 'raw_current.users raw_current.orders' (multiple tables)"
            ),
        ),
    },
) as dag:
    
    start = EmptyOperator(task_id="start", task_display_name="Start")
    end = EmptyOperator(
        task_id="end", 
        task_display_name="End", 
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    # Branch: check if should skip waiting for ingestion
    @task.branch(task_display_name="Skip Wait for Ingestion?")
    def check_skip_wait(**context):
        """Branch based on skip_wait_ingestion parameter."""
        skip_wait = context["params"].get("is_backfill", False)
        if skip_wait:
            return "all_ingestions_done"
        return "wait_for_all_ingestions"
    
    branch_skip_wait = check_skip_wait()
    
    # Wait for all Ingestion DAGs to complete
    wait_tasks = []
    for dag_id in INGESTION_DAGS:
        wait_task = ExternalTaskSensor(
            task_id=f"wait_for_{dag_id}",
            task_display_name=f"Wait for {dag_id}",
            external_dag_id=dag_id,
            external_task_id="end",
            mode="poke",
            timeout=3600,  # 1 hour timeout
            poke_interval=60,  # Check every minute
            allowed_states=["success"],
            failed_states=["failed", "skipped"],
        )
        wait_tasks.append(wait_task)
    
    # Wait for all ingestions
    wait_for_all_ingestions = EmptyOperator(
        task_id="wait_for_all_ingestions",
        task_display_name="Wait for All Ingestions",
    )

    all_ingestions_done = EmptyOperator(
        task_id="all_ingestions_done",
        task_display_name="All Ingestions Done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    dbt_daily_build = BashOperator(
        task_id="dbt_daily_build",
        task_display_name="dbt build: daily",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build" + " --vars '{\"snapshot_date\": \"{{ (logical_date - macros.timedelta(days=1)) | ds }}\"}'",
    )

    base_backfill_cmd = f"cd {DBT_PROJECT_DIR} && dbt build --select {dag.params['models']}"
    if dag.params.get("is_full_refresh", False):
        dbt_backfill_build = BashOperator(
            task_id="dbt_backfill_build",
            task_display_name="dbt build: backfill",
            bash_command=base_backfill_cmd + " --vars '{\"snapshot_date\": \"{{ (logical_date - macros.timedelta(days=1)) | ds }}\"}' --full-refresh",
        )
    else:
        dbt_backfill_build = BashOperator(
            task_id="dbt_backfill_build",
            task_display_name="dbt build: backfill",
            bash_command=base_backfill_cmd + " --vars '{\"snapshot_date\": \"{{ (logical_date - macros.timedelta(days=1)) | ds }}\"}'",
        )   

    # Branch: check if backfill mode
    @task.branch(task_display_name="Backfill Mode?")
    def check_backfill_mode(**context):
        """Branch based on backfill_mode parameter."""
        is_backfill = context["params"].get("is_backfill", False)
        if is_backfill:
            # Skip analytics and snapshots in backfill mode
            return "dbt_backfill_build"
        return "dbt_daily_build"
    
    branch_backfill = check_backfill_mode()
    
    # Task dependencies
    # Start -> Check if skip wait
    start >> branch_skip_wait
    
    # Normal path: wait for ingestions
    branch_skip_wait >> wait_for_all_ingestions >> wait_tasks >> all_ingestions_done
    
    # Skip wait path: go directly to all_ingestions_done
    branch_skip_wait >> all_ingestions_done
    
    # Continue with dbt tasks
    all_ingestions_done >> branch_backfill
    
    # Normal mode: run snapshots and analytics
    branch_backfill >> dbt_daily_build >> end
    
    # Backfill mode: skip to end
    branch_backfill >> dbt_backfill_build >> end
