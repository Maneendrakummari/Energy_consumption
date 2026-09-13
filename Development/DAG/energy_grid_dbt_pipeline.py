from datetime import datetime

from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from failure_handler import send_slack_failure_alert


with DAG(
    dag_id="energy_grid_dbt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "databricks"],
) as dag:

    run_dbt_job = DbtCloudRunJobOperator(
        task_id="run_dbt_job",
        dbt_cloud_conn_id="energy_grid_pipeline",
        job_id=70506183139143,
        wait_for_termination=True,
        on_failure_callback=send_slack_failure_alert,
    )