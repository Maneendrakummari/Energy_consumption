import requests

from airflow.hooks.base import BaseHook


def send_slack_failure_alert(context):
    """
    Sends a Slack notification when an Airflow task fails.
    """

    # Get Slack connection from Airflow
    connection = BaseHook.get_connection("slack_failure_alert")

    # Get webhook URL from Airflow Connection Extra
    webhook_url = connection.extra_dejson.get("webhook_token")

    # Get failed task information
    task_instance = context["task_instance"]

    task_id = task_instance.task_id
    dag_id = task_instance.dag_id

    # Get execution information
    execution_date = context.get("logical_date")

    message = {
        "text": f"""
🚨 ENERGY GRID PIPELINE FAILURE

DAG: {dag_id}
Task: {task_id}
Status: FAILED
Time: {execution_date}

Please check Airflow and dbt Cloud logs.
"""
    }

    response = requests.post(
        webhook_url,
        json=message,
        timeout=10,
    )

    response.raise_for_status()
