import os

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook

SLACK_CONN_ID = "slack"
ENV = os.environ["ENV"].lower()


def get_task_run_time(task_instance):
    return (task_instance.end_date - task_instance.start_date).total_seconds()


def task_success_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of successful task completion
    Args:
        context (dict): Context variable passed in from Airflow
    Returns:
        None: Calls the SlackWebhookOperator execute method internally
    """
    if ENV == "datatst":
        return
    if context["dag_run"].external_trigger is True:
        return
    if context["dag"].is_paused is True:
        return

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :large_blue_circle: Task Succeeded!
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Running For*: {run_time} secs
            *Log Url*: {log_url}
            """.format(
        task=context["task_instance"].task_id,
        dag=context["task_instance"].dag_id,
        ti=context["task_instance"],
        exec_date=context["execution_date"],
        run_time=get_task_run_time(context["task_instance"]),
        log_url=context["task_instance"].log_url,
    )

    success_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return success_alert.execute(context=context)


def task_fail_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of failure task completion
    Args:
        context (dict): Context variable passed in from Airflow
    Returns:
        None: Calls the SlackWebhookOperator execute method internally
    """
    if ENV == "datatst":
        return
    if context["dag_run"].external_trigger is True:
        return
    if context["dag"].is_paused is True:
        return

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Running For*: {run_time} secs
            *Log Url*: {log_url}
            """.format(
        task=context["task_instance"].task_id,
        dag=context["task_instance"].dag_id,
        ti=context["task_instance"],
        exec_date=context["execution_date"],
        run_time=get_task_run_time(context["task_instance"]),
        log_url=context["task_instance"].log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id=context["task_instance"].task_id,
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)
