import os

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.types import DagRunType

SLACK_CONN_ID = "slack"
ENV = os.environ["ENV"].lower()


def get_task_run_time(task_instance):
    return (task_instance.end_date - task_instance.start_date).total_seconds()


def _should_skip_alert(context):
    if ENV == "datatst":
        return True
    if context["dag_run"].run_type == DagRunType.MANUAL:
        return True
    if getattr(context["dag"], "is_paused", False):
        return True
    return False


def task_success_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of successful task completion
    Args:
        context (dict): Context variable passed in from Airflow
    Returns:
        None: Calls the SlackWebhookOperator execute method internally
    """
    if _should_skip_alert(context):
        return

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
        exec_date=context.get("logical_date", context.get("execution_date")),
        run_time=get_task_run_time(context["task_instance"]),
        log_url=context["task_instance"].log_url,
    )

    success_alert = SlackWebhookOperator(
        task_id="slack_test",
        slack_webhook_conn_id=SLACK_CONN_ID,
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
    if _should_skip_alert(context):
        return

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
        exec_date=context.get("logical_date", context.get("execution_date")),
        run_time=get_task_run_time(context["task_instance"]),
        log_url=context["task_instance"].log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id=context["task_instance"].task_id,
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)
