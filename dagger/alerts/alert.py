import logging
from abc import ABC, abstractmethod
from typing import List

from slack.web.client import WebClient
from dagger import conf
from dagger.utilities.config_validator import Attribute, ConfigValidator

_logger = logging.getLogger("alerts")


class AlertBase(ConfigValidator, ABC):
    ref_name = None

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [Attribute(attribute_name="type", auto_value=orig_cls.ref_name)]
        )

    def __init__(self, location, alert_config):
        super().__init__(location=location, config=alert_config)

    @abstractmethod
    def execute(self, dag, task, execution_date, run_time, url):
        raise NotImplementedError


class SlackAlert(AlertBase):
    ref_name = "slack"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="channel",
                    validator=str,
                    comment="Name of slack channel or slack id of user E.g.: #airflow-jobs or UN01EL1RU",
                ),
                Attribute(
                    attribute_name="mentions",
                    validator=list,
                    nullable=True,
                    comment="List of slack user ids or slack groups. E.g.: <@UN01EL1RU> for user, @data-eng for slack group",
                ),
            ]
        )

    def __init__(self, location, alert_config):
        super().__init__(location, alert_config)
        self._channel = self.parse_attribute("channel")
        self._mentions = self.parse_attribute("mentions") or []

        try:
            self._slack_token = conf.SLACK_TOKEN
        except KeyError:
            _logger.error("Couldn't get slack_bot_token from variables")
            self._slack_token = None
        except:
            _logger.error("Unexpected error")
            self._slack_token = None

    def execute(self, dag, task, execution_date, run_time, url):
        client = WebClient(token=self._slack_token)

        slack_msg = f"""
                :red_circle: {' '.join(self._mentions)} Task Failed.
                *Task*: {task}
                *Dag*: {dag}
                *Execution Time*: {execution_date}
                *Running For*: {run_time} secs
                *Log Url*: {url}
                """

        response = client.chat_postMessage(
            link_names=1, channel=self._channel, text=slack_msg
        )

        print(response)


class AlertFactory:
    def __init__(self):
        self.factory = dict()

        for cls in AlertBase.__subclasses__():
            self.factory[cls.ref_name] = cls

    def create_alert(self, ref_name, location, alert_config):
        return self.factory[ref_name](location, alert_config)


def get_task_run_time(task_instance):
    return (task_instance.end_date - task_instance.start_date).total_seconds()


def airflow_task_fail_alerts(alerts: List[AlertBase], context):
    if conf.ENV == "datatst":
        return
    if context["dag_run"].external_trigger is True:
        return
    if context["dag"].is_paused is True:
        return

    task_instance = context["task_instance"]
    try:
        run_time = (task_instance.end_date - task_instance.start_date).total_seconds()
    except TypeError as e:
        logging
        run_time = 0

    for alert in alerts:
        alert.execute(
            task_instance.dag_id,
            task_instance.task_id,
            context["execution_date"],
            run_time,
            task_instance.log_url,
        )
