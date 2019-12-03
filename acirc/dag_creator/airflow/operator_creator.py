from abc import ABC, abstractmethod
from datetime import timedelta
from circ.utils.slack_alerts import task_fail_slack_alert


class OperatorCreator(ABC):
    def __init__(self, task, dag):
        self._task = task
        self._dag = dag
        self._template_parameters = {}
        self._airflow_parameters = self._get_default_args()

    @staticmethod
    def _get_default_args():
        return {
            "depends_on_past": True,
            "retries": 0,
            "retry_delay": timedelta(minutes=5),
        }

    @abstractmethod
    def _create_operator(self, kwargs):
        raise NotImplementedError

    def _update_template_with_ios(self, ios):
        for io in ios:
            self._template_parameters[io.name] = io.rendered_name

    def _update_airflow_parameters(self):
        self._airflow_parameters.update(self._task.pipeline.default_args)
        self._airflow_parameters.update(self._task.airflow_parameters)
        if self._task.pipeline.slack_alert:
            self._airflow_parameters['on_failure_callback'] = task_fail_slack_alert

        self._airflow_parameters.update({
            'description': self._task.description,
            'owner': self._task.pipeline.owner.split('@')[0]
        })

        pool = self._task.pool
        if pool:
            self._airflow_parameters['pool'] = pool

    def create_operator(self):
        self._template_parameters.update(self._task.template_parameters)
        self._update_airflow_parameters()
        self._update_template_with_ios(self._task.inputs)
        self._update_template_with_ios(self._task.outputs)

        return self._create_operator(**self._airflow_parameters)
