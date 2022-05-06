from abc import ABC, abstractmethod
from datetime import timedelta

TIMEDELTA_PARAMETERS = ['execution_timeout']


class OperatorCreator(ABC):
    def __init__(self, task, dag):
        self._task = task
        self._dag = dag
        self._template_parameters = {}
        self._airflow_parameters = {}

    @abstractmethod
    def _create_operator(self, kwargs):
        raise NotImplementedError

    def _update_template_with_ios(self, ios):
        for io in ios:
            self._template_parameters[io.name] = io.rendered_name

    def _fix_timedelta_parameters(self):
        for timedelta_parameter in TIMEDELTA_PARAMETERS:
            if self._airflow_parameters.get(timedelta_parameter) is not None:
                self._airflow_parameters[timedelta_parameter] =\
                    timedelta(seconds=self._airflow_parameters[timedelta_parameter])

    def _update_airflow_parameters(self):
        self._airflow_parameters.update(self._task.airflow_parameters)

        self._airflow_parameters.update({"description": self._task.description})

        if self._task.pool:
            self._airflow_parameters["pool"] = self._task.pool

        if self._task.timeout_in_seconds:
            self._airflow_parameters["execution_timeout"] = self._task.timeout_in_seconds

        self._fix_timedelta_parameters()

    def create_operator(self):
        self._template_parameters.update(self._task.template_parameters)
        self._update_airflow_parameters()
        self._update_template_with_ios(self._task.inputs)
        self._update_template_with_ios(self._task.outputs)

        return self._create_operator(**self._airflow_parameters)
