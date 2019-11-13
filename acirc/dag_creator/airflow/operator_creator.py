from abc import ABC, abstractmethod


class OperatorCreator(ABC):
    def __init__(self, task, dag):
        self._task = task
        self._dag = dag
        self._template_parameters = {}

    @abstractmethod
    def _create_operator(self):
        raise NotImplementedError

    def _update_template_with_ios(self, ios):
        for io in ios:
            self._template_parameters[io.name] = io.rendered_name

    def create_operator(self):
        self._update_template_with_ios(self._task.inputs)
        self._update_template_with_ios(self._task.outputs)

        return self._create_operator()
