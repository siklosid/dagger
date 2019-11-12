from abc import ABC, abstractmethod


class OperatorCreator(ABC):
    def __init__(self, task, dag):
        self._task = task
        self._dag = dag

    @abstractmethod
    def create_operator(self):
        raise NotImplementedError
