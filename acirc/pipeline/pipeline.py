from acirc.pipeline.task import Task
from datetime import datetime
from acirc import conf

from os.path import (
    relpath,
)


class Pipeline:
    def __init__(self, directory: str, config: dict):
        self._directory = directory
        self._name = relpath(directory, conf.DAGS_DIR).replace('/', '-')
        self._owner = config['owner']
        self._description = config['description']
        default_args = config['airflow_parameters']['default_args']
        self._default_args = default_args if default_args else {}
        self._schedule = config['schedule']
        self._start_date = datetime.strptime(config['start_date'], '%Y-%m-%dT%H:%M')
        dag_parameters = config['airflow_parameters']['dag_parameters']
        self._parameters = dag_parameters if dag_parameters else {}

        self._tasks = []

    @property
    def directory(self):
        return self._directory

    @property
    def name(self):
        return self._name

    @property
    def owner(self):
        return self._owner

    @property
    def schedule(self):
        return self._schedule

    @property
    def start_date(self):
        return self._start_date

    @property
    def default_args(self):
        return self._default_args

    @property
    def parameters(self):
        return self._parameters

    @property
    def tasks(self):
        return self._tasks

    def add_task(self, task: Task):
        self._tasks.append(task)
