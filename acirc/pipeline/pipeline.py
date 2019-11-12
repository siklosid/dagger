from acirc.pipeline.task import Task
from datetime import datetime

class Pipeline:
    def __init__(self, name: str, config: dict):
        self._name = name
        self._owner = config['owner']
        self._description = config['description']
        self._default_args = config.get('default_args', {})
        self._schedule = config['schedule']
        self._start_date = datetime.strptime(config['start_date'], '%Y-%m-%dT%H:%M')
        self._parameters = config['parameters']

        self._tasks = []

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
