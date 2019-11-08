from pipeline.task import Task


class Pipeline:
    def __init__(self, name: str, config: dict):
        self._name = name
        self._owner = config['owner']
        self._description = config['description']
        self._schedule = config['schedule']
        self._parameters = config['parameters']

        self._tasks = []

    @property
    def name(self):
        return self._name

    @property
    def tasks(self):
        return self._tasks

    def add_task(self, task: Task):
        self._tasks.append(task)
