from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DaggerBaseOperator(BaseOperator):
    @apply_defaults
    def __init__(self, description=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._description = description

    @property
    def description(self):
        return self._description

    def execute(self, context):
        pass
