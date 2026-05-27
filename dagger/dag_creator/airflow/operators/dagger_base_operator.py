from airflow.models import BaseOperator


class DaggerBaseOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        pass
