from acirc.pipeline.task import Task
from acirc.pipeline.tasks import (
    batch_task,
    python_task,
    redshift_load_task,
    redshift_transform_task,
    redshift_unload_task,
    spark_task,
    sqoop_task,
)


class TaskFactory:
    def __init__(self):
        self.factory = dict()

        for cls in Task.__subclasses__():
            self.factory[cls.ref_name] = cls

    def create_task(self, ref_name, task_name, pipeline_name, pipeline, task_config):
        return self.factory[ref_name](task_name, pipeline_name, pipeline, task_config)
