from dagger.pipeline.task import Task
from dagger.pipeline.tasks import (
    airflow_op_task,
    athena_transform_task,
    batch_task,
    dbt_task,
    dummy_task,
    python_task,
    redshift_load_task,
    redshift_transform_task,
    redshift_unload_task,
    spark_task,
    sqoop_task,
)
from dagger.utilities.classes import get_deep_obj_subclasses


class TaskFactory:
    def __init__(self):
        self.factory = dict()

        for cls in get_deep_obj_subclasses(Task):
            self.factory[cls.ref_name] = cls

    def create_task(self, ref_name, task_name, pipeline_name, pipeline, task_config):
        return self.factory[ref_name](task_name, pipeline_name, pipeline, task_config)
