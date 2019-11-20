from acirc.pipeline.task import Task


class RedshiftTransformTask(Task):
    ref_name = "redshift_transform"

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._sql_file = job_config['sql']

    @property
    def sql_file(self):
        return self._sql_file
