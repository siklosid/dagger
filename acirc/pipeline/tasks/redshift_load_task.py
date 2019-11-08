from pipeline.task import Task


class RedshiftLoadTask(Task):
    ref_name = "redshift_load"

    def __init__(self, name, pipeline_name, job_config):
        super().__init__(name, pipeline_name, job_config)

