from pipeline.task import Task


class RedshiftTransformTask(Task):
    ref_name = "redshift_transform"

    def __init__(self, name, pipeline_name, job_config):
        super().__init__(name, pipeline_name, job_config)

