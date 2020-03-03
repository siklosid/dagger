from dagger.pipeline.task import Task


class BashTask(Task):
    ref_name = "bash"

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)
