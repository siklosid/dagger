from acirc.pipeline.task import Task


class BashTask(Task):
    ref_name = "bash"

    def __init__(self, name, pipeline_name, job_config):
        super().__init__(name, pipeline_name, job_config)

