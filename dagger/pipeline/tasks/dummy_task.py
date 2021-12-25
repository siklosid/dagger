from dagger.pipeline.task import Task


class DummyTask(Task):
    ref_name = "dummy"

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)
