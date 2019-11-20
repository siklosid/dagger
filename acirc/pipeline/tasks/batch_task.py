from acirc.pipeline.task import Task


class BatchTask(Task):
    ref_name = "batch"

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._executable = job_config['task_parameters']['executable']
        self._executable_prefix = job_config['task_parameters']['executable_prefix'] or ""
        job_name = "{}-{}".format(pipeline.name, job_config['task_parameters']['job_name'])
        self._job_name = job_name
        self._aws_conn_id = job_config['task_parameters']['aws_conn_id']
        self._region_name = job_config['task_parameters']['region_name'] or 'eu-central-1'
        self._job_queue = job_config['task_parameters']['job_queue'] or 'airflow-prio1'
        self._max_retries = job_config['task_parameters']['max_retries'] or 4200

    @property
    def executable(self):
        return self._executable

    @property
    def executable_prefix(self):
        return self._executable_prefix

    @property
    def job_name(self):
        return self._job_name

    @property
    def aws_conn_id(self):
        return self._aws_conn_id

    @property
    def region_name(self):
        return self._region_name

    @property
    def job_queue(self):
        return self._job_queue

    @property
    def max_retries(self):
        return self._max_retries
