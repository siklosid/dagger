from acirc.utilities.config_validator import Attribute
from acirc.pipeline.task import Task
from acirc import conf

from os.path import join, relpath


class SparkTask(Task):
    ref_name = "spark"
    default_pool = "spark"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='spark_engine', parent_fields=['task_parameters'], required=False,
                      comment="Where to run spark job. Accepted values: emr/batch"),
            Attribute(attribute_name='job_file', parent_fields=['task_parameters']),
            Attribute(attribute_name='spark_args', parent_fields=['task_parameters'],
                      required=False, format_help='Dictionary'),
            Attribute(attribute_name='s3_files_bucket', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='extra_py_files', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='emr_master', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='aws_conn_id', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='region_name', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='job_queue', parent_fields=['task_parameters'], required=False),
            Attribute(attribute_name='max_retries', parent_fields=['task_parameters'], required=False)
        ])

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._spark_engine = self.parse_attribute('spark_engine') or conf.SPARK_DEFAULT_ENGINE
        self._job_file = relpath(join(self.pipeline.directory, self.parse_attribute('job_file')), conf.DAGS_DIR)
        spark_args = self.parse_attribute('spark_args') or {}
        self._spark_args = self._get_default_spark_args()
        print('XXX', self._spark_args, spark_args)
        self._spark_args.update(spark_args)
        self._s3_files_bucket = self.parse_attribute('s3_files_bucket') or conf.SPARK_S3_FILES_BUCKET
        self._extra_py_files = self.parse_attribute('extra_py_files') or []
        self._emr_master = self.parse_attribute('emr_master') or conf.SPARK_EMR_MASTER
        self._aws_conn_id = self.parse_attribute('aws_conn_id')
        self._region_name = self.parse_attribute('region_name') or 'eu-central-1'
        self._job_queue = self.parse_attribute('job_queue') or 'airflow-prio1'
        self._max_retries = self.parse_attribute('max_retries') or 4200

    @property
    def spark_engine(self):
        return self._spark_engine

    @property
    def job_file(self):
        return self._job_file

    @property
    def spark_args(self):
        return self._spark_args

    @property
    def s3_files_bucket(self):
        return self._s3_files_bucket

    @property
    def extra_py_files(self):
        return self._extra_py_files

    @property
    def emr_master(self):
        return self._emr_master

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


    @staticmethod
    def _get_default_spark_args():
        return {
            'conf spark.driver.memory': '512m',
            'conf spark.executor.memory': '512m',
            'conf spark.cores.max': '1',
            'conf spark.scheduler.pool': '{}'.format(conf.ENV),
        }
