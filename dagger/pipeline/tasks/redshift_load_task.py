from dagger.utilities.config_validator import Attribute
from dagger.pipeline.task import Task
from dagger import conf


class RedshiftLoadTask(Task):
    ref_name = "redshift_load"
    default_pool = "redshift"

    @staticmethod
    def _get_default_load_params():
        return {
            "statupdate": "on",
        }

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='iam_role', required=False, parent_fields=['task_parameters']),
            Attribute(attribute_name='columns', required=False, parent_fields=['task_parameters']),
            Attribute(attribute_name='incremental', required=True, parent_fields=['task_parameters'], validator=bool,
                      format_help="on/off/yes/no/true/false", auto_value='true'),
            Attribute(attribute_name='delete_condition', required=True, nullable=True,
                      parent_fields=['task_parameters'],
                      format_help='SQL where statement',
                      comment="Recommended when doing incremental load"),
            Attribute(attribute_name='max_errors', required=False, parent_fields=['task_parameters'],
                      comment="Default is 0"),
            Attribute(attribute_name='postgres_conn_id', required=False, parent_fields=['task_parameters']),
            Attribute(attribute_name='extra_load_parameters', required=True, nullable=True,
                      parent_fields=['task_parameters'],
                      format_help="dictionary",
                      comment="Any additional parameter will be added like <key value> \
                      Check https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html")
        ])

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._incremental = self.parse_attribute('incremental')
        self._delete_condition = self.parse_attribute('delete_condition')
        self._iam_role = self.parse_attribute('iam_role') or conf.REDSHIFT_IAM_ROLE
        self._columns = self.parse_attribute('columns')
        self._max_errors = self.parse_attribute('max_errors')
        self._postgres_conn_id = self.parse_attribute('postgres_conn_id') or conf.REDSHIFT_CONN_ID
        load_parameters = self._get_default_load_params()
        if self._max_errors:
            load_parameters['maxerrors'] = self._max_errors
        load_parameters.update(self.parse_attribute('extra_load_parameters') or {})
        self._extra_parameters = load_parameters

    @property
    def iam_role(self):
        return self._iam_role

    @property
    def columns(self):
        return self._columns

    @property
    def incremental(self):
        return self._incremental

    @property
    def delete_condition(self):
        return self._delete_condition

    @property
    def max_errors(self):
        return self._max_errors

    @property
    def postgres_conn_id(self):
        return self._postgres_conn_id

    @property
    def extra_parameters(self):
        return self._extra_parameters
