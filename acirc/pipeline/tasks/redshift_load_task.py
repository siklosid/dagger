from acirc.utilities.config_validator import Attribute
from acirc.pipeline.task import Task

DEFAULT_IAM_ROLE = "arn:aws:iam::120444018371:role/redshift"


class RedshiftLoadTask(Task):
    ref_name = "redshift_load"

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
            Attribute(attribute_name='max_errors', required=False, parent_fields=['task_parameters'],
                      comment="Default is 0"),
            Attribute(attribute_name='extra_load_parameters', required=False, parent_fields=['task_parameters'],
                      format_help="dictionary",
                      comment="Any additional parameter will be added like <key value> \
                      Check https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html")
        ])

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._iam_role = self.parse_attribute('iam_role') or DEFAULT_IAM_ROLE
        self._columns = self.parse_attribute('columns')
        self._max_errors = self.parse_attribute('max_errors')
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
    def max_errors(self):
        return self._max_errors

    @property
    def extra_parameters(self):
        return self._extra_parameters
