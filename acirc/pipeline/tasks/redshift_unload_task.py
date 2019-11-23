from acirc.utilities.config_validator import Attribute
from acirc.pipeline.task import Task

DEFAULT_IAM_ROLE = "arn:aws:iam::120444018371:role/redshift"


class RedshiftUnloadTask(Task):
    ref_name = "redshift_unload"

    @staticmethod
    def _get_default_unload_params():
        return {
            "allowoverwrite": "",
            "maxfilesize": "128 mb"
        }

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='sql', required=False, parent_fields=['task_parameters'],
                      comment="Relative path to sql file. If not present default is SELECT * FROM <input_table>"),
            Attribute(attribute_name='iam_role', required=False, parent_fields=['task_parameters']),
            Attribute(attribute_name='extra_unload_parameters', required=False, parent_fields=['task_parameters'],
                      format_help="dictionary",
                      comment="Any additional parameter will be added like <key value> \
                      Check https://docs.aws.amazon.com/redshift/latest/dg/t_Unloading_tables.html")
        ])

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._sql_file = self.parse_attribute('sql')
        self._iam_role = self.parse_attribute('iam_role') or DEFAULT_IAM_ROLE
        unload_parameters = self._get_default_unload_params()
        unload_parameters.update(self.parse_attribute('extra_unload_parameters') or {})
        self._extra_parameters = unload_parameters

    @property
    def sql_file(self):
        return self._sql_file

    @property
    def iam_role(self):
        return self._iam_role

    @property
    def extra_parameters(self):
        return self._extra_parameters
