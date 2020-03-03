from dagger import conf
from dagger.pipeline.task import Task
from dagger.utilities.config_validator import Attribute


class RedshiftUnloadTask(Task):
    ref_name = "redshift_unload"
    default_pool = "redshift"

    @staticmethod
    def _get_default_unload_params():
        return {"maxfilesize": "128 mb"}

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="sql",
                    nullable=True,
                    parent_fields=["task_parameters"],
                    comment="Relative path to sql file. If not present default is SELECT * FROM <input_table>",
                ),
                Attribute(
                    attribute_name="iam_role",
                    required=False,
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="allow_overwrite",
                    required=False,
                    parent_fields=["task_parameters"],
                    format_help="on/off",
                    comment="Default is on",
                ),
                Attribute(
                    attribute_name="postgres_conn_id",
                    required=False,
                    parent_fields=["task_parameters"],
                ),
                Attribute(
                    attribute_name="extra_unload_parameters",
                    required=True,
                    nullable=True,
                    parent_fields=["task_parameters"],
                    format_help="dictionary",
                    comment="Any additional parameter will be added like <key value> \
                          Check https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html#unload-parameters",
                ),
            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._sql_file = self.parse_attribute("sql")
        self._iam_role = self.parse_attribute("iam_role") or conf.REDSHIFT_IAM_ROLE
        self._allow_overwrite = self.parse_attribute("allow_overwrite") or True
        self._postgres_conn_id = (
            self.parse_attribute("postgres_conn_id") or conf.REDSHIFT_CONN_ID
        )
        unload_parameters = self._get_default_unload_params()
        if self._allow_overwrite:
            unload_parameters["allowoverwrite"] = ""
        unload_parameters.update(self.parse_attribute("extra_unload_parameters") or {})
        self._extra_parameters = unload_parameters

    @property
    def sql_file(self):
        return self._sql_file

    @property
    def iam_role(self):
        return self._iam_role

    @property
    def postgres_conn_id(self):
        return self._postgres_conn_id

    @property
    def extra_parameters(self):
        return self._extra_parameters
