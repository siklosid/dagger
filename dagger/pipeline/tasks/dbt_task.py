from dagger.pipeline.tasks.batch_task import BatchTask
from dagger.utilities.config_validator import Attribute


class DbtTask(BatchTask):
    ref_name = "dbt"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="project_dir",
                    parent_fields=["task_parameters"],
                    comment="Which directory to look in for the dbt_project.yml file",
                ),
                Attribute(
                    attribute_name="profile_dir",
                    parent_fields=["task_parameters"],
                    comment="Which directory to look in for the profiles.yml file",
                ),
                Attribute(
                    attribute_name="profile_name",
                    required=False,
                    parent_fields=["task_parameters"],
                    comment="Which target to load for the given profile (--target dbt option). Default is 'default'",
                ),
                Attribute(
                    attribute_name="select",
                    required=False,
                    parent_fields=["task_parameters"],
                    comment="Specify the nodes to include (--select dbt option)",
                ),
                Attribute(
                    attribute_name="dbt_command",
                    required=False,
                    parent_fields=["task_parameters"],
                    comment="Specify the name of the DBT command to run",
                ),
            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._project_dir = self.parse_attribute("project_dir")
        self._profile_dir = self.parse_attribute("profile_dir")
        self._profile_name = self.parse_attribute("profile_name") or "default"
        self._select = self.parse_attribute("select")
        self._dbt_command = self.parse_attribute("dbt_command")

    @property
    def project_dir(self):
        return self._project_dir

    @property
    def profile_dir(self):
        return self._profile_dir

    @property
    def profile_name(self):
        return self._profile_name

    @property
    def select(self):
        return self._select

    @property
    def dbt_command(self):
        return self._dbt_command