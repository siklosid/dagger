from dagger.pipeline.task import Task
from dagger.utilities.config_validator import Attribute


class DbtTask(Task):
    ref_name = "dbt"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="project_dir",
                    parent_fields=["task_parameters"],
                    comment="Path to the dbt project directory",
                ),
                Attribute(
                    attribute_name="profile_dir",
                    parent_fields=["task_parameters"],
                    comment="Path to the dbt profile dir",
                ),
                Attribute(
                    attribute_name="profile_name",
                    required=False,
                    parent_fields=["task_parameters"],
                    comment="Dbt profile name passed into dbt --target, default is 'default'",
                ),
                Attribute(
                    attribute_name="select",
                    required=False,
                    parent_fields=["task_parameters"],
                    comment="Passed into dbt --select",
                ),
            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._project_dir = self.parse_attribute("project_dir")
        self._profile_dir = self.parse_attribute("profile_dir")
        self._profile_name = self.parse_attribute("profile_name") or 'default'
        self._select = self.parse_attribute("select")

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
