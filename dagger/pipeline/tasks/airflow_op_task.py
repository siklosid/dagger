from dagger.pipeline.task import Task
from dagger.utilities.config_validator import Attribute


class AirflowOpTask(Task):
    ref_name = "airflow_operator"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="module",
                    parent_fields=["task_parameters"],
                    comment="Name of the module to import from. E.g.: airflow.opeartors.python_operator",
                ),
                Attribute(
                    attribute_name="class_name",
                    parent_fields=["task_parameters"],
                    comment="Name of the operator class. E.g.: PythonBranchOperator",
                ),
                Attribute(
                    attribute_name="python",
                    parent_fields=["task_parameters"],
                    required=False,
                    comment="Relative path to python file that implements the function",
                ),
                Attribute(
                    attribute_name="function",
                    parent_fields=["task_parameters"],
                    required=False,
                    comment="Name of the function",
                ),
            ]
        )

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._module = self.parse_attribute("module")
        self._class_name = self.parse_attribute("class_name")
        self._python = self.parse_attribute("python")
        self._function = self.parse_attribute("function")

    @property
    def module(self):
        return self._module

    @property
    def class_name(self):
        return self._class_name

    @property
    def python(self):
        return self._python

    @property
    def function(self):
        return self._function
