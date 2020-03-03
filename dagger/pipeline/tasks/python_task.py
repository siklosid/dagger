from dagger.utilities.config_validator import Attribute
from dagger.pipeline.task import Task


class PythonTask(Task):
    ref_name = "python"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='python', parent_fields=['task_parameters'],
                      comment="Relative path to python file that implements the function"),
            Attribute(attribute_name='function', parent_fields=['task_parameters'],
                      comment="Name of the function"),
        ])

    def __init__(self, name, pipeline_name, pipeline, job_config):
        super().__init__(name, pipeline_name, pipeline, job_config)

        self._python = self.parse_attribute('python')
        self._function = self.parse_attribute('function')

    @property
    def python(self):
        return self._python

    @property
    def function(self):
        return self._function
