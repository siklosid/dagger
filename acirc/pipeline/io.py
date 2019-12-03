from acirc.utilities.config_validator import ConfigValidator, Attribute
from abc import ABC, abstractmethod

from os.path import join


class IO(ConfigValidator, ABC):
    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='type', auto_value=orig_cls.ref_name),
            Attribute(attribute_name='name'),
        ])

    def __init__(self, io_config, task):
        super().__init__(config=io_config, location=join(task.pipeline.directory, task.name + '.yaml'))
        self._task = task
        self._name = self.parse_attribute('name')

    def __eq__(self, other):
        return self.alias() == other.alias()

    @abstractmethod
    def alias(self):
        pass

    @property
    def name(self):
        return self._name

    @property
    def rendered_name(self):
        raise NotImplementedError

    @property
    def airflow_name(self):
        raise NotImplementedError

    @property
    def task(self):
        return self._task
