from acirc.utilities.config_validator import ConfigValidator, Attribute
from abc import ABC, abstractmethod


class IO(ConfigValidator, ABC):
    @classmethod
    def init_attributes(cls):
        cls.add_config_attributes([
            Attribute(attribute_name='type'),
            Attribute(attribute_name='name'),
        ])

    def __init__(self, io_config):
        super().__init__(config=io_config, location="")
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
