from acirc.utilities.config_validator import ConfigValidator, Attribute
from abc import ABC, abstractmethod


class IO(ABC, ConfigValidator):
    s_attributes = []
    @classmethod
    def init_attributes_once(cls):
        if len(IO.s_attributes) > 0:
            return

        IO.init_attributes()

    @staticmethod
    def init_attributes():
        IO.s_attributes = [
            Attribute(attribute_name='type'),
            Attribute(attribute_name='name'),
        ]

    def __init__(self, io_config):
        IO.init_attributes_once()
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
