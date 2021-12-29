from abc import ABC, abstractmethod
from os.path import join

from dagger.utilities.config_validator import Attribute, ConfigValidator


class IO(ConfigValidator, ABC):
    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(attribute_name="type", auto_value=orig_cls.ref_name),
                Attribute(attribute_name="name"),
                Attribute(
                    attribute_name="has_dependency",
                    required=False,
                    comment="Weather this i/o should be added to the dependency graph or not. Default is True",
                ),
                Attribute(
                    attribute_name="follow_external_dependency",
                    required=False,
                    comment="Weather an external task sensor should be created if this dataset"
                            "is created in another pipeline. Default is False",
                ),
            ]
        )

    def __init__(self, io_config, config_location):
        super().__init__(
            config=io_config,
            location=config_location
        )
        self._name = self.parse_attribute("name")
        self._has_dependency = self.parse_attribute("has_dependency")
        if self._has_dependency is None:
            self._has_dependency = True
        self._follow_external_dependency = self.parse_attribute("follow_external_dependency") or False

    def __eq__(self, other):
        return self.alias() == other.alias()

    def update(self, other_io):
        self._has_dependency = self._has_dependency or other_io.has_dependency
        self._follow_external_dependency = self._follow_external_dependency or other_io.follow_external_dependency

    @abstractmethod
    def alias(self):
        pass

    @property
    def name(self):
        return self._name

    @property
    def has_dependency(self):
        return self._has_dependency

    @property
    def follow_external_dependency(self):
        return self._follow_external_dependency

    @property
    def rendered_name(self):
        raise NotImplementedError

    @property
    def airflow_name(self):
        raise NotImplementedError

    @property
    def task(self):
        return self._task
