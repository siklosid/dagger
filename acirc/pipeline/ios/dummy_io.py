from acirc.utilities.config_validator import Attribute
from acirc.pipeline.io import IO


class DummyIO(IO):
    ref_name = "dummy"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
        ])

    def __init__(self, io_config, task):
        super().__init__(io_config, task)

    def alias(self):
        return "dummy://{}".format(self._name)

    @property
    def rendered_name(self):
        return "{}".format(self._name)

    @property
    def airflow_name(self):
        return "dummy-{}".format(self._name)
