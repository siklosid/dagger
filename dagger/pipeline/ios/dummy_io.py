from dagger.pipeline.io import IO


class DummyIO(IO):
    ref_name = "dummy"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([])

    def __init__(self, io_config, config_location):
        super().__init__(io_config, config_location)

    def alias(self):
        return "dummy://{}".format(self._name)

    @property
    def rendered_name(self):
        return "{}".format(self._name)

    @property
    def airflow_name(self):
        return "dummy-{}".format(self._name)
