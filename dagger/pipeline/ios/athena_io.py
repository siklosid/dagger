from dagger.pipeline.io import IO
from dagger.utilities.config_validator import Attribute


class AthenaIO(IO):
    ref_name = "athena"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(
                    attribute_name="schema", comment="Leave it empty for system tables"
                ),
                Attribute(attribute_name="table"),
            ]
        )

    def __init__(self, io_config, config_location):
        super().__init__(io_config, config_location)

        self._schema = self.parse_attribute("schema")
        self._table = self.parse_attribute("table")

    def alias(self):
        return "athena://{}/{}".format(self._schema, self._table)

    @property
    def rendered_name(self):
        return "{}.{}".format(self._schema, self._table)

    @property
    def airflow_name(self):
        return "athena-{}-{}".format(self._schema, self._table)

    @property
    def schema(self):
        return self._schema

    @property
    def table(self):
        return self._table
