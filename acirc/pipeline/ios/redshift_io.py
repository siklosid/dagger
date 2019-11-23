from acirc.utilities.config_validator import Attribute
from acirc.pipeline.io import IO


class RedshiftIO(IO):
    ref_name = "redshift"

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes([
            Attribute(attribute_name='schema'),
            Attribute(attribute_name='table'),
        ])

    def __init__(self, io_config, task):
        super().__init__(io_config, task)

        self._schema = self.parse_attribute('schema')
        self._table = self.parse_attribute('table')

    def alias(self):
        return "redshift://{schema}/{table}"\
            .format(
                schema=self._schema,
                table=self._table
            )

    @property
    def rendered_name(self):
        return "{schema}.{table}"\
            .format(
                schema=self._schema,
                table=self._table
            )
