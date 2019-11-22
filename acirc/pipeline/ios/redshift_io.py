from acirc.utilities.config_validator import Attribute
from acirc.pipeline.io import IO


class RedshiftIO(IO):
    ref_name = "redshift"

    s_attributes = []
    @classmethod
    def init_attributes_once(cls):
        if len(RedshiftIO.s_attributes) > 0:
            return

        IO.init_attributes_once()
        RedshiftIO.init_attributes()
        RedshiftIO.s_attributes = IO.s_attributes + RedshiftIO.s_attributes

    @staticmethod
    def init_attributes():
        RedshiftIO.s_attributes = [
            Attribute(attribute_name='schema'),
            Attribute(attribute_name='table'),
        ]

    def __init__(self, io_config):
        RedshiftIO.init_attributes_once()
        super().__init__(io_config)

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
