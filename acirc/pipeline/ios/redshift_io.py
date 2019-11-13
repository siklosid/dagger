from acirc.pipeline.io import IO


class RedshiftIO(IO):
    ref_name = "redshift"

    def __init__(self, io_config):
        super().__init__(io_config)

        self._database = io_config.get('database', 'production')
        self._schema = io_config['schema']
        self._table = io_config['table']

    def alias(self):
        return "redshift://{database}/{schema}/{table}"\
            .format(
                database=self._database,
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
