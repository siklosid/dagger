from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.utils.decorators import apply_defaults
from dagger.dag_creator.airflow.operators.dagger_base_operator import DaggerBaseOperator


class SnowflakeOperator(DaggerBaseOperator):
    """
    Executes sql code in a Snowflake database

    :param snowflake_conn_id: reference to specific snowflake connection id
    :type snowflake_conn_id: str
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :type warehouse: str
    :param database: name of database (will overwrite database defined
        in connection)
    :type database: str
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :type schema: str
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    """

    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#ededed"

    @apply_defaults
    def __init__(
        self,
        sql,
        snowflake_conn_id="snowflake_default",
        parameters=None,
        autocommit=True,
        warehouse=None,
        database=None,
        role=None,
        schema=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema

    @property
    def statements(self):
        self.sql = self._ignore_oneline_comments(self.sql)
        statements = self.sql.strip().split(";")
        assert statements != [], "No valid query was found"
        if statements is None:
            return [self.sql]
        return [s for s in statements if s != "" and s != "\n"]

    def get_hook(self):
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
        )

    def execute(self, context):
        hook = self.get_hook()
        for statement in self.statements:
            hook.run(statement, autocommit=self.autocommit, parameters=self.parameters)

    def _ignore_oneline_comments(self, sql):
        lines = []
        for line in sql.split("\n"):
            line = line.strip()
            if line.startswith("--") or line.startswith("#"):
                continue
            lines.append(line)
        return "\n".join(lines)
