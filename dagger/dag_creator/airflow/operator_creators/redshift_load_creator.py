from os.path import join
from typing import Optional

from dagger.dag_creator.airflow.operator_creator import OperatorCreator
from dagger.dag_creator.airflow.operators.redshift_sql_operator import (
    RedshiftSQLOperator,
)


class RedshiftLoadCreator(OperatorCreator):
    ref_name = "redshift_load"

    def __init__(self, task, dag):
        super().__init__(task, dag)

        self._input_path = join(self._task.inputs[0].rendered_name, "")
        self._input_s3_bucket = self._task.inputs[0].bucket
        self._input_s3_prefix = self._task.inputs[0].path

        self._output_schema = self._task.outputs[0].schema
        self._output_table = self._task.outputs[0].table
        self._output_schema_quoted = f'"{self._output_schema}"'
        self._output_table_quoted = f'"{self._output_table}"'

        self._tmp_table = (
            f"{self._task.tmp_table_prefix}_{self._output_table}"
            if self._task.tmp_table_prefix
            else None
        )
        self._tmp_table_quoted = f'"{self._tmp_table}"' if self._tmp_table else None

        self._copy_ddl_from = self._task.copy_ddl_from
        self._alter_columns = self._task.alter_columns

        self._sort_keys = self._task.sort_keys

    @staticmethod
    def _read_sql(directory, file_path):
        full_path = join(directory, file_path)

        with open(full_path, "r") as f:
            sql_string = f.read()

        return sql_string

    def _get_create_table_cmd(self) -> Optional[str]:
        if self._tmp_table and self._task.create_table_ddl:
            ddl = self._read_sql(
                self._task.pipeline.directory, self._task.create_table_ddl
            )
            return ddl.format(
                schema_name=self._output_schema_quoted,
                table_name=self._tmp_table_quoted,
            )
        if self._tmp_table and self._copy_ddl_from:
            return (
                f"CREATE TABLE {self._output_schema_quoted}.{self._tmp_table_quoted}"
                f"(LIKE {self._copy_ddl_from})"
            )
        elif self._tmp_table:
            return (
                f"CREATE TABLE {self._output_schema_quoted}.{self._tmp_table_quoted}"
                f"(LIKE {self._output_schema_quoted}.{self._output_table_quoted})"
            )
        elif self._task.create_table_ddl:
            ddl = self._read_sql(
                self._task.pipeline.directory, self._task.create_table_ddl
            )
            return ddl.format(
                schema_name=self._output_schema_quoted,
                table_name=self._output_table_quoted,
            )
        elif self._copy_ddl_from:
            return (
                f"CREATE TABLE IF NOT EXISTS {self._output_schema_quoted}.{self._output_table}"
                f"(LIKE {self._copy_ddl_from})"
            )

        return None

    def _get_sort_key_cmd(self) -> Optional[str]:
        sort_key_cmd = None
        if self._sort_keys:
            sort_key_cmd = (
                f"ALTER TABLE {self._output_schema_quoted}.{self._tmp_table_quoted} "
                f"ALTER COMPOUND SORTKEY({self._sort_keys})"
            )
        return sort_key_cmd

    def _get_delete_cmd(self) -> Optional[str]:
        if self._task.incremental:
            return (
                f"DELETE FROM {self._output_schema_quoted}.{self._output_table_quoted}"
                f"WHERE {self._task.delete_condition}"
            )

        if not self._task.incremental and self._tmp_table is None:
            return f"TRUNCATE TABLE {self._output_schema_quoted}.{self._output_table_quoted}"

        return None

    def _get_load_cmd(self) -> Optional[str]:
        table_name = self._tmp_table_quoted or self._output_table_quoted
        columns = "({})".format(self._task.columns) if self._task.columns else ""
        extra_parameters = "\n".join(
            [
                "{} {}".format(key, value)
                for key, value in self._task.extra_parameters.items()
            ]
        )

        return (
            f"copy {self._output_schema_quoted}.{table_name}{columns}\n"
            f"from '{self._input_path}'\n"
            f"iam_role '{self._task.iam_role}'\n"
            f"{extra_parameters}"
        )

    def _get_replace_table_cmd(self) -> Optional[str]:
        if self._tmp_table is None:
            return None

        return (
            f"BEGIN TRANSACTION;\n"
            f"DROP TABLE IF EXISTS {self._output_schema_quoted}.{self._output_table_quoted};\n"
            f"ALTER TABLE {self._output_schema_quoted}.{self._tmp_table_quoted} "
            f"RENAME TO {self._output_table_quoted};\n"
            f"END"
        )

    def _get_alter_columns_cmd(self) -> Optional[str]:
        if self._alter_columns is None:
            return None

        alter_column_commands = []
        alter_columns = self._alter_columns.split(",")
        for alter_column in alter_columns:
            [column_name, column_type] = alter_column.split(":")
            alter_column_commands.append(
                f"ALTER TABLE {self._output_schema_quoted}.{self._tmp_table_quoted} "
                f"ALTER COLUMN {column_name} TYPE {column_type}"
            )

        return ";\n".join(alter_column_commands)

    def _get_drop_tmp_table_cmd(self) -> Optional[str]:
        if self._tmp_table is None:
            return None

        return f"DROP TABLE IF EXISTS {self._output_schema_quoted}.{self._tmp_table_quoted}"

    def _get_cmd(self) -> str:
        raw_load_cmd = [
            self._get_drop_tmp_table_cmd(),
            self._get_create_table_cmd(),
            self._get_alter_columns_cmd(),
            self._get_sort_key_cmd(),
            self._get_delete_cmd(),
            self._get_load_cmd(),
            self._get_replace_table_cmd(),
        ]

        load_cmd = [cmd for cmd in raw_load_cmd if cmd]

        return ";\n".join(load_cmd)

    def _create_operator(self, **kwargs):
        load_cmd = self._get_cmd()

        redshift_op = RedshiftSQLOperator(
            dag=self._dag,
            task_id=self._task.name,
            sql=load_cmd,
            redshift_conn_id=self._task.postgres_conn_id,
            autocommit=True,
            **kwargs,
        )
        return redshift_op
