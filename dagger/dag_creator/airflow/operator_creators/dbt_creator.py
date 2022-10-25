from dagger.dag_creator.airflow.operator_creator import OperatorCreator
from airflow.operators.bash import BashOperator
import json

class DbtCreator(OperatorCreator):
    ref_name = "dbt"

    def __init__(self, task, dag):
        super().__init__(task, dag)

        self._project_dir = task.project_dir
        self._profile_dir = task.profile_dir
        self._profile_name = task.profile_name
        self._select = task.select

    def _generate_deps_command(self):
        command = [
            "dbt deps",
            f"--project-dir {self._project_dir}",
            f"--profiles-dir {self._profile_dir}",
        ]

        return " ".join(command)

    def _generate_build_command(self):
        command = [
            "dbt build",
            f"--project-dir {self._project_dir}",
            f"--profiles-dir {self._profile_dir}",
            f"--target {self._profile_name}",
        ]

        if self._select:
            command += [f"--select {self._select}"]

        if len(self._template_parameters) > 0:
            dbt_vars = json.dumps(self._template_parameters)
            command += [f"--vars '{dbt_vars}'"]

        return " ".join(command)

    def _create_operator(self, **kwargs):
        full_bash_command = f"{self._generate_deps_command()}; {self._generate_build_command()}"

        dbt_op = BashOperator(
            dag=self._dag,
            task_id=self._task.name,
            bash_command=full_bash_command,
            **kwargs
        )

        return dbt_op

    # Overwriting function because for dbt we don't want to add inputs/outputs to the template parameters
    def create_operator(self):
        self._template_parameters.update(self._task.template_parameters)
        self._update_airflow_parameters()

        return self._create_operator(**self._airflow_parameters)