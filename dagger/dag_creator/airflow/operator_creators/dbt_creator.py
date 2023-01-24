import base64

from dagger.dag_creator.airflow.operator_creators.batch_creator import BatchCreator
import json


class DbtCreator(BatchCreator):
    ref_name = "dbt"

    def __init__(self, task, dag):
        super().__init__(task, dag)

        self._project_dir = task.project_dir
        self._profile_dir = task.profile_dir
        self._profile_name = task.profile_name
        self._select = task.select

    def _generate_command(self):
        command = [self._task.executable_prefix, self._task.executable]
        command.append(f"--project_dir={self._project_dir}")
        command.append(f"--profiles_dir={self._profile_dir}")
        command.append(f"--profile_name={self._profile_name}")
        if self._select:
            command.append(f"--select={self._select}")

        if len(self._template_parameters) > 0:
            dbt_vars = json.dumps(self._template_parameters)
            command.append(f"--vars='{dbt_vars}'")

        return command

    # Overwriting function because for dbt we don't want to add inputs/outputs to the template parameters
    def create_operator(self):
        self._template_parameters.update(self._task.template_parameters)
        self._update_airflow_parameters()

        return self._create_operator(**self._airflow_parameters)
