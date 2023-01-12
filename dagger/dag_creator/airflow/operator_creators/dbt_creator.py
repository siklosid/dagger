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
        command = super()._generate_command()

        if len(self._template_parameters) > 0:
            dbt_vars = json.dumps(self._template_parameters)
            command.append(f"--vars='{dbt_vars}'")

        return command
