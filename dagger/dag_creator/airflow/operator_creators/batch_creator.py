from dagger.dag_creator.airflow.operator_creator import OperatorCreator
from dagger.dag_creator.airflow.operators.awsbatch_operator import AWSBatchOperator


class BatchCreator(OperatorCreator):
    ref_name = "batch"

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def _generate_command(self):
        command = [self._task.executable_prefix, self._task.executable]
        for param_name, param_value in self._template_parameters.items():
            command.append(
                "--{name}={value}".format(name=param_name, value=param_value)
            )

        return command

    def _create_operator(self, **kwargs):
        overrides = self._task.overrides
        overrides.update({"command": self._generate_command()})

        batch_op = AWSBatchOperator(
            dag=self._dag,
            task_id=self._task.name,
            job_name=self._task.job_name,
            absolute_job_name=self._task.absolute_job_name,
            region_name=self._task.region_name,
            cluster_name=self._task.cluster_name,
            job_queue=self._task.job_queue,
            overrides=overrides,
            **kwargs,
        )

        return batch_op
