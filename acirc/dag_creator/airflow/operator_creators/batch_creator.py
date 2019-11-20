from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from circ.operators.awsbatch_operator import AWSBatchOperator


class BatchCreator(OperatorCreator):
    ref_name = 'batch'

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def _generate_command(self):
        command = []
        command += [self._task.executable_prefix, self._task.executable]
        for param_name, param_value in self._template_parameters.items():
            command.append("--{name}={value}".format(name=param_name, value=param_value))

        return command

    def _create_operator(self):
        overrides = {
            "command": self._generate_command()
        }

        batch_op = AWSBatchOperator(
            task_id=self._task.name,
            job_name=self._task.job_name,
            region_name=self._task.region_name,
            job_queue=self._task.job_queue,
            overrides=overrides,
            **self._task.airflow_parameters,
        )

        return batch_op
