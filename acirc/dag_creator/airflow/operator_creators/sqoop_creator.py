from acirc.dag_creator.airflow.operator_creator import OperatorCreator
from circ.operators.sqoop_operator import SqoopOperator


class SqoopCreator(OperatorCreator):
    ref_name = 'sqoop'

    def __init__(self, task, dag):
        super().__init__(task, dag)

    def _create_operator(self, **kwargs):

        properties = {
            'mapreduce.job.user.classpath.first': 'true',
        }

        batch_op = SqoopOperator(
            dag=self._dag,
            task_id=self._task.name,
            conn_id=self._task.conn_id,
            table=self._task.table,
            target_dir=self._task.target_dir,
            file_type=self._task.format,
            columns=self._task.columns,
            num_mappers=self._task.num_mappers,
            split_by=self._task.split_by,
            delete_target_dir=self._task.delete_target_dir,
            where=self._task.where,
            emr_master=self._task.emr_master,
            properties=properties,
            **kwargs,
        )

        return batch_op
