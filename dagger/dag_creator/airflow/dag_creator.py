import re
from datetime import timedelta, datetime
from functools import partial

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

import croniter
from dagger import conf
from dagger.alerts.alert import airflow_task_fail_alerts
from dagger.dag_creator.airflow.operator_factory import OperatorFactory
from dagger.dag_creator.airflow.utils.macros import user_defined_macros
from dagger.dag_creator.graph_traverser_base import GraphTraverserBase
from dagger.graph.task_graph import Graph, Node


# noinspection PyStatementEffect
class DagCreator(GraphTraverserBase):
    def __init__(self, task_graph: Graph, with_data_nodes: bool = conf.WITH_DATA_NODES):
        super().__init__(task_graph=task_graph, with_data_nodes=with_data_nodes)
        self._operator_factory = OperatorFactory()
        self._sensor_dict = {}

    @staticmethod
    def _get_control_flow_task_id(pipe_id):
        return "control_flow:{}".format(pipe_id)

    @staticmethod
    def _get_default_args():
        return {
            "depends_on_past": True,
            "retries": 0,
            "retry_delay": timedelta(minutes=5),
        }

    @staticmethod
    def _get_execution_date_fn(from_dag_schedule: str, to_dag_schedule: str):
        def execution_date_fn(execution_date, **kwargs):
            to_dag_cron = croniter.croniter(to_dag_schedule, execution_date)
            to_dag_next_schedule = to_dag_cron.get_next(datetime)

            from_dag_cron = croniter.croniter(from_dag_schedule, to_dag_next_schedule)
            from_dag_cron.get_next(datetime)
            # skipping one schedule
            from_dag_cron.get_prev(datetime)
            from_dag_target_schedule = from_dag_cron.get_prev(datetime)

            return from_dag_target_schedule

        return execution_date_fn

    def _get_external_task_sensor_name_dict(self, from_task_id: str) -> dict:
        from_pipeline_name = self._task_graph.get_node(from_task_id).obj.pipeline_name
        from_task_name = self._task_graph.get_node(from_task_id).obj.name
        return {
            "from_pipeline_name": from_pipeline_name,
            "from_task_name": from_task_name,
            "external_sensor_name": f"{from_pipeline_name}-{from_task_name}-sensor",
        }

    def _get_external_task_sensor(self, from_task_id: str, to_task_id: str, follow_external_dependency: dict) -> ExternalTaskSensor:
        """
        create an object of external task sensor for a specific from_task_id and to_task_id
        """
        external_task_sensor_name_dict = self._get_external_task_sensor_name_dict(from_task_id)
        external_sensor_name = external_task_sensor_name_dict["external_sensor_name"]
        from_pipeline_name = external_task_sensor_name_dict["from_pipeline_name"]
        from_task_name = external_task_sensor_name_dict["from_task_name"]

        from_pipeline_schedule = self._task_graph.get_node(from_task_id).obj.pipeline.schedule
        to_pipeline_schedule = self._task_graph.get_node(to_task_id).obj.pipeline.schedule

        to_pipe_id = self._task_graph.get_node(to_task_id).obj.pipeline.name


        extra_args = {
            'mode': conf.EXTERNAL_SENSOR_MODE,
            'poke_interval': conf.EXTERNAL_SENSOR_POKE_INTERVAL,
            'timeout': conf.EXTERNAL_SENSOR_TIMEOUT,
        }
        extra_args.update(follow_external_dependency)

        return ExternalTaskSensor(
            dag=self._dags[to_pipe_id],
            task_id=external_sensor_name,
            external_dag_id=from_pipeline_name,
            external_task_id=from_task_name,
            execution_date_fn=self._get_execution_date_fn(
                from_pipeline_schedule, to_pipeline_schedule
            ),
            **extra_args
        )

    def _create_control_flow_task(self, pipe_id, dag):
        control_flow_task_id = self._get_control_flow_task_id(pipe_id)
        self._tasks[control_flow_task_id] = self._operator_factory.create_control_flow_operator(
            conf.IS_DUMMY_OPERATOR_SHORT_CIRCUIT, dag
        )

    def _create_dag(self, pipe_id, node):
        pipeline = node.obj
        default_args = DagCreator._get_default_args()
        default_args.update(pipeline.default_args)
        default_args["owner"] = pipeline.owner.split("@")[0]
        if len(pipeline.alerts) > 0:
            default_args["on_failure_callback"] = partial(airflow_task_fail_alerts, pipeline.alerts)
        dag = DAG(
            pipeline.name,
            description=pipeline.description,
            default_args=default_args,
            start_date=pipeline.start_date,
            schedule_interval=pipeline.schedule,
            user_defined_macros=user_defined_macros,
            **pipeline.parameters,
        )

        self._create_control_flow_task(pipe_id, dag)

        return dag

    def _create_job_task(self, node):
        pipeline_id = node.obj.pipeline_name
        return self._operator_factory.create_operator(node.obj, self._dags[pipeline_id])

    def _create_data_task(self, pipe_id, node):
        if pipe_id not in self._data_tasks:
            self._data_tasks[pipe_id] = {}

        dataset_id = node.obj.airflow_name
        if dataset_id not in self._data_tasks[pipe_id]:
            self._data_tasks[pipe_id][dataset_id] = self._operator_factory.create_dataset_operator(
                re.sub("[^0-9a-zA-Z-_]+", "_", dataset_id), self._dags[pipe_id]
            )

    def _create_edge_without_data(self, from_task_id: str, to_task_ids: list, node: Node) -> None:
        """
        Creates an edge between tasks without transferring data.

        Args:
            from_task_id: The ID of the task from which the edge originates.
            to_task_ids: The IDs of the tasks to which the edge connects.
            node: The current node in a task graph.
        """
        from_pipe = (
            self._task_graph.get_node(from_task_id).obj.pipeline_name if from_task_id else None
        )
        for to_task_id in to_task_ids:
            edge_properties = self._task_graph.get_edge(node.obj.alias(), to_task_id)
            to_pipe = self._task_graph.get_node(to_task_id).obj.pipeline_name
            if from_pipe and from_pipe == to_pipe:
                self._tasks[from_task_id] >> self._tasks[to_task_id]
            elif from_pipe and from_pipe != to_pipe and edge_properties.follow_external_dependency is not None:
                from_schedule = self._task_graph.get_node(from_task_id).obj.pipeline.schedule
                to_schedule = self._task_graph.get_node(to_task_id).obj.pipeline.schedule
                if not from_schedule.startswith("@") and not to_schedule.startswith("@"):
                    external_task_sensor_name = self._get_external_task_sensor_name_dict(
                        from_task_id
                    )["external_sensor_name"]
                    if (
                        external_task_sensor_name
                        not in self._sensor_dict.get(to_pipe, dict()).keys()
                    ):
                        external_task_sensor = self._get_external_task_sensor(
                            from_task_id, to_task_id, edge_properties.follow_external_dependency
                        )
                        self._sensor_dict[to_pipe] = {
                            external_task_sensor_name: external_task_sensor
                        }
                        (
                            self._tasks[self._get_control_flow_task_id(to_pipe)]
                            >> external_task_sensor
                        )
                    self._sensor_dict[to_pipe][external_task_sensor_name] >> self._tasks[to_task_id]
            else:
                self._tasks[self._get_control_flow_task_id(to_pipe)] >> self._tasks[to_task_id]

    def _create_edge_with_data(self, from_task_id, to_task_ids, node):
        from_pipe = (
            self._task_graph.get_node(from_task_id).obj.pipeline_name if from_task_id else None
        )
        data_id = node.obj.airflow_name
        if from_pipe:
            self._tasks[from_task_id] >> self._data_tasks[from_pipe][data_id]
        for to_task_id in to_task_ids:
            to_pipe = self._task_graph.get_node(to_task_id).obj.pipeline_name
            self._data_tasks[to_pipe][data_id] >> self._tasks[to_task_id]
            if not from_pipe or (from_pipe != to_pipe):
                (
                    self._tasks[self._get_control_flow_task_id(to_pipe)]
                    >> self._data_tasks[to_pipe][data_id]
                )
