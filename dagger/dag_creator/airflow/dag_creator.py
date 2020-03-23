import re
from datetime import timedelta
from functools import partial

from airflow import DAG
from dagger import conf
from dagger.alerts.alert import airflow_task_fail_alerts
from dagger.dag_creator.airflow.operator_factory import OperatorFactory
from dagger.dag_creator.airflow.utils.macros import user_defined_macros
from dagger.graph.task_graph import Graph, TaskGraph
from dagger.pipeline.pipeline import Pipeline


class DagCreator:
    def __init__(self, task_graph: Graph):
        self._task_graph = task_graph
        self._operator_factory = OperatorFactory()

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
    def _create_dag(pipeline: Pipeline):
        default_args = DagCreator._get_default_args()
        default_args.update(pipeline.default_args)
        default_args["owner"] = pipeline.owner.split("@")[0]
        if len(pipeline.alerts) > 0:
            default_args["on_failure_callback"] = partial(
                airflow_task_fail_alerts, pipeline.alerts
            )

        dag = DAG(
            pipeline.name,
            description=pipeline.description,
            default_args=default_args,
            catchup=False,
            start_date=pipeline.start_date,
            schedule_interval=pipeline.schedule,
            user_defined_macros=user_defined_macros,
            **pipeline.parameters,
        )

        return dag

    def _create_dags(self):
        dags = {}
        for pipe_id, node in self._task_graph.get_nodes(
            TaskGraph.NODE_TYPE_PIPELINE
        ).items():
            dag = self._create_dag(node.obj)
            dags[pipe_id] = dag

        return dags

    def _create_control_flow_tasks(self, dags):
        tasks = {}
        for pipe_id, node in self._task_graph.get_nodes(
            TaskGraph.NODE_TYPE_PIPELINE
        ).items():
            control_flow_task_id = self._get_control_flow_task_id(pipe_id)
            tasks[
                control_flow_task_id
            ] = self._operator_factory.create_control_flow_operator(
                conf.ENV, dags[pipe_id]
            )

        return tasks

    def _create_job_tasks(self, dags):
        tasks = {}
        for node_id, node in self._task_graph.get_nodes(
            TaskGraph.NODE_TYPE_TASK
        ).items():
            pipeline_id = node.obj.pipeline_name
            tasks[node_id] = self._operator_factory.create_operator(
                node.obj, dags[pipeline_id]
            )

        return tasks

    def _create_data_tasks(self, dags):
        data_tasks = {}

        def __add_to_data_tasks(pipe_id, dataset_id):
            if pipe_id not in data_tasks:
                data_tasks[pipe_id] = {}

            if dataset_id not in data_tasks[pipe_id]:
                data_tasks[pipe_id][
                    dataset_id
                ] = self._operator_factory.create_dataset_operator(
                    re.sub("[^0-9a-zA-Z\-_]+", "_", dataset_id), dags[pipe_id]
                )

        for node_id, node in self._task_graph.get_nodes(
            TaskGraph.NODE_TYPE_DATASET
        ).items():
            parent_task_ids = list(node.parents)
            parent_task_id = (
                None if len(parent_task_ids) == 0 else parent_task_ids[0]
            )  # TODO: Something better
            children_ids = list(node.children)

            if parent_task_id:
                from_pipe = self._task_graph.get_node(parent_task_id).obj.pipeline_name
                __add_to_data_tasks(from_pipe, node.obj.airflow_name)

            for children_id in children_ids:
                to_pipe = self._task_graph.get_node(children_id).obj.pipeline_name
                __add_to_data_tasks(to_pipe, node.obj.airflow_name)

        return data_tasks

    def _create_edge_without_data(self, from_task_id, to_task_ids, tasks):
        from_pipe = (
            self._task_graph.get_node(from_task_id).obj.pipeline_name
            if from_task_id
            else None
        )
        for to_task_id in to_task_ids:
            to_pipe = self._task_graph.get_node(to_task_id).obj.pipeline_name
            if not from_pipe or (from_pipe != to_pipe):
                tasks[self._get_control_flow_task_id(to_pipe)] >> tasks[to_task_id]
            else:
                tasks[from_task_id] >> tasks[to_task_id]

    def _create_edge_with_data(
        self, from_task_id, to_task_ids, data_id, tasks, data_tasks
    ):
        from_pipe = (
            self._task_graph.get_node(from_task_id).obj.pipeline_name
            if from_task_id
            else None
        )
        if from_pipe:
            tasks[from_task_id] >> data_tasks[from_pipe][data_id]
        for to_task_id in to_task_ids:
            to_pipe = self._task_graph.get_node(to_task_id).obj.pipeline_name
            data_tasks[to_pipe][data_id] >> tasks[to_task_id]
            if not from_pipe or (from_pipe != to_pipe):
                tasks[self._get_control_flow_task_id(to_pipe)] >> data_tasks[to_pipe][
                    data_id
                ]

    def _create_edges(self, tasks, data_tasks):
        for node_id, node in self._task_graph.get_nodes(
            TaskGraph.NODE_TYPE_DATASET
        ).items():
            parent_task_ids = list(node.parents)
            parent_task_id = (
                None if len(parent_task_ids) == 0 else parent_task_ids[0]
            )  # TODO: Something better
            children_ids = list(node.children)

            if conf.WITH_DATA_NODES:
                self._create_edge_with_data(
                    parent_task_id,
                    children_ids,
                    node.obj.airflow_name,
                    tasks,
                    data_tasks,
                )
            else:
                self._create_edge_without_data(parent_task_id, children_ids, tasks)

    def create_dags(self):
        dags = self._create_dags()
        tasks = self._create_control_flow_tasks(dags)
        tasks.update(self._create_job_tasks(dags))
        data_tasks = None
        if conf.WITH_DATA_NODES:
            data_tasks = self._create_data_tasks(dags)
        self._create_edges(tasks, data_tasks)

        return dags
