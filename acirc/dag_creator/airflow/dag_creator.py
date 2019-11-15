from acirc.graph.task_graph import Graph, TaskGraph
from acirc.pipeline.pipeline import Pipeline
from acirc import conf
from acirc.dag_creator.airflow.operator_factory import OperatorFactory

from datetime import datetime, timedelta

from airflow import DAG
from circ.utils.operator_factories import make_control_flow


class DagCreator:
    def __init__(self, task_graph: Graph):
        self._task_graph = task_graph
        self._operator_factory = OperatorFactory()

    @staticmethod
    def _get_default_args():
        return {
            "depends_on_past": True,
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
        }

    @staticmethod
    def _get_control_flow_task_id(pipe_id):
        return 'control_flow:{}'.format(pipe_id)

    def _create_dag(self, pipeline: Pipeline):
        default_args = self._get_default_args()
        default_args.update(pipeline.default_args)
        default_args['owner'] = pipeline.owner

        dag = DAG(
            pipeline.name,
            default_args=default_args,
            catchup=False,
            start_date=pipeline.start_date,
            schedule_interval=pipeline.schedule,
            **pipeline.parameters,
        )

        return dag

    def _create_dags(self):
        dags = {}
        tasks = {}
        for pipe_id, node in self._task_graph.get_nodes(TaskGraph.NODE_TYPE_PIPELINE).items():
            dag = self._create_dag(node.obj)
            dags[pipe_id] = dag

        return dags

    def _create_control_flow_tasks(self, dags):
        tasks = {}
        for pipe_id, node in self._task_graph.get_nodes(TaskGraph.NODE_TYPE_PIPELINE).items():
            control_flow_task_id = self._get_control_flow_task_id(pipe_id)
            tasks[control_flow_task_id] = make_control_flow(conf.ENV, dags[pipe_id])

        return tasks

    def _create_tasks(self, dags):
        tasks = {}
        for node_id, node in self._task_graph.get_nodes(TaskGraph.NODE_TYPE_TASK).items():
            pipeline_id = node.obj.pipeline_name
            tasks[node_id] = self._operator_factory.create_operator(node.obj, dags[pipeline_id])

        return tasks

    def _create_edge(self, from_task_id, to_task_id, tasks):
        if from_task_id is None:
            pipe_id = self._task_graph.get_node(to_task_id).obj.pipeline_name
            tasks[self._get_control_flow_task_id(pipe_id)] >> tasks[to_task_id]
        else:
            from_pipe = self._task_graph.get_node(from_task_id).obj.pipeline_name
            to_pipe = self._task_graph.get_node(to_task_id).obj.pipeline_name
            if from_pipe == to_pipe:
                tasks[from_task_id] >> tasks[to_task_id]
            else:
                tasks[self._get_control_flow_task_id(to_pipe)] >> tasks[to_task_id]

    def _create_edges(self, dags, tasks):
        for node_id, node in self._task_graph.get_nodes(TaskGraph.NODE_TYPE_DATASET).items():
            parent_task_ids = list(node.parents)
            parent_task_id = None if len(parent_task_ids) == 0 else parent_task_ids[0] # TODO: Something better
            children_ids = list(node.children)
            for task_id in children_ids:
                self._create_edge(parent_task_id, task_id, tasks)

    def create_dags(self):
        dags = self._create_dags()
        tasks = self._create_control_flow_tasks(dags)
        tasks.update(self._create_tasks(dags))
        self._create_edges(dags, tasks)

        return dags
