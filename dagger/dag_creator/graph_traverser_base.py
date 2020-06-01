from abc import ABC, abstractmethod

from dagger import conf
from dagger.graph.task_graph import Graph, TaskGraph
from dagger.pipeline.pipeline import Pipeline


class GraphTraverserBase(ABC):
    def __init__(self, task_graph: Graph, with_data_nodes: bool = conf.WITH_DATA_NODES):
        self._task_graph = task_graph
        self._with_data_nodes = with_data_nodes

        self._dags = {}
        self._tasks = {}
        self._data_tasks = {}

    @abstractmethod
    def _create_dag(self, pipe_id, node):
        raise NotImplementedError

    def _create_dags(self):
        for pipe_id, node in self._task_graph.get_nodes(
            TaskGraph.NODE_TYPE_PIPELINE
        ).items():
            self._dags[pipe_id] = self._create_dag(pipe_id, node)

    @abstractmethod
    def _create_job_task(self, node):
        raise NotImplementedError

    def _create_job_tasks(self):
        for node_id, node in self._task_graph.get_nodes(
            TaskGraph.NODE_TYPE_TASK
        ).items():
            self._tasks[node_id] = self._create_job_task(node)

    @abstractmethod
    def _create_data_task(self, pipe_id, node):
        raise NotImplementedError

    def _create_data_tasks(self):
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
                self._create_data_task(from_pipe, node)

            for children_id in children_ids:
                to_pipe = self._task_graph.get_node(children_id).obj.pipeline_name
                self._create_data_task(to_pipe, node)

    @abstractmethod
    def _create_edge_without_data(self, from_task_id, to_task_ids):
        raise NotImplementedError

    def _create_edge_with_data(self, from_task_id, to_task_ids, node):
        raise NotImplementedError

    def _create_edges(self):
        for node_id, node in self._task_graph.get_nodes(
            TaskGraph.NODE_TYPE_DATASET
        ).items():
            parent_task_ids = list(node.parents)
            parent_task_id = (
                None if len(parent_task_ids) == 0 else parent_task_ids[0]
            )  # TODO: Something better
            children_ids = list(node.children)

            if self._with_data_nodes:
                self._create_edge_with_data(parent_task_id, children_ids, node)
            else:
                self._create_edge_without_data(parent_task_id, children_ids)

    def _finish_dag_creation(self):
        pass

    def traverse_graph(self):
        self._create_dags()
        self._create_job_tasks()
        if self._with_data_nodes:
            self._create_data_tasks()
        self._create_edges()
        self._finish_dag_creation()

        return self._dags
