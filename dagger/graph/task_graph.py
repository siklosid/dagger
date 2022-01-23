import logging
import sys
from abc import ABC

import dagger.pipeline.pipeline
from dagger.pipeline.io import IO
from dagger.pipeline.task import Task
from dagger.utilities.exceptions import IdAlreadyExistsException
from dagger.conf import config

_logger = logging.getLogger("graph")


class Node(ABC):
    def __init__(self, node_id: str, name_to_show: str, obj=None):
        self._node_id = node_id
        self._name = name_to_show if name_to_show else node_id
        self._parents = set()
        self._children = set()

        self._obj = obj

    def __repr__(self):
        return """
            id: {node_id}
            \tparents: {parents}
            \tchildren: {children}
        """.format(
            node_id=self._name,
            parents=", ".join(list(self._parents)),
            children=", ".join(list(self._children)),
        )

    @property
    def name(self):
        return self._name

    @property
    def parents(self):
        return self._parents

    @property
    def children(self):
        return self._children

    @property
    def obj(self):
        return self._obj

    def add_parent(self, parent_id):
        self._parents.add(parent_id)

    def add_child(self, child_id):
        self._children.add(child_id)


class Edge:
    def __init__(self, follow_external_dependency=False):
        print('XXX Creating edge with: ', follow_external_dependency)
        self._follow_external_dependency = follow_external_dependency

    @property
    def follow_external_dependency(self):
        return self._follow_external_dependency


class Graph(object):
    def __init__(self):
        self._nodes = {}
        self._node2type = {}
        self._edges = {}

    def _node_exists(self, node_id):
        return self._node2type.get(node_id, None) is not None

    def add_node(
        self,
        node_type: str,
        node_id: str,
        name_to_show: str = None,
        obj: object = None
    ):
        if self._nodes.get(node_type, None) is None:
            self._nodes[node_type] = {}

        if self._nodes[node_type].get(node_id, None) is None and self._node2type.get(node_id, None):
            _logger.exception(
                "A different type of node with the same id: %s already exists",
                node_id,
            )
            raise IdAlreadyExistsException(f"A different type of node with the same id: {node_id} already exists")

        if self._nodes[node_type].get(node_id):
            _logger.debug("Node with name: %s already exists", node_id)
            return

        self._node2type[node_id] = node_type
        self._nodes[node_type][node_id] = Node(node_id, name_to_show, obj)

    def get_node(self, node_id: str):
        if not self._node_exists(node_id):
            return None

        return self._nodes[self._node2type[node_id]][node_id]

    def get_nodes(self, node_type):
        return self._nodes.get(node_type, None)

    def add_edge(self, from_node_id, to_node_id, **attributes):
        print('XXX add edge', from_node_id, to_node_id, attributes)
        from_node = self.get_node(from_node_id)
        to_node = self.get_node(to_node_id)

        if from_node is None:
            _logger.exception(
                "Adding edge (%s, %s), %s does not exist in graph",
                from_node_id,
                to_node_id,
                from_node_id,
            )

        if to_node is None:
            _logger.exception(
                "Adding edge (%s, %s), %s does not exist in graph",
                from_node_id,
                to_node_id,
                to_node_id,
            )

        from_node.add_child(to_node_id)
        to_node.add_parent(from_node_id)
        self._edges[(from_node_id, to_node_id)] = Edge(**attributes)

    def get_type(self, node_id):
        if not self._node_exists(node_id):
            return None

        return self._node2type[node_id]

    def get_edge(self, from_node_id, to_node_id):
        return self._edges.get((from_node_id, to_node_id))


class TaskGraph:
    NODE_TYPE_PIPELINE = "pipeline"
    NODE_TYPE_TASK = "task"
    NODE_TYPE_DATASET = "dataset"

    def __init__(self):
        self._graph = Graph()

    def add_pipeline(self, pipeline: dagger.pipeline.pipeline.Pipeline):
        self._graph.add_node(
            node_type=self.NODE_TYPE_PIPELINE, node_id=pipeline.name, obj=pipeline
        )

        for task in pipeline.tasks:
            self.add_task(task)
            self._graph.add_edge(pipeline.name, task.uniq_name)

    def add_task(self, task: Task):
        self._graph.add_node(
            node_type=self.NODE_TYPE_TASK,
            node_id=task.uniq_name,
            name_to_show=task.name,
            obj=task,
        )

        for task_input in task.inputs:
            self.add_dataset(task_input)
            if task_input.has_dependency:
                self._graph.add_edge(
                    task_input.alias(),
                    task.uniq_name,
                    follow_external_dependency=task_input.follow_external_dependency
                )

        for task_output in task.outputs:
            self.add_dataset(task_output)
            if task_output.has_dependency:
                self._graph.add_edge(task.uniq_name, task_output.alias())

    def add_dataset(self, io: IO):
        self._graph.add_node(node_type=self.NODE_TYPE_DATASET, node_id=io.alias(), obj=io)

    def print_graph(self, out_file=None):
        fs = open(out_file, "w") if out_file else sys.stdout
        for pipe_id, node in self._graph.get_nodes(self.NODE_TYPE_PIPELINE).items():
            fs.write(f"Pipeline: {pipe_id}\n")
            for node_id in list(node.children):
                child_node = self._graph.get_node(node_id)
                fs.write(f"\t task: {child_node.name}\n")
                fs.write(f"\t inputs:\n")
                for parent_id in list(child_node.parents):
                    if self._graph.get_type(parent_id) == self.NODE_TYPE_DATASET:
                        parent_node = self._graph.get_node(parent_id)
                        fs.write(f"\t\t {parent_node.name}\n")
                fs.write(f"\t outputs:\n")
                for output_id in list(child_node.children):
                    output_node = self._graph.get_node(output_id)
                    fs.write(f"\t\t {output_node.name}\n")
                    for output_task_id in list(output_node.children):
                        task_node = self._graph.get_node(output_task_id)
                        fs.write(f"\t\t\t dependency: {task_node.name}\n")

                fs.write("\n")

            fs.write("\n")
