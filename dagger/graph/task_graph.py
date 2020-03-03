import dagger.pipeline.pipeline
from dagger.pipeline.task import Task
from dagger.pipeline.io import IO

from abc import ABC
import sys
import logging
_logger = logging.getLogger('graph')


class Node(ABC):
    def __init__(self, node_id: str, name_to_show: str, obj=None):
        self._node_id = node_id
        self._name = name_to_show if name_to_show else node_id
        self._parents = set()
        self._children = set()
        self._attributes = {}

        self._obj = obj

    def __repr__(self):
        return """
            id: {node_id}
            \tparents: {parents}
            \tchildren: {children}
        """.format(
            node_id=self._name,
            parents=', '.join(list(self._parents)),
            children=', '.join(list(self._children)))

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
    def attributes(self):
        return self._attributes

    @attributes.setter
    def attributes(self, t_key_value):
        key, value = t_key_value
        self._attributes[key] = value

    @property
    def obj(self):
        return self._obj

    def add_parent(self, parent_id):
        self._parents.add(parent_id)

    def add_child(self, child_id):
        self._children.add(child_id)


class Graph(object):
    def __init__(self):
        self._nodes = {}
        self._node2type = {}

    def _node_exists(self, node_id):
        return self._node2type.get(node_id, None) is not None

    def add_node(self, node_type: str, node_id: str, name_to_show: str = None,
                 obj: object = None, overwrite: bool = False):
        if self._nodes.get(node_type, None) is None:
            self._nodes[node_type] = {}

        if self._nodes[node_type].get(node_id, None) is None and self._node2type.get(node_id, None):
            _logger.exception("A different type of node with the same name: %s already exists", node_id)
            exit(1)

        if self._nodes[node_type].get(node_id) and not overwrite:
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

    def add_edge(self, from_node_id, to_node_id):
        from_node = self.get_node(from_node_id)
        to_node = self.get_node(to_node_id)

        if from_node is None:
            _logger.exception("Adding edge (%s, %s), %s does not exist in graph",
                              from_node_id,
                              to_node_id,
                              from_node_id)

        if to_node is None:
            _logger.exception("Adding edge (%s, %s), %s does not exist in graph", from_node_id, to_node_id, to_node_id)

        from_node.add_child(to_node_id)
        to_node.add_parent(from_node_id)

    def get_type(self, node_id):
        if not self._node_exists(node_id):
            return None

        return self._node2type[node_id]


class TaskGraph:
    NODE_TYPE_PIPELINE = 'pipeline'
    NODE_TYPE_TASK = 'task'
    NODE_TYPE_DATASET = 'dataset'

    def __init__(self):
        self._graph = Graph()

    def add_pipeline(self, pipeline: dagger.pipeline.pipeline.Pipeline):
        self._graph.add_node(node_type=self.NODE_TYPE_PIPELINE, node_id=pipeline.name, obj=pipeline)

        for task in pipeline.tasks:
            self.add_task(task)
            self._graph.add_edge(pipeline.name, task.uniq_name)

    def add_task(self, task: Task):
        self._graph.add_node(node_type=self.NODE_TYPE_TASK, node_id=task.uniq_name, name_to_show=task.name, obj=task)

        for task_input in task.inputs:
            self.add_dataset(task_input)
            if task_input.has_dependency:
                self._graph.add_edge(task_input.alias(), task.uniq_name)

        for task_output in task.outputs:
            self.add_dataset(task_output)
            if task_output.has_dependency:
                self._graph.add_edge(task.uniq_name, task_output.alias())

    def add_dataset(self, io: IO):
        self._graph.add_node(node_type=self.NODE_TYPE_DATASET, node_id=io.alias(), obj=io)

    def print_graph(self, out_file=None):
        fs = open(out_file, "w") if out_file else sys.stdout
        for pipe_id, node in self._graph.get_nodes(self.NODE_TYPE_PIPELINE).items():
            fs.write("Pipeline: {}\n".format(pipe_id))
            fs.write("Tasks:")
            for node_id in list(node.children):
                child_node = self._graph.get_node(node_id)
                fs.write("\t" + str(child_node))
            fs.write("\n")
