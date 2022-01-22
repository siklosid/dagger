import unittest
import os
import yaml
from io import StringIO
from contextlib import redirect_stdout

from dagger.graph import task_graph
from dagger.utilities.exceptions import IdAlreadyExistsException
from dagger.pipeline.ios.dummy_io import DummyIO
from dagger.pipeline.tasks.dummy_task import DummyTask
from dagger.pipeline.pipeline import Pipeline
from dagger import conf


class TestGraph(unittest.TestCase):
    def setUp(self):
        os.environ["AIRFLOW_HOME"] = '/Users/david/mygit/dagger/tests/fixtures/config_finder/root/'

    def test_add_node(self):
        graph = task_graph.Graph()

        graph.add_node("type1", "id1")
        self.assertEqual(len(graph._nodes), 1)
        self.assertEqual(graph.get_node("id1")._node_id, "id1")
        self.assertEqual(graph.get_nodes("type1").get("id1")._node_id, "id1")
        self.assertEqual(graph.get_nodes("type2"), None)
        self.assertEqual(graph.get_type("id1"), "type1")
        self.assertEqual(graph.get_type("id2"), None)

        graph.add_node("type2", "id2")
        self.assertEqual(len(graph._nodes), 2)
        self.assertEqual(graph.get_node("id2")._node_id, "id2")
        self.assertEqual(graph.get_nodes("type1").get("id1")._node_id, "id1")
        self.assertEqual(graph.get_nodes("type1").get("id2"), None)
        self.assertEqual(graph.get_nodes("type2").get("id2")._node_id, "id2")
        self.assertEqual(graph.get_type("id1"), "type1")
        self.assertEqual(graph.get_type("id2"), "type2")

    def test_unique_constraints(self):
        graph = task_graph.Graph()
        graph.add_node("type1", "id1", "name1")

        self.assertRaises(IdAlreadyExistsException, graph.add_node, "type2", "id1")

        graph.add_node("type1", "id1", "name2")
        self.assertEqual(graph.get_node("id1")._name, "name1")

    def test_add_edge(self):
        graph = task_graph.Graph()
        graph.add_node("type1", "id1")
        graph.add_node("type2", "id2")
        graph.add_edge("id1", "id2")

        self.assertIn("id2", graph.get_node("id1")._children)
        self.assertIn("id1", graph.get_node("id2")._parents)


class TestTaskGraph(unittest.TestCase):
    def setUp(self):
        with open('tests/fixtures/graph/dummy_input.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.dummy_input = DummyIO(config, conf.DAGS_DIR)

        with open('tests/fixtures/graph/dummy_output.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.dummy_output = DummyIO(config, conf.DAGS_DIR)

        with open('tests/fixtures/graph/pipeline.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.pipeline = Pipeline(os.path.join(conf.DAGS_DIR, "test_pipeline"), config)

        with open('tests/fixtures/graph/dummy_task.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.dummy_task = DummyTask("dummy_task", "pipeline", self.pipeline, config)
        self.pipeline.add_task(self.dummy_task)

        with open('tests/fixtures/graph/graph.txt', "r") as stream:
            self.graph_str = stream.read()

    def test_add_dataset(self):
        graph = task_graph.TaskGraph()

        graph.add_dataset(self.dummy_input)
        self.assertEqual(len(graph._graph._nodes), 1)
        self.assertEqual(graph._graph.get_node("dummy://test_dummy_input")._node_id, "dummy://test_dummy_input")
        self.assertEqual(
            graph._graph.get_nodes(graph.NODE_TYPE_DATASET).get("dummy://test_dummy_input")._node_id,
            "dummy://test_dummy_input"
        )
        self.assertEqual(graph._graph.get_nodes(graph.NODE_TYPE_TASK), None)
        self.assertEqual(graph._graph.get_type("dummy://test_dummy_input"), graph.NODE_TYPE_DATASET)
        self.assertEqual(graph._graph.get_type("dummy://test_dummy_output"), None)

        graph.add_dataset(self.dummy_output)
        self.assertEqual(len(graph._graph._nodes), 1)
        self.assertEqual(graph._graph.get_nodes(graph.NODE_TYPE_PIPELINE), None)
        self.assertEqual(graph._graph.get_nodes(graph.NODE_TYPE_TASK), None)
        self.assertEqual(len(graph._graph.get_nodes(graph.NODE_TYPE_DATASET)), 2)
        self.assertEqual(graph._graph.get_node("dummy://test_dummy_output")._node_id, "dummy://test_dummy_output")
        self.assertEqual(graph._graph.get_node("dummy://test_dummy_input")._node_id, "dummy://test_dummy_input")
        self.assertEqual(
            graph._graph.get_nodes(graph.NODE_TYPE_DATASET).get("dummy://test_dummy_input")._node_id,
            "dummy://test_dummy_input"
        )
        self.assertEqual(
            graph._graph.get_nodes(graph.NODE_TYPE_DATASET).get("dummy://test_dummy_output")._node_id,
            "dummy://test_dummy_output"
        )
        self.assertEqual(graph._graph.get_type("dummy://test_dummy_input"), graph.NODE_TYPE_DATASET)
        self.assertEqual(graph._graph.get_type("dummy://test_dummy_output"), graph.NODE_TYPE_DATASET)

    def test_add_task(self):
        graph = task_graph.TaskGraph()

        graph.add_task(self.dummy_task)
        self.assertEqual(graph._graph.get_nodes(graph.NODE_TYPE_PIPELINE), None)
        self.assertEqual(len(graph._graph.get_nodes(graph.NODE_TYPE_DATASET)), 2)
        self.assertEqual(len(graph._graph.get_nodes(graph.NODE_TYPE_TASK)), 1)
        self.assertEqual(graph._graph.get_node("dummy://dummy_input")._node_id, "dummy://dummy_input")
        self.assertEqual(graph._graph.get_node("dummy://dummy_output")._node_id, "dummy://dummy_output")
        self.assertEqual(graph._graph.get_node("dummy_task:pipeline")._node_id, "dummy_task:pipeline")

        self.assertIn("dummy://dummy_output", graph._graph.get_node("dummy_task:pipeline")._children)
        self.assertIn("dummy_task:pipeline", graph._graph.get_node("dummy://dummy_output")._parents)

        self.assertIn("dummy://dummy_input", graph._graph.get_node("dummy_task:pipeline")._parents)
        self.assertIn("dummy_task:pipeline", graph._graph.get_node("dummy://dummy_input")._children)

        self.assertNotIn("dummy://dummy_input", graph._graph.get_node("dummy_task:pipeline")._children)
        self.assertNotIn("dummy://dummy_output", graph._graph.get_node("dummy_task:pipeline")._parents)

    def test_add_pipeline(self):
        graph = task_graph.TaskGraph()
        graph.add_pipeline(self.pipeline)

        self.assertEqual(len(graph._graph.get_nodes(graph.NODE_TYPE_PIPELINE)), 1)
        self.assertEqual(len(graph._graph.get_nodes(graph.NODE_TYPE_DATASET)), 2)
        self.assertEqual(len(graph._graph.get_nodes(graph.NODE_TYPE_TASK)), 1)
        self.assertEqual(graph._graph.get_node("test_pipeline")._node_id, "test_pipeline")
        self.assertEqual(graph._graph.get_node("dummy://dummy_input")._node_id, "dummy://dummy_input")
        self.assertEqual(graph._graph.get_node("dummy://dummy_output")._node_id, "dummy://dummy_output")

        self.assertIn("dummy://dummy_output", graph._graph.get_node("dummy_task:pipeline")._children)
        self.assertIn("dummy_task:pipeline", graph._graph.get_node("dummy://dummy_output")._parents)

        self.assertIn("dummy://dummy_input", graph._graph.get_node("dummy_task:pipeline")._parents)
        self.assertIn("dummy_task:pipeline", graph._graph.get_node("dummy://dummy_input")._children)

        self.assertNotIn("dummy://dummy_input", graph._graph.get_node("dummy_task:pipeline")._children)
        self.assertNotIn("dummy://dummy_output", graph._graph.get_node("dummy_task:pipeline")._parents)

        self.assertIn("test_pipeline", graph._graph.get_node("dummy_task:pipeline")._parents)
        self.assertIn("dummy_task:pipeline", graph._graph.get_node("test_pipeline")._children)

    def test_print_graph(self):
        graph = task_graph.TaskGraph()
        graph.add_pipeline(self.pipeline)

        graph.print_graph()
        result = ""
        with StringIO() as stdout_buffer, redirect_stdout(stdout_buffer):
            graph.print_graph()
            stdout_buffer.flush()
            result = stdout_buffer.getvalue()

        self.assertEqual(result, self.graph_str)
