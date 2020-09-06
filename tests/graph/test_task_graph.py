import unittest
import os
from dagger.graph import task_graph
from dagger.utilities.exceptions import IdAlreadyExistsException


class TestGraph(unittest.TestCase):
    def setUp(self):
        os.environ["AIRFLOW_HOME"] = '/Users/david/mygit/dagger/tests/fixtures/config_finder/root/'

    def test_add_node(self):
        graph = task_graph.Graph()
        graph.add_node("type1", "id1")

        self.assertEqual(len(graph._nodes), 1)
        self.assertEqual(graph.get_node("id1")._node_id, "id1")
        self.assertEqual(graph.get_nodes("type1").get("id1")._node_id, "id1")
        self.assertEqual(graph.get_type("id1"), "type1")

    def test_unique_constraints(self):
        graph = task_graph.Graph()
        graph.add_node("type1", "id1", "name1")

        self.assertRaises(IdAlreadyExistsException, graph.add_node, "type2", "id1")

        graph.add_node("type1", "id1", "name2")
        self.assertEqual(graph.get_node("id1")._name, "name1")

        graph.add_node("type1", "id1", "name2", overwrite=True)
        self.assertEqual(graph.get_node("id1")._name, "name2")

    def test_add_edge(self):
        graph = task_graph.Graph()
        graph.add_node("type1", "id1")
        graph.add_node("type2", "id2")
        graph.add_edge("id1", "id2")

        self.assertIn("id2", graph.get_node("id1")._children)
        self.assertIn("id1", graph.get_node("id2")._parents)


# class TestTaskGraph(unittest.TestCase):
