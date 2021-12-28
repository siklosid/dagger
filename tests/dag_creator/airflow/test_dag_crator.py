import unittest

from dagger import conf
from dagger.config_finder.config_finder import ConfigFinder
from dagger.config_finder.config_processor import ConfigProcessor
from dagger.dag_creator.airflow.dag_creator import DagCreator
from dagger.graph.task_graph import TaskGraph

from airflow.utils.dot_renderer import render_dag


class TestDagCreator(unittest.TestCase):
    @staticmethod
    def _fetch_task_graph():
        cf = ConfigFinder(conf.DAGS_DIR)
        cp = ConfigProcessor(cf)

        pipelines = cp.process_pipeline_configs()

        task_graph = TaskGraph()
        for pipeline in pipelines:
            task_graph.add_pipeline(pipeline)

        return task_graph

    @staticmethod
    def _read_file(filename):
        with open(filename, "r") as stream:
            return stream.read()

    def setUp(self) -> None:
        self.task_graph = self._fetch_task_graph()

        self.dot_test_batch_graph_without_dataset =\
            self._read_file("tests/fixtures/dag_creator/airflow/dag_test_batch_without_dataset.dot")

        self.dot_test_batch_graph_with_dataset =\
            self._read_file("tests/fixtures/dag_creator/airflow/dag_test_batch_with_dataset.dot")

    def test_dag_creator_without_dataset(self):
        dag_creator = DagCreator(self.task_graph._graph)
        dags = dag_creator.traverse_graph()

        self.assertEqual(len(dags), 2)
        test_batch_dag = dags['test_batch']

        dot = render_dag(test_batch_dag)
        self.assertEqual(dot.source, self.dot_test_batch_graph_without_dataset)

    def test_dag_creator_with_dataset(self):
        dag_creator = DagCreator(self.task_graph._graph, with_data_nodes=True)
        dags = dag_creator.traverse_graph()

        self.assertEqual(len(dags), 2)
        test_batch_dag = dags['test_batch']

        dot = render_dag(test_batch_dag)
        self.assertEqual(dot.source, self.dot_test_batch_graph_with_dataset)


