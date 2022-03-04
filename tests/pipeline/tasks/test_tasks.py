import unittest

from dagger import conf
from dagger.config_finder.config_finder import ConfigFinder
from dagger.config_finder.config_processor import ConfigProcessor


class TestConfigProcessor(unittest.TestCase):
    def test_override(self):
        cf = ConfigFinder(conf.DAGS_DIR)
        cp = ConfigProcessor(cf)

        pipelines = cp.process_pipeline_configs()
        for pipe in pipelines:
            for task in pipe.tasks:
                if task.name == "spark":
                    spark_task = task

        self.assertEqual(spark_task.template_parameters["frequency"], "24")
        self.assertEqual(spark_task.airflow_parameters["retries"], 4)

    def test_read_env(self):
        DAGS_DIR = '/Users/lauralehoczki/git/dataeng-airflow-dags/dags/core/orders/'
        cf = ConfigFinder(DAGS_DIR)
        cp = ConfigProcessor(cf)

        pipelines = cp.process_pipeline_configs()

        for pipe in pipelines:
            for task in pipe.tasks:
                print(task.name)
                if task.name == "batch":
                    batch_task = task

        #self.assertEqual(batch_task.outputs[2].bucket, "cholocal-test")
