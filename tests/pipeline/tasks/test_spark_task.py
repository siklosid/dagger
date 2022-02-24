import unittest

from dagger import conf
from dagger.config_finder.config_finder import ConfigFinder
from dagger.config_finder.config_processor import ConfigProcessor


class TestOperatorCreator(unittest.TestCase):
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
