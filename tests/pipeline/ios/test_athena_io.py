import unittest
from dagger.pipeline.io_factory import athena_io

import yaml


class DbIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open('tests/fixtures/pipeline/ios/athena_io.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.db_io = athena_io.AthenaIO(config, "/")

    def test_properties(self):
        self.assertEqual(self.db_io.alias(), "athena://test_schema/test_table")
        self.assertEqual(self.db_io.rendered_name, "test_schema.test_table")
        self.assertEqual(self.db_io.airflow_name, "athena-test_schema-test_table")
