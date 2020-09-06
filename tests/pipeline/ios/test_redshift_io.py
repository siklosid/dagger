import unittest
from dagger.pipeline.io_factory import redshift_io

import yaml


class DbIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open('tests/fixtures/pipeline/ios/redshift_io.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.db_io = redshift_io.RedshiftIO(config, "/")

    def test_properties(self):
        self.assertEqual(self.db_io.alias(), "redshift://test_schema/test_table")
        self.assertEqual(self.db_io.rendered_name, "test_schema.test_table")
        self.assertEqual(self.db_io.airflow_name, "redshift-test_schema-test_table")
