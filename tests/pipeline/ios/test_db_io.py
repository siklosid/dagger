import unittest
from dagger.pipeline.io_factory import db_io

import yaml
import json


class DbIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open('tests/fixtures/pipeline/ios/db_io.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.db_io = db_io.DbIO(config, "/")

    def test_properties(self):
        self.assertEqual(self.db_io.alias(), "mysql://host:port/test_table")
        expected_rendered_name = json.dumps(
            {
                "database_type": "mysql",
                "conn_id": "host:port",
                "table": "test_table",
            }
        )
        self.assertEqual(self.db_io.rendered_name, expected_rendered_name)
        self.assertEqual(self.db_io.airflow_name, "host:port-test_table")
