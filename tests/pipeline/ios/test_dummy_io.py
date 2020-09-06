import unittest
from dagger.pipeline.io_factory import dummy_io

import yaml


class DummyIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open('tests/fixtures/pipeline/ios/dummy_io.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.db_io = dummy_io.DummyIO(config, "/")

    def test_properties(self):
        self.assertEqual(self.db_io.alias(), "dummy://test_dummy")
        self.assertEqual(self.db_io.rendered_name, "test_dummy")
        self.assertEqual(self.db_io.airflow_name, "dummy-test_dummy")
