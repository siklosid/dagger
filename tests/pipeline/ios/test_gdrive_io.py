import unittest
from dagger.pipeline.io_factory import gdrive_io

import yaml


class GDriveIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open('tests/fixtures/pipeline/ios/gdrive_io.yaml', "r") as stream:
            config = yaml.safe_load(stream)

        self.db_io = gdrive_io.GDriveIO(config, "/")

    def test_properties(self):
        self.assertEqual(self.db_io.alias(), "gdrive://test_folder/test_file_name")
        self.assertEqual(self.db_io.rendered_name, "test_folder/test_file_name")
        self.assertEqual(self.db_io.airflow_name, "gdrive-test_folder-test_file_name")
