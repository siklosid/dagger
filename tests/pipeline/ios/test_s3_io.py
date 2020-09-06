import unittest
from dagger.pipeline.io_factory import s3_io

import yaml


class DbIOTest(unittest.TestCase):
    def setUp(self) -> None:
        with open('tests/fixtures/pipeline/ios/s3_io.yaml', "r") as stream:
            self.config = yaml.safe_load(stream)

    def test_properties(self):
        db_io = s3_io.S3IO(self.config, "/")

        self.assertEqual(db_io.alias(), "s3://test_bucket/test_path")
        self.assertEqual(db_io.rendered_name, "s3://test_bucket/test_path")
        self.assertEqual(db_io.airflow_name, "s3-test_bucket-test_path")

    def test_with_protocol(self):
        self.config['s3_protocol'] = 's3a'
        db_io = s3_io.S3IO(self.config, "/")

        self.assertEqual(db_io.alias(), "s3://test_bucket/test_path")
        self.assertEqual(db_io.rendered_name, "s3a://test_bucket/test_path")
        self.assertEqual(db_io.airflow_name, "s3-test_bucket-test_path")
