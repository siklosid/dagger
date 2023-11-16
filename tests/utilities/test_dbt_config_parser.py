import logging
import unittest
from unittest import skip
from unittest.mock import patch, MagicMock

from dagger.utilities.dbt_config_parser import DBTConfigParser
from dagger.utilities.module import Module
from tests.fixtures.modules.dbt_config_parser_fixtures import (
    EXPECTED_DBT_MODEL_PARENTS,
    EXPECTED_DAGGER_INPUTS,
    DBT_MANIFEST_FILE_FIXTURE,
    DBT_PROFILE_FIXTURE,
    EXPECTED_DAGGER_OUTPUTS,
)

_logger = logging.getLogger("root")

DEFAULT_CONFIG_PARAMS = {
    "data_bucket": "bucket1-data-lake",
    "project_dir": "main",
    "profile_dir": ".dbt",
}
MODEL_NAME = "model1"


class TestDBTConfigParser(unittest.TestCase):
    @patch("builtins.open", new_callable=MagicMock, read_data=DBT_MANIFEST_FILE_FIXTURE)
    @patch("json.loads", return_value=DBT_MANIFEST_FILE_FIXTURE)
    @patch("yaml.safe_load", return_value=DBT_PROFILE_FIXTURE)
    def setUp(self, mock_open, mock_json_load, mock_safe_load):
        self._dbt_config_parser = DBTConfigParser(DEFAULT_CONFIG_PARAMS)


    def test_get_dbt_model_parents(self):
        result = self._dbt_config_parser._get_dbt_model_parents(MODEL_NAME)

        self.assertDictEqual(result, EXPECTED_DBT_MODEL_PARENTS)

    def test_generate_dagger_inputs(self):
        result_inputs = self._dbt_config_parser.generate_dagger_inputs(
            EXPECTED_DBT_MODEL_PARENTS
        )

        self.assertListEqual(result_inputs, EXPECTED_DAGGER_INPUTS)

    def test_generate_dagger_outputs(self):
        result_outputs = self._dbt_config_parser.generate_dagger_outputs(
            EXPECTED_DBT_MODEL_PARENTS["model_name"],
            EXPECTED_DBT_MODEL_PARENTS["schema"],
            EXPECTED_DBT_MODEL_PARENTS["relative_s3_path"],
        )

        self.assertListEqual(result_outputs, EXPECTED_DAGGER_OUTPUTS)
