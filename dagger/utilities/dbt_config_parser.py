from os import path
from os.path import join
from typing import Union
import json
import yaml

ATHENA_IO_BASE = {"type": "athena"}
S3_IO_BASE = {"type": "s3"}

class DBTConfigParser:

    def __init__(self, default_config_parameters:dict):
        self._default_data_bucket = default_config_parameters["data_bucket"]
        self._dbt_project_dir = default_config_parameters.get("project_dir", None)
        dbt_manifest_path = path.join(self._dbt_project_dir, "target","manifest.json")
        self._dbt_profile_dir = default_config_parameters.get("profile_dir", None)
        dbt_profile_path = path.join(self._dbt_profile_dir, "profiles.yml")

        with open(dbt_manifest_path, "r") as f:
            data = f.read()
        self._manifest_data = json.loads(data)
        profile_yaml = yaml.safe_load(open(dbt_profile_path, "r"))
        prod_dbt_profile = profile_yaml[self._dbt_project_dir]['outputs']['data']
        self._default_data_dir = prod_dbt_profile.get('s3_data_dir') or prod_dbt_profile.get('s3_staging_dir')