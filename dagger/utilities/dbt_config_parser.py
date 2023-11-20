import json
from os import path
from os.path import join
from pprint import pprint
from typing import Tuple, List, Dict

import yaml

ATHENA_TASK_BASE = {"type": "athena"}
S3_TASK_BASE = {"type": "s3"}



class DBTConfigParser:
    """
    Module that parses the manifest.json file generated by dbt and generates the dagger inputs and outputs for the respective dbt model
    """

    def __init__(self, default_config_parameters: dict):
        self._dbt_profile = default_config_parameters.get("dbt_profile", "data")
        self._default_data_bucket = default_config_parameters["data_bucket"]
        self._dbt_project_dir = default_config_parameters.get("project_dir", None)
        dbt_manifest_path = path.join(self._dbt_project_dir, "target", "manifest.json")
        self._dbt_profile_dir = default_config_parameters.get("profile_dir", None)
        dbt_profile_path = path.join(self._dbt_profile_dir, "profiles.yml")

        with open(dbt_manifest_path, "r") as f:
            data = f.read()
        self._manifest_data = json.loads(data)
        profile_yaml = yaml.safe_load(open(dbt_profile_path, "r"))
        prod_dbt_profile = profile_yaml[self._dbt_project_dir.split("/")[-1]][
            "outputs"
        ][self._dbt_profile]
        self._default_data_dir = prod_dbt_profile.get(
            "s3_data_dir"
        ) or prod_dbt_profile.get("s3_staging_dir")

    def _generate_dagger_dependency(self, node: dict) -> List[Dict]:
        """
        Generates the dagger task based on whether the DBT model node is a staging model or not.
        If the DBT model node represents a staging model, then a dagger athena task is generated for each source of the DBT model.
        If the DBT model node is not a staging model, then a dagger athena task and an s3 task is generated for the DBT model node itself.
        Args:
            node: The extracted node from the manifest.json file

        Returns:
            List[Dict]: The respective dagger tasks for the DBT model node

        """
        model_name = node["name"]

        s3_task = S3_TASK_BASE.copy()
        dagger_tasks = []

        if model_name.startswith("stg_"):
            source_nodes = node.get("depends_on", {}).get("nodes", [])
            for source_node in source_nodes:
                _, project_name, schema_name, table_name = source_node.split(".")
                athena_task = ATHENA_TASK_BASE.copy()

                athena_task["name"] = f"stg_{schema_name}__{table_name}"
                athena_task["schema"] = schema_name
                athena_task["table"] = table_name

                dagger_tasks.append(athena_task)
        else:
            athena_task = ATHENA_TASK_BASE.copy()
            model_schema = node["schema"]
            athena_task["name"] = f"{model_schema}_{model_name}_athena"
            athena_task["table"] = model_name
            athena_task["schema"] = node["schema"]

            s3_task["name"] = f"{model_schema}_{model_name}_s3"
            s3_task["bucket"] = self._default_data_bucket
            s3_task["path"] = self._get_model_data_location(
                node, model_schema, model_name
            )

            dagger_tasks.append(athena_task)
            dagger_tasks.append(s3_task)

        return dagger_tasks

    def _get_model_data_location(
        self, node: dict, schema: str, dbt_model_name: str
    ) -> str:
        """
        Gets the S3 path of the dbt model relative to the data bucket.
        If external location is not specified in the DBT model config, then the default data directory from the
        DBT profiles configuration is used.
        Args:
            node: The extracted node from the manifest.json file
            schema: The schema of the dbt model
            dbt_model_name: The name of the dbt model

        Returns:
            str: The relative S3 path of the dbt model relative to the data bucket

        """
        location = node.get("config", {}).get("external_location")
        if not location:
            location = join(self._default_data_dir, schema, dbt_model_name)

        return location.split(self._default_data_bucket)[1].lstrip("/")

    def generate_dagger_io(self, model_name: str) -> Tuple[list, list]:
        """
        Parse through all the parents of the DBT model and return the dagger inputs and outputs for the DBT model
        Args:
            model_name: The name of the DBT model

        Returns:
            Tuple[list, list]: The dagger inputs and outputs for the DBT model

        """
        inputs_list = []

        nodes = self._manifest_data["nodes"]
        model_node = nodes[f"model.main.{model_name}"]

        parent_node_names = model_node.get("depends_on", {}).get("nodes", [])

        for index, parent_node_name in enumerate(parent_node_names):
            if not (".int_" in parent_node_name):
                parent_model_node = nodes.get(parent_node_name)
                dagger_input = self._generate_dagger_dependency(parent_model_node)

                inputs_list += dagger_input

        output_list = self._generate_dagger_dependency(model_node)

        return inputs_list, output_list