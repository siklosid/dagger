import logging
from os import environ
from os.path import join, relpath, splitext
from mergedeep import merge

import yaml
from envyaml import EnvYAML
from dagger.config_finder.config_finder import ConfigFinder
from dagger.pipeline.pipeline import Pipeline
from dagger.pipeline.task_factory import TaskFactory

import dagger.conf as conf


_logger = logging.getLogger("configFinder")
DAG_DIR = join(environ.get("AIRFLOW_HOME", "./"), "dags")


class ConfigProcessor:
    def __init__(self, config_finder: ConfigFinder):
        self._config_finder = config_finder
        self._task_factory = TaskFactory()

    def _load_yaml(self, yaml_path):
        config_dict = EnvYAML(yaml_path).export()
        config_dict = self.localize_params(config_dict)
        return config_dict

    def localize_params(self, config):
        env_dependent_params = config.get("environments", {}).get(conf.ENV, {})
        if env_dependent_params.get("deactivate"):
            return None
        merge(config, env_dependent_params)
        return config

    def process_pipeline_configs(self):
        configs = self._config_finder.find_configs()
        pipelines = []

        for pipeline_config in configs:
            pipeline_name = relpath(pipeline_config.directory, DAG_DIR).replace(
                "/", "-"
            )
            config_path = join(pipeline_config.directory, pipeline_config.config)

            _logger.info("Processing config: %s", config_path)
            config_dict = self._load_yaml(config_path)
            if config_dict:
                pipeline = Pipeline(pipeline_config.directory, config_dict)
            else:
                _logger.info(f"{pipeline_name} pipeline is disabled in {conf.ENV} environment")
                continue

            for task_config in pipeline_config.job_configs:
                task_name = splitext(task_config.config)[0]
                task_config_path = join(pipeline_config.directory, task_config.config)

                _logger.info("Processing task config: %s", task_config_path)
                task_config = self._load_yaml(task_config_path)
                if task_config:
                    task_type = task_config["type"]
                    pipeline.add_task(
                        self._task_factory.create_task(
                            task_type, task_name, pipeline_name, pipeline, task_config
                        )
                    )
                else:
                    _logger.info(f"{task_name} job is disabled in {conf.ENV} environment")

            pipelines.append(pipeline)

        return pipelines
