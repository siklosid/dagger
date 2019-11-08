import logging
from pipeline.pipeline import Pipeline
from config_finder.config_finder import ConfigFinder
from pipeline.task_factory import TaskFactory
import conf

from os.path import join, splitext, relpath
import yaml

_logger = logging.getLogger('configFinder')
DAG_DIR = conf.DAGS_DIR


class ConfigProcessor:
    def __init__(self, config_finder: ConfigFinder):
        self._config_finder = config_finder
        self._task_factory = TaskFactory()

    @staticmethod
    def _load_yaml(yaml_path):
        with open(yaml_path, 'r') as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                _logger.exception("Couldn't read config file {}", yaml_path, exc)
                exit(1)
        return config

    def process_pipeline_configs(self):
        configs = self._config_finder.find_configs()
        pipelines = []

        for pipeline_config in configs:
            pipeline_name = relpath(pipeline_config._directory, DAG_DIR).replace('/', '-')
            config_path = join(pipeline_config._directory, pipeline_config._config)

            _logger.info("Processing config: %s", config_path)
            pipeline = Pipeline(pipeline_name, self._load_yaml(config_path))

            for task_config in pipeline_config._job_configs:
                task_name = splitext(task_config._config)[0]
                task_config_path = join(pipeline_config._directory, task_config._config)

                _logger.info("Processing task config: %s", task_config_path)
                task_config = self._load_yaml(task_config_path)
                task_type = task_config['type']
                pipeline.add_task(
                    self._task_factory.create_task(
                        task_type,
                        task_name,
                        pipeline_name,
                        task_config
                    )
                )

            pipelines.append(pipeline)

        return pipelines
