import fnmatch
import logging
import os
from typing import List

PIPELINE_CONFIG_FILENAME = "pipeline.yaml"
_logger = logging.getLogger("configFinder")


class TaskConfig:
    def __init__(self, config: str):
        self._config = config

    @property
    def config(self):
        return self._config


class PipelineConfig:
    def __init__(self, directory: str, config: str, job_configs: List[TaskConfig]):
        """[summary]

        Parameters
        ----------
        config : str
            [Pipeline config string]
        job_configs : List[TaskConfig]
            [List of job config strings belonging to this pipeline]
        """
        self._directory = directory
        self._config = config
        self._job_configs = job_configs

    @property
    def directory(self):
        return self._directory

    @property
    def config(self):
        return self._config

    @property
    def job_configs(self):
        return self._job_configs


class ConfigFinder:
    def __init__(self, root: str):
        self._root = root

    def find_configs(self) -> List[PipelineConfig]:
        _logger.info("Collecting config files from: %s", self._root)
        pipeline_configs = []

        for root, _, files in os.walk(self._root):
            confs = fnmatch.filter(files, "*.yaml")
            if len(confs) <= 1:
                continue

            job_configs = []

            _logger.info("Searching pipeline config in directory: %s", root)
            pipeline_config_file = ""
            for conf in confs:
                if conf == PIPELINE_CONFIG_FILENAME:
                    pipeline_config_file = conf
                    _logger.info("Config found in directory: %s", root)
                else:
                    job_configs.append(TaskConfig(conf))

            if pipeline_config_file == "":
                _logger.info("Didn't find config in directory: %s", root)
                continue

            pipeline_configs.append(
                PipelineConfig(root, pipeline_config_file, job_configs)
            )

        return pipeline_configs
