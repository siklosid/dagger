import logging
from os.path import join

from dagger.pipeline.io import IO
from dagger.pipeline.io_factory import IOFactory
from dagger.utilities.config_validator import Attribute, ConfigValidator

_logger = logging.getLogger("configFinder")


class Task(ConfigValidator):
    ref_name = None
    default_pool = None

    @classmethod
    def init_attributes(cls, orig_cls):
        cls.add_config_attributes(
            [
                Attribute(attribute_name="type", auto_value=orig_cls.ref_name),
                Attribute(attribute_name="description"),
                Attribute(
                    attribute_name="inputs",
                    format_help="list",
                    comment="Use dagger init-io cli",
                ),
                Attribute(
                    attribute_name="outputs",
                    format_help="list",
                    comment="Use dagger init-io cli",
                ),
                Attribute(attribute_name="pool", required=False),
                Attribute(
                    attribute_name="airflow_task_parameters",
                    nullable=True,
                    format_help="dictionary",
                ),
                Attribute(
                    attribute_name="template_parameters",
                    nullable=True,
                    format_help="dictionary",
                ),
                Attribute(attribute_name="task_parameters", nullable=True),
            ]
        )

    def __init__(self, name: str, pipeline_name, pipeline, config: dict):
        super().__init__(join(pipeline.directory, name + ".yaml"), config)

        self._io_factory = IOFactory()

        self._name = name
        self._pipeline_name = pipeline_name
        self._pipeline = pipeline
        self._description = self.parse_attribute("description")
        self._parameters = self.parse_attribute("task_parameters")
        self._airflow_parameters = self.parse_attribute("airflow_task_parameters") or {}
        self._template_parameters = self.parse_attribute("template_parameters") or {}

        self._inputs = []
        self._outputs = []
        self._pool = self.parse_attribute("pool") or self.default_pool
        self.process_inputs(config["inputs"])
        self.process_outputs(config["outputs"])

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return self._description

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def pipeline(self):
        return self._pipeline

    @property
    def uniq_name(self):
        return "{}:{}".format(self.name, self.pipeline_name)

    @property
    def airflow_parameters(self):
        return self._airflow_parameters

    @property
    def template_parameters(self):
        return self._template_parameters

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    @property
    def pool(self):
        return self._pool

    def add_input(self, task_input: IO):
        _logger.info("Adding input: %s to task: %s", task_input.name, self._name)
        self._inputs.append(task_input)

    def add_output(self, task_output: IO):
        _logger.info("Adding output: %s to task: %s", task_output.name, self._name)
        self._outputs.append(task_output)

    def process_inputs(self, inputs):
        if inputs:
            for io_config in inputs:
                io_type = io_config["type"]
                self.add_input(self._io_factory.create_io(io_type, io_config, self))

    def process_outputs(self, outputs):
        if outputs:
            for io_config in outputs:
                io_type = io_config["type"]
                self.add_output(self._io_factory.create_io(io_type, io_config, self))
