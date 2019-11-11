from acirc.pipeline.io import IO
from acirc.pipeline.io_factory import IOFactory

import logging
_logger = logging.getLogger('configFinder')


class Task:
    ref_name = None

    def __init__(self, name: str, pipeline_name, config: dict):
        self._name = name
        self._pipeline_name = pipeline_name
        self._parameters = config['parameters']
        self._io_factory = IOFactory()

        self._inputs = []
        self._outputs = []
        self.process_inputs(config['inputs'])
        self.process_outputs(config['outputs'])

    @property
    def name(self):
        return self._name

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def uniq_name(self):
        return "{}:{}".format(self.name, self.pipeline_name)

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    def add_input(self, task_input: IO):
        _logger.info("Adding input: %s to task: %s", task_input.name, self._name)
        self._inputs.append(task_input)

    def add_output(self, task_output: IO):
        _logger.info("Adding output: %s to task: %s", task_output.name, self._name)
        self._outputs.append(task_output)

    def process_inputs(self, inputs: dict):
        for io_config in inputs:
            io_type = io_config['type']
            self.add_input(self._io_factory.create_io(io_type, io_config))

    def process_outputs(self, outputs: dict):
        for io_config in outputs:
            io_type = io_config['type']
            self.add_output(self._io_factory.create_io(io_type, io_config))
