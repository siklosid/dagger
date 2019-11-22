from acirc.pipeline.task import Task
from datetime import datetime
from acirc import conf
import logging

import yaml
from os.path import (
    relpath,
    join,
)

_logger = logging.getLogger('configFinder')


class Attribute:
    def __init__(self, attribute_name: str, parent_fields: list = [], required: bool = True, validator=None, default=None):
        self._name = attribute_name
        self._required = required
        self._parent_fields = parent_fields
        self._validator = validator
        self._default = default

    @property
    def name(self):
        return self._name

    @property
    def required(self):
        return self._required

    @property
    def parent_fields(self):
        return self._parent_fields

    @property
    def validator(self):
        return self._validator

    @property
    def default(self):
        return self._default


class ConfigValidator:
    s_attributes = []

    def __init__(self, location, config: dict):
        self._location = location
        self._config = config
        self._attributes = {}

        for i, attr in enumerate(self.s_attributes):
            self._attributes[attr.name] = i

    def parse_attribute(self, attribute_name):
        attr = self.s_attributes[self._attributes[attribute_name]]
        parsed_value = self._config
        for i in range(len(attr.parent_fields)):
            parsed_value = parsed_value[attr.parent_fields[i]]
        parsed_value = parsed_value[attribute_name] or attr.default

        try:
            if attr.validator:
                parsed_value = attr.validator(parsed_value)
        except:
            _logger.error("Error in {} with attribute {}", self._location, attribute_name)

        return parsed_value

    @property
    def attributes(self):
        return self.s_attributes

    @property
    def sample(self):
        pass


class Pipeline(ConfigValidator):

    @staticmethod
    def init_attributes():
        Pipeline.s_attributes = [
            Attribute(attribute_name='owner', validator=str),
            Attribute(attribute_name='description', validator=str),
            Attribute(attribute_name='airflow_parameters'),
            Attribute(attribute_name='default_args', required=False, validator=dict,
                      parent_fields=['airflow_parameters'], default={}),
            Attribute(attribute_name='schedule'),
            Attribute(attribute_name='start_date',
                      validator=lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M')),
            Attribute(attribute_name='dag_parameters', required=False, validator=dict,
                      parent_fields=['airflow_parameters'], default={})
        ]

    def __init__(self, directory: str, config: dict):
        self.init_attributes()
        super().__init__(join(directory, 'pipeline.yaml'), config)

        self._directory = directory
        self._name = relpath(directory, conf.DAGS_DIR).replace('/', '-')

        self._owner = self.parse_attribute(attribute_name='owner')
        self._description = self.parse_attribute(attribute_name='description')
        self._default_args = self.parse_attribute(attribute_name='default_args')
        self._schedule = self.parse_attribute(attribute_name='schedule')
        self._start_date = self.parse_attribute(attribute_name='start_date')
        self._parameters = self.parse_attribute(attribute_name='dag_parameters')

        self._tasks = []
        print("XXX", [x.name for x in self.attributes])

    @property
    def directory(self):
        return self._directory

    @property
    def name(self):
        return self._name

    @property
    def owner(self):
        return self._owner

    @property
    def schedule(self):
        return self._schedule

    @property
    def start_date(self):
        return self._start_date

    @property
    def default_args(self):
        return self._default_args

    @property
    def parameters(self):
        return self._parameters

    @property
    def tasks(self):
        return self._tasks

    def add_task(self, task: Task):
        self._tasks.append(task)

    @staticmethod
    def sample_config():
        config = {
            'owner': '<team>@circ.com',
            'description': '<description>',
            'schedule': '0 3 * * *',
            'start_date': '2019-11-12T02:00',
            'airflow_parameters': {
                'default_args': None,
                'dag_parameters': None
            },
            'alerts': [
                {
                    'slack': '#data-alerts'
                },
                {
                    'email':
                        [
                            'marketing@circ.com',
                            'david.siklosi@email.com'
                        ]
                }
            ]
        }
        with open('pipeline.yml.template', 'w') as stream:
            yaml.safe_dump(config, stream, default_flow_style=False, sort_keys=False)
