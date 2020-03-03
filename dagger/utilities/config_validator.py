import logging
from dagger.utilities.exceptions import (
    DaggerMissingFieldException,
    DaggerFieldFormatException,
)

_logger = logging.getLogger('configFinder')


class Attribute:
    def __init__(self, attribute_name: str, parent_fields: list = None,
                 required: bool = True, nullable: bool = False,
                 validator=None, auto_value=None,
                 format_help: str = None, comment: str = None):

        self._name = attribute_name
        self._parent_fields = parent_fields or []
        self._required = required
        self._nullable = nullable
        self._validator = validator
        self._auto_value = auto_value
        self._format = format_help
        self._comment = comment
        self._is_parent = False

    def __repr__(self):
        comment = []
        if self._required and self.nullable and not self.is_parent:
            comment.append("[Can be empty]")
        if self._format:
            comment.append("format: " + self._format)
        if self._comment:
            comment.append(self._comment)

        return "{indent}{attribute_value:<30} {comment}".format(
            indent="  " * len(self._parent_fields),
            attribute_value="{}: {}".format(self._name, self._auto_value or ""),
            comment="# " + " | ".join(comment) if len(comment) > 0 else ""
        )

    @property
    def name(self):
        return self._name

    @property
    def parent_fields(self):
        return self._parent_fields

    @property
    def required(self):
        return self._required

    @property
    def nullable(self):
        return self._nullable

    @property
    def validator(self):
        return self._validator

    @property
    def auto_value(self):
        return self._auto_value

    @property
    def is_parent(self):
        return self._is_parent

    @is_parent.setter
    def is_parent(self, value):
        self._is_parent = value


class ConfigValidator:
    config_attributes = {}

    @classmethod
    def init_attributes_once(cls, orig_cls):
        if cls.config_attributes.get(cls.__name__, None):
            return

        parent_class = cls.__mro__[1]
        if parent_class.__name__ != 'ConfigValidator':
            parent_class.init_attributes_once(orig_cls)

        cls.init_attributes(orig_cls)
        if parent_class.__name__ != 'ConfigValidator':
            cls.config_attributes[cls.__name__] =\
                cls.config_attributes[parent_class.__name__] + cls.config_attributes[cls.__name__]

        attributes_lookup = {}
        for index, attribute in enumerate(cls.config_attributes[cls.__name__]):
            attributes_lookup[attribute.name] = index

        for attribute in cls.config_attributes[cls.__name__]:
            for parent_attribute in attribute.parent_fields:
                cls.config_attributes[cls.__name__][attributes_lookup[parent_attribute]].is_parent = True

    @classmethod
    def add_config_attributes(cls, attributes: list):
        cls.config_attributes[cls.__name__] = attributes

    def __init__(self, location, config: dict):
        self._location = location
        self._config = config

        self.init_attributes_once(self.__class__)

        self._attributes = {}
        for i, attr in enumerate(self.config_attributes[self.__class__.__name__]):
            self._attributes[attr.name] = i

    def parse_attribute(self, attribute_name):
        attr = self.config_attributes[self.__class__.__name__][self._attributes[attribute_name]]
        parsed_value = self._config
        try:
            for i in range(len(attr.parent_fields)):
                parsed_value = parsed_value[attr.parent_fields[i]]
            parsed_value = parsed_value[attribute_name]
        except (TypeError, KeyError):
            if attr.required:
                msg = "Required field: {} is missing in {}".format(attribute_name, self._location)
                _logger.error(msg)
                raise DaggerMissingFieldException(msg)
            else:
                return None

        if parsed_value is None and not attr.nullable:
            msg = "Field {} cannot be empty in {}".format(attribute_name, self._location)
            _logger.error(msg)
            raise DaggerFieldFormatException(msg)

        try:
            if attr.validator and parsed_value:
                parsed_value = attr.validator(parsed_value)
        except Exception as e:
            msg = "Wrong format for field: {} in {} with error: {}".format(attribute_name, self._location, str(e))
            _logger.error(msg)
            raise DaggerFieldFormatException(msg)

        return parsed_value

    @classmethod
    def sample(cls):
        if cls.config_attributes.get(cls.__name__, None) is None:
            cls.init_attributes_once(cls)

        yaml_str = []
        for attribute in cls.config_attributes[cls.__name__]:
            if attribute.required:
                yaml_str.append(str(attribute))

        yaml_str.append("\n\n\n# Other attributes:\n")
        for attribute in cls.config_attributes[cls.__name__]:
            if not attribute.required or attribute.is_parent:
                yaml_str.append("# " + str(attribute))

        return "\n".join(yaml_str)
