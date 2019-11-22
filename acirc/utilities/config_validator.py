import logging

_logger = logging.getLogger('configFinder')


class Attribute:
    def __init__(self, attribute_name: str, parent_fields: list = None, required: bool = True, validator=None,
                 default=None, format_help: str = None, comment: str = None):

        self._name = attribute_name
        self._required = required
        self._parent_fields = parent_fields or []
        self._validator = validator
        self._default = default
        self._format = format_help
        self._comment = comment

    def __repr__(self):
        comment = ["(required)" if self._required else "(optional)"]
        if self._format:
            comment.append("format: " + self._format)
        if self._comment:
            comment.append(self._comment)

        return "{indent}{attribute_name:<30} # {comment}".format(
            indent="  " * len(self._parent_fields),
            attribute_name=self._name + ":",
            comment=" / ".join(comment)
        )

    @property
    def name(self):
        return self._name

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
    config_attributes = {}
    @classmethod
    def init_attributes_once(cls):
        if cls.config_attributes.get(cls.__name__, None):
            return

        parent_class = cls.__mro__[1]
        if parent_class.__name__ != 'ConfigValidator':
            parent_class.init_attributes_once()

        cls.init_attributes()
        if parent_class.__name__ != 'ConfigValidator':
            cls.config_attributes[cls.__name__] =\
                cls.config_attributes[parent_class.__name__] + cls.config_attributes[cls.__name__]

    @classmethod
    def add_config_attributes(cls, attributes: list):
        cls.config_attributes[cls.__name__] = attributes

    def __init__(self, location, config: dict):
        self._location = location
        self._config = config

        self.init_attributes_once()

        self._attributes = {}
        for i, attr in enumerate(self.config_attributes[self.__class__.__name__]):
            self._attributes[attr.name] = i

    def parse_attribute(self, attribute_name):
        attr = self.config_attributes[self.__class__.__name__][self._attributes[attribute_name]]
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

    # @property
    # def attributes(self):
    #     return self.config_attributes

    @classmethod
    def sample(cls):
        if cls.config_attributes.get(cls.__name__, None) is None:
            cls.init_attributes_once()

        yaml_str = []
        for attribute in cls.config_attributes[cls.__name__]:
            yaml_str.append(str(attribute))

        return "\n".join(yaml_str)
