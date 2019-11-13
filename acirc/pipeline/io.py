from abc import ABC, abstractmethod


class IO(ABC):
    def __init__(self, io_config):
        self._name = io_config['name']

    def __eq__(self, other):
        return self.alias() == other.alias()

    @abstractmethod
    def alias(self):
        pass

    @property
    def name(self):
        return self._name

    @property
    def rendered_name(self):
        raise NotImplementedError
