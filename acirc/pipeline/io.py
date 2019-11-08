from abc import ABC, abstractmethod


class IO(ABC):
    def __init__(self, io_config):
        self.name = io_config['name']

    def __eq__(self, other):
        return self.alias() == other.alias()

    @abstractmethod
    def alias(self):
        pass
