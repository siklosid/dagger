

class AcircMissingFieldException(Exception):
    def __init__(self, message):
        super().__init__(message)


class AcircFieldFormatException(Exception):
    def __init__(self, message):
        super().__init__(message)


class InvalidConfigException(Exception):
    def __init__(self, message):
        super().__init__(message)
