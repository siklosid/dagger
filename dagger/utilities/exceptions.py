class DaggerMissingFieldException(Exception):
    def __init__(self, message):
        super().__init__(message)


class DaggerFieldFormatException(Exception):
    def __init__(self, message):
        super().__init__(message)


class InvalidConfigException(Exception):
    def __init__(self, message):
        super().__init__(message)


class IdAlreadyExistsException(Exception):
    def __init__(self, message):
        super().__init__(message)
