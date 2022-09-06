class DmlError(Exception):
    pass


class ApiError(DmlError):
    pass


class DatumError(DmlError):
    pass


class NodeError(DmlError):
    """Nodes that fail with throw this error

    A standard way to catch errors is:

    >>> try:
    ...     y = f(x)
    ... except NodeError:
    ...     y = None
    """
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'NodeError: ' + self.message
