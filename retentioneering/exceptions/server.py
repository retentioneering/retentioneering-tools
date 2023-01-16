from .base import BaseReteException


class ServerNotFoundActionError(BaseReteException):
    message: str
    method: str

    def __init__(self, message: str, method: str):
        self.message = message
        self.method = method


class ServerNotFound(BaseReteException):
    pass
