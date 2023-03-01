import json
import traceback
from typing import Any, Optional, TypedDict

from .base import BaseReteException


class ServerNotFoundActionError(BaseReteException):
    message: str
    method: str

    def __init__(self, message: str, method: str):
        self.message = message
        self.method = method


class ServerNotFound(BaseReteException):
    pass


class ServerErrorDict(TypedDict):
    type: str
    msg: str
    errors: Optional[Any]
    formatted: str


class ServerErrorWithResponse(BaseReteException):
    type: str
    message: str
    errors: Optional[Any]
    formatted: str

    def __init__(self, message: str, type: str, errors: Any = None):
        self.message = message
        self.type = type
        self.errors = errors
        self.formatted = traceback.format_exc()

    def dict(self) -> ServerErrorDict:
        # check errors serializable
        message = self.message
        errors = self.errors

        try:
            json.dumps(self.errors)
        except:
            message = "serialize error response error!"
            errors = None

        return {
            "type": self.type,
            "msg": message,
            "errors": errors,
            "formatted": self.formatted,
        }
