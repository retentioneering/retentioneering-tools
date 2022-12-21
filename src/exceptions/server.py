from .base import BaseReteException
from typing import Any, Optional
import json


class ServerNotFoundActionError(BaseReteException):
    message: str
    method: str

    def __init__(self, message: str, method: str):
        self.message = message
        self.method = method


class ServerNotFound(BaseReteException):
    pass

class ServerErrorWithResponse(BaseReteException):
    type: str
    message: str
    errors: Optional[Any]

    def __init__(self, message: str, type: str, errors: Any = None):
        self.message = message
        self.type = type
        self.errors = errors

    def dict(self):
        # check errors serializable
        try:
            json.dumps(self.errors)
        except:
            return {
                "type": self.type,
                "msg": "serialize error response error!",
            }
        return {
            "type": self.type,
            "msg": self.message,
            "errors": self.errors,
        }



