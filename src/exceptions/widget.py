from .base import BaseReteException
from typing import Optional

class WidgetParseError(BaseReteException):
    field_name: Optional[str] 

    def __init__(self, *args: object) -> None:
        self.field_name = None
        super().__init__(*args)

class ParseReteFuncError(WidgetParseError):
    pass