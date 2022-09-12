from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Dict

from pydantic import BaseModel, validator


class ParamsModel(
    BaseModel,
    # ParamsModelWidget
):
    @validator("*")
    def validate_subiterable(cls, value: Any) -> Any:
        array_types = (Iterable, dict)
        if isinstance(value, array_types):
            try:
                if isinstance(value, dict):
                    subvalue = list(value.values())[0]
                else:
                    subvalue = next(iter(value))
                if (isinstance(subvalue, array_types) or hasattr(subvalue, "__getitem__")) and not isinstance(
                    subvalue, str
                ):
                    raise ValueError("Inner iterable or hashable not allowed!")
            except TypeError:
                pass
        return value

    def __init__(self, **data: Dict[str, Any]) -> None:
        super().__init__(**data)
