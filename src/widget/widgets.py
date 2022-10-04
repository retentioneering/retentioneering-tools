from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Union


@dataclass
class StringWidget:
    name: str
    optional: bool
    value: str
    widget: str = "string"


@dataclass
class IntegerWidget:
    name: str
    optional: bool
    value: int
    widget: str = "integer"


@dataclass
class EnumWidget:
    name: str
    optional: bool
    default: str
    params: list[str]
    value: Any
    widget: str = "enum"


# @TODO: make default dict
WIDGET_MAPPING: dict[str, Callable] = {"string": StringWidget, "integer": IntegerWidget, "enum": EnumWidget}
WIDGET_TYPE = Union[StringWidget, IntegerWidget, EnumWidget]
