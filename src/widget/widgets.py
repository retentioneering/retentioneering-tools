from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Union


@dataclass
class StringWidget:
    name: str
    optional: bool
    widget: str = "string"


@dataclass
class IntegerWidget:
    name: str
    optional: bool
    widget: str = "integer"


@dataclass
class EnumWidget:
    name: str
    optional: bool
    default: str
    params: list[str]
    widget: str = "enum"


# @TODO: make default dict
WIDGET_MAPPING: dict[str, Callable] = {"string": StringWidget, "integer": IntegerWidget, "enum": EnumWidget}
WIDGET_TYPE = Union[StringWidget, IntegerWidget, EnumWidget]
