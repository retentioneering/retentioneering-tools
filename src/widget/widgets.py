from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Protocol, Type, Union


class DataclassProtocol(Protocol):
    __dataclass_fields__: Dict
    __dataclass_params__: Dict
    __post_init__: Optional[Callable]


@dataclass
class StringWidget:
    name: str
    optional: bool
    widget: str = "string"

    @classmethod
    def from_dict(cls, **kwargs) -> "StringWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class IntegerWidget:
    name: str
    optional: bool
    widget: str = "integer"

    @classmethod
    def from_dict(cls, **kwargs) -> "IntegerWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class EnumWidget:
    name: str
    optional: bool
    default: str
    params: list[str]
    widget: str = "enum"

    @classmethod
    def from_dict(cls, **kwargs) -> "EnumWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class ArrayWidget:
    name: str
    optional: bool
    default: str
    type: str
    widget: str = "array"

    @classmethod
    def from_dict(cls, **kwargs) -> "ArrayWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class BooleanWidget:
    name: str
    optional: bool
    default: str
    type: str
    widget: str = "boolean"

    @classmethod
    def from_dict(cls, **kwargs) -> "BooleanWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


WIDGET_TYPE = Union[Type[StringWidget], Type[IntegerWidget], Type[EnumWidget], Type[ArrayWidget], Type[BooleanWidget]]
WIDGET = Union[StringWidget, IntegerWidget, EnumWidget, ArrayWidget, BooleanWidget]

# @TODO: make default dict
WIDGET_MAPPING: dict[str, WIDGET_TYPE] = {
    "string": StringWidget,
    "integer": IntegerWidget,
    "enum": EnumWidget,
    "array": ArrayWidget,
    "boolean": BooleanWidget,
}
