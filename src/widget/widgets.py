from __future__ import annotations

import inspect
import types
from dataclasses import dataclass, field
from typing import Callable, Type, Union


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
    params: list[str]
    default: str = ""
    widget: str = "enum"

    @classmethod
    def from_dict(cls, **kwargs) -> "EnumWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class ArrayWidget:
    name: str
    optional: bool
    default: str = ""
    widget: str = "array"

    @classmethod
    def from_dict(cls, **kwargs) -> "ArrayWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class BooleanWidget:
    name: str
    optional: bool
    widget: str = "boolean"

    @classmethod
    def from_dict(cls, **kwargs) -> "BooleanWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class ReteTimeWidget:
    name: str
    optional: bool
    widget: str = "time_widget"

    params: list = field(default_factory=list)

    @classmethod
    def from_dict(cls, **kwargs) -> "ReteTimeWidget":
        kwargs["params"] = [
            {"widget": "float"},
            {"widget": "enum", "params": ["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "μs", "ns", "ps", "fs", "as"]},
        ]
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls, value: tuple[float, str]) -> str:
        return ",".join([str(x) for x in value])

    @classmethod
    def _parse(cls, value: str) -> tuple[float, str]:  # type: ignore
        if type(value) is tuple:
            return value  # type: ignore

        TIME, QUANT = 0, 1
        data = value.split(",")
        if len(data) > 2:
            raise Exception("Incorrect input")
        return float(data[TIME]), str(data[QUANT])


@dataclass
class ReteFunction:
    name: str
    optional: bool
    widget: str = "function"
    _source_code: str = ""

    @classmethod
    def from_dict(cls, **kwargs) -> "ReteFunction":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls, value: Callable) -> str:
        try:
            code = inspect.getsource(value)
            cls._source_code = code
            return code
        except OSError:
            return cls._source_code

    @classmethod
    def _parse(cls, value: str) -> Callable:  # type: ignore
        code_obj = compile(value, "<string>", "exec")
        new_func_type = types.FunctionType(code_obj.co_consts[2], {})
        cls._source_code = value
        return new_func_type


@dataclass
class ListOfInt:
    name: str
    optional: bool
    widget: str = "list_of_int"

    @classmethod
    def from_dict(cls, **kwargs) -> "ListOfInt":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls, value: list[int] | None) -> str | None:
        if value is None:
            return None
        return ",".join([str(x) for x in value])

    @classmethod
    def _parse(cls, value: str) -> list[int] | None:  # type: ignore
        if type(value) is list:
            return value  # type: ignore
        if value is None:
            return None
        _data: list[str] = value.split(",")
        data = [int(x) for x in _data]
        return data


@dataclass
class ListOfIntNewUsers:
    # @TODO: remove this widget and make his functionality in ListOfInt
    name: str
    optional: bool
    params: list[str]
    widget: str = "list_of_int"

    @classmethod
    def from_dict(cls, **kwargs) -> "ListOfIntNewUsers":
        kwargs["params"] = [{"disable_value": "all"}]
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls, value: list[int] | None) -> str | None:
        if value is None:
            return None
        return ",".join([str(x) for x in value])

    @classmethod
    def _parse(cls, value: str) -> list[int] | None:  # type: ignore
        if type(value) is list:
            return value  # type: ignore
        if value is None:
            return None
        _data: list[str] = value.split(",")
        data = [int(x) for x in _data]
        return data


@dataclass
class ListOfString:
    name: str
    optional: bool
    widget: str = "list_of_string"

    @classmethod
    def from_dict(cls, **kwargs) -> "ListOfString":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls, value: list[str] | None) -> str | None:
        if value is None:
            return None
        return ",".join(value)

    @classmethod
    def _parse(cls, value: str) -> list[str] | None:  # type: ignore
        if type(value) is list:
            return value  # type: ignore
        if value is None:
            return None
        data: list[str] = value.split(",")
        return data


WIDGET_TYPE = Union[
    Type[StringWidget],
    Type[IntegerWidget],
    Type[EnumWidget],
    Type[ArrayWidget],
    Type[BooleanWidget],
    Type[ReteTimeWidget],
]
WIDGET = Union[StringWidget, IntegerWidget, EnumWidget, ArrayWidget, BooleanWidget, ReteTimeWidget]

# @TODO: make default dict
WIDGET_MAPPING: dict[str, WIDGET_TYPE] = {
    "string": StringWidget,
    "integer": IntegerWidget,
    "enum": EnumWidget,
    "array": ArrayWidget,
    "boolean": BooleanWidget,
    "tuple": ReteTimeWidget,
}
