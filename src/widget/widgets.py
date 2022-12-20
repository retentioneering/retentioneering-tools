from __future__ import annotations

import inspect
import types
from dataclasses import dataclass, field
from typing import Any, Callable, Type, Union

from src.constants import DATETIME_UNITS_LIST


@dataclass
class StringWidget:
    widget: str = "string"

    @classmethod
    def from_dict(cls: Type[StringWidget], **kwargs: Any) -> "StringWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class IntegerWidget:
    widget: str = "integer"

    @classmethod
    def from_dict(cls: Type[IntegerWidget], **kwargs: Any) -> "IntegerWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class EnumWidget:
    params: list[str] | None = None
    default: str = ""
    widget: str = "enum"

    @classmethod
    def from_dict(cls: Type[EnumWidget], **kwargs: Any) -> "EnumWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class BooleanWidget:
    widget: str = "boolean"

    @classmethod
    def from_dict(cls: Type[BooleanWidget], **kwargs: Any) -> "BooleanWidget":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})


@dataclass
class ReteTimeWidget:
    widget: str = "time_widget"
    params: list = field(
        default_factory=lambda: [{"widget": "float"}, {"params": DATETIME_UNITS_LIST, "widget": "enum"}]
    )

    @classmethod
    def from_dict(cls: Type[ReteTimeWidget], **kwargs: Any) -> "ReteTimeWidget":
        kwargs["params"] = [
            {"widget": "float"},
            {"widget": "enum", "params": ["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "μs", "ns", "ps", "fs", "as"]},
        ]
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls: Type[ReteTimeWidget], value: tuple[float, str]) -> str:
        return ",".join([str(x) for x in value])

    @classmethod
    def _parse(cls: Type[ReteTimeWidget], value: str) -> tuple[float, str]:  # type: ignore
        if type(value) is tuple:
            return value  # type: ignore

        TIME, QUANT = 0, 1
        data = value.split(",")
        if len(data) > 2:
            raise Exception("Incorrect input")
        return float(data[TIME]), str(data[QUANT])


@dataclass
class ReteFunction:
    widget: str = "function"
    _source_code: str = ""

    @classmethod
    def from_dict(cls: Type[ReteFunction], **kwargs: Any) -> "ReteFunction":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls: Type[ReteFunction], value: Callable) -> str:
        try:
            code = inspect.getsource(value)
            cls._source_code = code
            return code
        except OSError:
            return cls._source_code

    @classmethod
    def _parse(cls: Type[ReteFunction], value: str) -> Callable:  # type: ignore
        code_obj = compile(value, "<string>", "exec")
        new_func_type = types.FunctionType(code_obj.co_consts[2], {})
        cls._source_code = value
        return new_func_type


@dataclass
class ListOfInt:
    widget: str = "list_of_int"

    @classmethod
    def from_dict(cls: Type[ListOfInt], **kwargs: Any) -> "ListOfInt":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls: Type[ListOfInt], value: list[int] | None) -> list[int] | None:
        return value

    @classmethod
    def _parse(cls: Type[ListOfInt], value: list[int]) -> list[int] | None:  # type: ignore
        return value


@dataclass
class ListOfIntNewUsers:
    # @TODO: remove this widget and make his functionality in ListOfInt. Vladimir Makhanov
    name: str = "new_users_list"
    params: dict[str, str] | None = field(default_factory=lambda: {"disable_value": "all"})
    widget: str = "list_of_int"

    @classmethod
    def from_dict(cls: Type[ListOfIntNewUsers], **kwargs: Any) -> "ListOfIntNewUsers":
        kwargs["params"] = {"disable_value": "all"}
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls: Type[ListOfIntNewUsers], value: list[int] | None) -> list[int] | None:
        return value

    @classmethod
    def _parse(cls: Type[ListOfIntNewUsers], value: list[int]) -> list[int] | None:  # type: ignore
        return value


@dataclass
class ListOfString:
    widget: str = "list_of_string"

    @classmethod
    def from_dict(cls: Type[ListOfString], **kwargs: Any) -> "ListOfString":
        return cls(**{k: v for k, v in kwargs.items() if k in inspect.signature(cls).parameters})

    @classmethod
    def _serialize(cls: Type[ListOfString], value: list[str] | None) -> list[str] | None:
        return value

    @classmethod
    def _parse(cls: Type[ListOfString], value: list[str]) -> list[str] | None:  # type: ignore
        return value


WIDGET_TYPE = Union[
    Type[StringWidget],
    Type[IntegerWidget],
    Type[EnumWidget],
    Type[BooleanWidget],
    Type[ReteTimeWidget],
]
WIDGET = Union[StringWidget, IntegerWidget, EnumWidget, BooleanWidget, ReteTimeWidget]

# @TODO: make default dict. Vladimir Makhanov
WIDGET_MAPPING: dict[str, WIDGET_TYPE] = {
    "string": StringWidget,
    "integer": IntegerWidget,
    "enum": EnumWidget,
    "boolean": BooleanWidget,
    "tuple": ReteTimeWidget,
}
