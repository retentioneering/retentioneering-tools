from __future__ import annotations

import typing
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Dict

from pydantic import BaseModel, validator


@dataclass
class Widget:
    name: str
    widget: str
    optional: bool = False


@dataclass
class StringWidget(Widget):
    widget: str = 'str'


widget_mapping = {
    str: StringWidget
}


# class ParamsModelWidget:
#     widgets: list[Widget]
#

class ParamsModel(
    BaseModel,
    # ParamsModelWidget
):

    __widgets: typing.Dict[str, Widget]

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
        self.__widgets = self._define_widgets()
        super().__init__(**data)
        # print(hints)
        print(data)

    def _define_widgets(self) -> dict[str, Widget]:
        hints = typing.get_type_hints(self)
        widgets: dict[str, Widget] = dict()
        for widget_name, widget_type in hints.items():
            type_widget = widget_mapping.get(widget_type)

            widgets[widget_name] = type_widget(
                name=widget_name,
                optional=False
            )
        return widgets
