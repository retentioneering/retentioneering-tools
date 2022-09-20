from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict
from typing import Any, Callable, Dict

from pydantic import BaseModel, validator

from widget import WIDGET_MAPPING, WIDGET_TYPE


class ParamsModel(BaseModel):
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

    def _parse_schemas(self) -> dict[str, Any]:
        params_schema: dict[str, Any] = self.schema()
        properties: dict[str, dict] = params_schema.get("properties", {})
        required: list[str] = params_schema.get("required", [])
        optionals = {name: name not in required for name in properties.keys()}
        definitions = params_schema.get("definitions", {})
        widgets = {}
        for name, params in properties.items():
            if "$ref" in params:
                widget = self._parse_schema_definition(params, definitions, optional=optionals[name])
            elif "allOf" in params:
                default = params["default"]
                widget = self._parse_schema_definition(params, definitions, default=default, optional=optionals[name])

            else:
                widget = self._parse_simple_widget(name, params, optional=optionals[name])
            widgets[name] = asdict(widget)
        return widgets

    def _parse_schema_definition(
        self,
        params: dict[str, dict[str, Any]] | Any,
        definitions: dict[str, Any],
        default: Any | None = None,
        optional: bool = True,
    ) -> WIDGET_TYPE:
        ref: str = params.get("$ref", "") or params.get("allOf", [{}])[0].get("$ref", "")  # type: ignore
        definition_name = ref.split("/")[-1]
        definition = definitions[definition_name]
        params = definition.get("enum", [])
        kwargs = {"name": definition_name, "widget": "enum", "default": default, "optional": optional, "params": params}
        return WIDGET_MAPPING["enum"](**kwargs)

    def _parse_simple_widget(self, name: str, params: dict[str, Any], optional: bool = False) -> WIDGET_TYPE:
        widget_type = params.get("type")
        try:
            widget: Callable = WIDGET_MAPPING[widget_type]  # type: ignore
            return widget(
                optional=optional,
                name=name,
                widget=widget_type,
            )

        except KeyError:
            raise Exception("Not found widget. Define new widget for %s and add it to mapping." % widget_type)

    def get_widgets(self):
        return self._parse_schemas()
