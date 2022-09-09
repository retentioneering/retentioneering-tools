from __future__ import annotations

import uuid
from dataclasses import asdict
from typing import Any, Callable

from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel
from src.widget import WIDGET_MAPPING, WIDGET_TYPE


class DataProcessor:
    params: ParamsModel

    def __init__(self, params: ParamsModel | Any) -> None:
        if not issubclass(type(params), ParamsModel):
            raise TypeError("params is not subclass of ParamsModel")

        self.params = params
        self.pk = uuid.uuid4()

    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError

    def export(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        widgets: dict[str, Any] = self._parse_schemas()
        data["name"] = self.__class__.__name__
        data["pk"] = str(self.pk)
        data["schema"] = self.params.schema()
        data["widgets"] = widgets
        return data

    def _parse_schemas(self) -> dict[str, Any]:
        params_schema: dict[str, Any] = self.params.schema()
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
