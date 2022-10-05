from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict
from typing import Any, Callable, Dict, Optional

from pydantic import BaseModel, validator
from typing_extensions import TypedDict

from src.widget import WIDGET_MAPPING, WIDGET_TYPE


class CustomWidgetProperties(TypedDict):
    widget: str
    serialize: Callable
    parse: Callable


class CustomWidgetDataType(dict):
    custom_widgets: dict[str, CustomWidgetProperties]


class ParamsModel(BaseModel):

    class Options:
        custom_widgets: Optional[CustomWidgetDataType]

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

    def __init__(
        self,
        **data: Dict[str, Any],
    ) -> None:
        super().__init__(**data)

    @classmethod
    def _parse_schemas(cls) -> dict[str, Any]:
        params_schema: dict[str, Any] = cls.schema()
        # print(params_schema)
        # print(cls.schema())
        properties: dict[str, dict] = params_schema.get("properties", {})
        required: list[str] = params_schema.get("required", [])
        optionals = {name: name not in required for name in properties.keys()}
        definitions = params_schema.get("definitions", {})
        widgets = {}
        custom_widgets = getattr(getattr(cls, 'Options', None), 'custom_widgets', [])
        for name, params in properties.items():
            widget = None
            default = params.get('default')
            if name in custom_widgets:  # type: ignore
                widget = cls._parse_custom_widget(name=name, optional=optionals[name])
            elif "$ref" in params:
                widget = cls._parse_schema_definition(params, definitions, optional=optionals[name])
            elif "allOf" in params:
                widget = cls._parse_schema_definition(params, definitions, default=default, optional=optionals[name])

            else:
                widget = cls._parse_simple_widget(name, params, optional=optionals[name], default=default)

            if widget:
                widgets[name] = asdict(widget)
        return widgets

    @classmethod
    def _parse_schema_definition(
        cls,
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

    @classmethod
    def _parse_simple_widget(
            cls,
            name: str,
            params: dict[str, Any],
            default: Any | None = None,
            optional: bool = False
    ) -> WIDGET_TYPE:
        widget_type = params.get("type")
        print(params)
        try:
            items = params.get("items", [{}])[-1]
        except KeyError:
            items = params.get("items", [{}])
        widget_params = dict(optional=optional, name=name, widget=widget_type)
        widget_params['type'] = widget_type
        if 'enum' in items and widget_type != 'enum':
            widget_type = 'enum'
            widget_params['params'] = items['enum']
            widget_params['default'] = default


        print(f'{widget_type=} - {params=}')
        try:
            widget: Callable = WIDGET_MAPPING[widget_type]  # type: ignore
            return widget(**widget_params)

        except KeyError:
            raise Exception("Not found widget. Define new widget for <%s> and add it to mapping." % widget_type)

    @classmethod
    def _parse_custom_widget(cls, name: str, optional: bool = False) -> WIDGET_TYPE:
        custom_widget = cls.Options.custom_widgets[name]  # type: ignore
        _widget = WIDGET_MAPPING[custom_widget["widget"]]
        current_value = getattr(cls, name)
        serialized_value = custom_widget["serialize"](current_value)
        return _widget(optional=optional, name=name, widget=custom_widget["widget"], value=serialized_value)

    @classmethod
    def get_widgets(cls):
        return cls._parse_schemas()

    # @classmethod
    # def get_view(cls) -> list[dict[str, str]]:
    #     data: list[dict[str, str]] = []
    #     schema = cls.schema()
    #     properties = schema['properties']
    #     for field, property in properties.items():
    #         field_data = {
    #                 'name': field,
    #                 'widget': property['type'],
    #                 'default': true,
    #                 'optional': false
    #             }
    #         data.append(field_data)
    #     # for field, properties in cls.__fields__.items():
    #     #     print(field)
    #     #     print(properties)
    #     #     field_data = {
    #     #         'name': field,
    #     #         'widget': 'bool',
    #     #         'default': true,
    #     #         'optional': false
    #     #     }
    #
    #     return data
