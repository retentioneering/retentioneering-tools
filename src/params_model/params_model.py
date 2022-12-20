from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type, Union

from pydantic import BaseModel, ValidationError, validator
from typing_extensions import TypedDict

from src.params_model.registry import register_params_model
from src.widget import WIDGET, WIDGET_MAPPING, WIDGET_TYPE

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, MappingIntStrAny


class CustomWidgetProperties(TypedDict):
    widget: str
    serialize: Callable
    parse: Callable


class ParamsModel(BaseModel):

    _widgets: dict = {}

    @classmethod
    def __init_subclass__(cls: Type[ParamsModel], **kwargs: Any):
        super().__init_subclass__(**kwargs)
        obj = cls.__new__(cls)
        register_params_model(obj)

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
        try:
            super().__init__(**data)
        except ValidationError:
            for key in data:
                if key in self._widgets:
                    data[key] = self._widgets[key]._parse(data[key])
            super().__init__(**data)

    def __call__(self, **data: Dict[str, Any]) -> ParamsModel:
        ParamsModel.__init__(self, **data)
        return self

    @classmethod
    def _parse_schemas(cls) -> dict[str, str | dict | list]:
        params_schema: dict[str, Any] = cls.schema()
        for field_name, field in cls.__fields__.items():
            if getattr(field, "type_", None) is Callable:
                params_schema["properties"][field_name] = {
                    "title": field_name.title(),
                    "type": "callable",
                    "_source_code": cls._widgets[field_name]._serialize(value=field.default),
                }
        properties: dict[str, dict] = params_schema.get("properties", {})
        required: list[str] = params_schema.get("required", [])
        optionals = {name: name not in required for name in properties.keys()}
        definitions = params_schema.get("definitions", {})
        widgets = {}
        custom_widgets = cls._widgets
        for name, params in properties.items():
            custom_widget: dict[str, Any] | None = None
            default = params.get("default")
            if name in custom_widgets:  # type: ignore
                custom_widget = cls._parse_custom_widget(name=name, optional=optionals[name])

            if "$ref" in params or "allOf" in params:
                widget: dict[str, Any] = cls._parse_schema_definition(
                    params=params, definitions=definitions, default=default, optional=optionals[name]
                )
            elif "anyOf" in params:
                widget = cls._parse_anyof_schema_definition(
                    params, definitions, default=default, optional=optionals[name]
                )
            else:
                widget = cls._parse_simple_widget(name, params, optional=optionals[name], default=default)

            if custom_widget:
                if default := widget.get("default", None):
                    custom_widget["default"] = default
                if optional := widget.get("optional", None):
                    custom_widget["optional"] = optional
                if "name" not in custom_widget:
                    custom_widget["name"] = widget["name"]
                if "optional" not in custom_widget:
                    custom_widget["optional"] = False
                custom_widget["name"] = custom_widget["name"].lower().replace(" ", "_")
                widgets[name] = custom_widget
            elif widget:
                widgets[name] = widget
            else:
                raise ValueError("Not created widget")
        return widgets  # type: ignore

    @classmethod
    def _parse_schema_definition(
        cls,
        params: dict[str, dict[str, Any]] | Any,
        definitions: dict[str, Any],
        default: Any | None = None,
        optional: bool = True,
    ) -> dict[str, Any]:
        ref: str = params.get("$ref", "") or params.get("allOf", [{}])[0].get("$ref", "")  # type: ignore
        definition_name = ref.split("/")[-1]
        definition = definitions[definition_name]
        params = definition.get("enum", [])
        kwargs = {"name": definition_name, "widget": "enum", "default": default, "optional": optional, "params": params}
        return asdict(WIDGET_MAPPING["enum"].from_dict(**kwargs))

    @classmethod
    def _parse_anyof_schema_definition(
        cls,
        params: dict[str, dict[str, Any] | list[dict[str, Any]]] | Any,
        definitions: dict[str, Any],
        default: Any | None = None,
        optional: bool = True,
    ) -> dict[str, Any]:
        definition_name = params.get("title")
        enum_params = [x.get("enum", [None])[0] for x in params["anyOf"] if hasattr(x, "get")]  # type: ignore
        enum_params = list(filter(lambda x: x is not None, enum_params))
        kwargs: dict = {"name": definition_name, "widget": "enum", "default": default, "optional": optional}
        if len(enum_params) > 0:
            kwargs["params"] = enum_params

        widget_data: dict[str, Any] = asdict(WIDGET_MAPPING["enum"].from_dict(**kwargs))
        kwargs.update(widget_data)
        return kwargs

    @classmethod
    def _parse_simple_widget(
        cls, name: str, params: dict[str, Any], default: Any | None = None, optional: bool = False
    ) -> dict[str, Any]:
        widget_type = params.get("type")
        if widget_type == "array":
            widget_type = "enum"
        try:
            items = params.get("items", [{}])[-1]
        except KeyError:
            items = params.get("items", [{}])
        widget_params = dict(optional=optional, name=name, widget=widget_type)
        # widget_params["type"] = widget_type
        # widget_params["default"] = default
        if "enum" in items and widget_type != "enum":
            widget_type = "enum"
            widget_params["params"] = items["enum"]  # type: ignore

        return widget_params
        # try:
        #     widget: WIDGET_TYPE = WIDGET_MAPPING[widget_type]  # type: ignore
        #     return widget.from_dict(**widget_params)
        #
        # except KeyError:
        #     raise Exception("Not found widget. Define new widget for <%s> and add it to mapping." % widget_type)

    @classmethod
    def _parse_custom_widget(cls, name: str, optional: bool = False) -> dict[str, Any]:
        custom_widget = cls._widgets[name]  # type: ignore
        if isinstance(custom_widget, dict):
            _widget = WIDGET_MAPPING[custom_widget["widget"]]
            widget_type = custom_widget["widget"]
            return asdict(_widget.from_dict(**dict(optional=optional, name=name, widget=widget_type, value=None)))
        else:
            return asdict(custom_widget)

    @classmethod
    def get_widgets(cls) -> dict[str, str | dict | list]:
        return cls._parse_schemas()

    def dict(
        self,
        *,
        include: Optional[Union["AbstractSetIntStr", "MappingIntStrAny"]] = None,
        exclude: Optional[Union["AbstractSetIntStr", "MappingIntStrAny"]] = None,
        by_alias: bool = False,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Dict:
        data = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )
        for key in data:
            if widget := self._widgets.get(key, None):
                data[key] = widget._serialize(value=data[key])  # type: ignore
        return data
