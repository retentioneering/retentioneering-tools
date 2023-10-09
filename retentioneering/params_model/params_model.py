from __future__ import annotations

import warnings
from collections.abc import Iterable
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type, Union

from pydantic import BaseModel, ValidationError, validator
from typing_extensions import TypedDict

from retentioneering.exceptions.widget import WidgetParseError
from retentioneering.params_model.registry import register_params_model
from retentioneering.utils.dict import clear_dict
from retentioneering.widget import WIDGET_MAPPING

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
        register_params_model(cls)

    @validator("*")
    def validate_subiterable(cls, value: Any) -> Any:
        array_types = (Iterable, dict)
        if isinstance(value, array_types):
            try:
                if isinstance(value, dict):
                    subvalue = list(value.values())[0]
                else:
                    try:
                        subvalue = next(iter(value))
                    except StopIteration:
                        # empty iterator
                        subvalue = None
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
        except ValidationError as v_error:
            for key in data:
                if key in self._widgets:
                    try:
                        data[key] = self._widgets[key]._parse(data[key])
                    except WidgetParseError as parse_err:
                        parse_err.field_name = key
                        raise parse_err
            super().__init__(**data)

    def __call__(self, **data: Dict[str, Any]) -> ParamsModel:
        ParamsModel.__init__(self, **data)
        return self

    @classmethod
    def _parse_schemas(cls) -> dict[str, str | dict | list]:
        with warnings.catch_warnings():
            # disable warning for pydantic schema Callable type
            warnings.simplefilter(action="ignore", category=UserWarning)
            params_schema: dict[str, Any] = cls.schema()

        params_schema["required"] = params_schema.get("required", [])

        for field_name, field in cls.__fields__.items():
            field_type = getattr(field, "type_", None)
            # TODO: python3.8 fix
            field_type_classname_legacy = getattr(field_type, "_name", None)
            field_type_classname = getattr(field_type, "__name__", None)

            if field.required and field_name not in params_schema["required"]:
                params_schema["required"].append(field_name)

            if field_type_classname == "Callable" or field_type_classname_legacy == "Callable":
                params_schema["properties"][field_name] = {
                    "title": field_name.title(),
                    "type": "callable",
                }
            params_schema["properties"][field_name]["default"] = field.default

        properties: dict[str, dict] = params_schema.get("properties", {})
        required: list[str] = params_schema.get("required", [])
        optionals = {name: name not in required for name in properties.keys()}
        definitions = params_schema.get("definitions", {})
        widgets = {}
        custom_widgets = cls._widgets
        for name, params in properties.items():
            custom_widget: dict[str, Any] | None = None
            default = params.get("default", None)
            if name in custom_widgets:  # type: ignore
                custom_widget = cls._parse_custom_widget(name=name, optional=optionals[name])

            if "$ref" in params or "allOf" in params:
                widget: dict[str, Any] = cls._parse_schema_definition(
                    params=params, definitions=definitions, default=default, optional=optionals[name]
                )
            elif params.get("type") == "string" and "enum" in params:
                widget = cls._parse_enum_schema_definition(
                    name=name, params=params, definitions=definitions, default=default, optional=optionals[name]
                )
            elif "anyOf" in params:
                widget = cls._parse_anyof_schema_definition(
                    params, definitions, default=default, optional=optionals[name]
                )
            else:
                widget = cls._parse_simple_widget(name, params, optional=optionals[name], default=default)

            if custom_widget:
                widget_optional = custom_widget.get("optional", widget.get("optional", False))
                widget_name = widget["name"] if "name" not in custom_widget else None
                widget_default = cls._get_serialized_default_value(
                    field_name=name,
                    widget_type=custom_widget.get("widget", None),
                    field_default=default,
                    widget_default=custom_widget.get("default", None),
                )
                custom_widget_data = {
                    "default": widget_default,
                    "optional": widget_optional,
                    "name": widget_name,
                }
                if isinstance(custom_widget_data["name"], str):
                    custom_widget_data["name"] = custom_widget_data["name"].lower().replace(" ", "_")

                custom_widget.update(clear_dict(custom_widget_data))
                widgets[name] = custom_widget
            elif widget:
                widget["default"] = cls._get_serialized_default_value(
                    field_name=name,
                    widget_type=widget.get("widget", None),
                    field_default=default,
                    widget_default=widget.get("default", None),
                )
                widgets[name] = widget
            else:
                raise ValueError("Not created widget")
        return widgets  # type: ignore

    @classmethod
    def _get_serialized_default_value(
        cls, field_name: str, widget_type: Optional[str] = None, field_default: Any = None, widget_default: Any = None
    ) -> Any:
        widget = WIDGET_MAPPING.get(widget_type, None) if widget_type else None
        default = widget_default if field_default is None else field_default

        # если дефолтного значения нет - значит его не нужно сериализовывать
        if default is None:
            return default

        serialize: None | Callable = None

        if field_name in cls._widgets:
            custom_widget = cls._widgets[field_name]  # type: ignore
            if isinstance(custom_widget, dict):
                serialize = custom_widget.get("_serialize", None)
            elif hasattr(custom_widget, "_serialize"):
                serialize = custom_widget._serialize

        if widget and hasattr(widget, "_serialize"):
            serialize = widget._serialize  # type: ignore

        if serialize is not None:
            return serialize(default)

        return default

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
    def _parse_enum_schema_definition(
        cls,
        name: str,
        params: dict[str, dict[str, Any] | list[dict[str, Any]]] | Any,
        definitions: dict[str, Any],
        default: Any | None = None,
        optional: bool = True,
    ) -> dict[str, Any]:
        enum_params = params.get("enum")

        if not isinstance(enum_params, list):
            raise ValueError("unexpected enum value")

        enum_params = list(filter(lambda x: x is not None, enum_params))  # type: ignore
        kwargs: dict = {"name": name, "widget": "enum", "default": default, "optional": optional}
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
        widget_params = dict(optional=optional, name=name, widget=widget_type, default=default)

        if "enum" in items and widget_type != "enum":
            widget_type = "enum"
            widget_params["params"] = items["enum"]  # type: ignore

        return widget_params

    @classmethod
    def _parse_custom_widget(cls, name: str, optional: bool = False) -> dict[str, Any]:
        custom_widget = cls._widgets[name]  # type: ignore
        if isinstance(custom_widget, dict):
            _widget = WIDGET_MAPPING[custom_widget["widget"]]
            widget_type = custom_widget["widget"]
            return asdict(_widget.from_dict(**dict(optional=optional, name=name, widget=widget_type, value=None)))
        else:
            widget = asdict(custom_widget)
            if "optional" not in widget or widget["optional"] is None:
                widget["optional"] = optional
            return widget

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
