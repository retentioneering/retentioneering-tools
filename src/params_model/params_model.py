from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict
from typing import Any, Callable, Dict, Optional, Type
from typing_extensions import TypedDict

from dill.source import getsource
from pydantic import BaseModel, ValidationError, validator
from pydantic.fields import FieldInfo, ModelField, Field
from pydantic.main import ModelMetaclass

from src.widget import WIDGET_MAPPING, WIDGET_TYPE

CUSTOM_WIDGET = Optional[Dict[str, Dict]]


# from pydantic.json import ENCODERS_BY_TYPE
# ENCODERS_BY_TYPE[Callable] = lambda _: getsource(_)
# ENCODERS_BY_TYPE['function'] = lambda _: getsource(_)

class CustomWidgetProperties(TypedDict):
    widget: str
    serialize: Callable
    parse: Callable

class CustomWidgetDataType(dict):
    custom_widgets: dict[str, CustomWidgetDataType]
    # @classmethod  # type: ignore
    # def __get_validators__(cls):
    #     yield cls.validate
    #
    # @classmethod  # type: ignore
    # def validate(cls, value: FieldInfo, field: ModelField):
    #     custom_widgets = field.field_info.extra.get("custom_widgets") or value.default
    #     custom_fields = custom_widgets.keys()
    #     required_params_for_widget = ["widget", "serialize", "parse"]
    #     for custom_field in custom_fields:
    #         widget_params = custom_widgets[custom_field]
    #         if not all([x in required_params_for_widget for x in widget_params]):
    #             raise ValidationError("Not all fields in <%s>" % custom_field)  # type: ignore
    #     return custom_widgets

    @classmethod
    def __modify_schema__(cls, field_schema, field: ModelField | None):
        print('CALL ME!')
        if field:
            field_schema['custom_widgets'] = ''

from pydantic.utils import ROOT_KEY, get_model, lenient_issubclass
from pydantic.typing import is_callable_type, get_origin
DictStrAny = Dict[str, Any]


def _my_callable_check(type_: Type[Any]) -> bool:
    print('AAAAA')
    print(f'{type_=}')
    return type_ is Callable or get_origin(type_) is Callable or type_ is CustomWidgetDataType

is_callable_type = _my_callable_check
#
# def get_field_info_schema(field: ModelField, schema_overrides: bool = False) -> Tuple[Dict[str, Any], bool]:
#
#     # If no title is explicitly set, we don't set title in the schema for enums.
#     # The behaviour is the same as `BaseModel` reference, where the default title
#     # is in the definitions part of the schema.
#     schema_: Dict[str, Any] = {}
#     if field.field_info.title or not lenient_issubclass(field.type_, Enum):
#         schema_['title'] = field.field_info.title or field.alias.title().replace('_', ' ')
#
#     if field.field_info.title:
#         schema_overrides = True
#
#     if field.field_info.description:
#         schema_['description'] = field.field_info.description
#         schema_overrides = True
#
#     if not field.required and field.default is not None and not is_callable_type(field.outer_type_):
#         schema_['default'] = encode_default(field.default)
#         schema_overrides = True
#
#     return schema_, schema_overrides


class _ReteMetaModel(ModelMetaclass):
    def __new__(cls, name: str, bases: tuple, namespace: dict, **kwargs: dict) -> type:
        obj = super().__new__(cls, name, bases, namespace, **kwargs)
        return obj

from pydantic.schema import default_ref_template, model_schema, get_flat_models_from_model, get_model_name_map, \
    get_schema_ref, TypeModelOrEnum, TypeModelSet, enum_process_schema, SkipField, warnings, is_namedtuple, \
    pydantic_encoder, get_field_schema_validations, field_type_schema
from typing import Union, Tuple, Set, cast
from enum import Enum


def encode_default(dft: Any) -> Any:
    if isinstance(dft, Enum):
        return dft.value
    elif isinstance(dft, (int, float, str)):
        return dft
    elif isinstance(dft, (list, tuple)):
        t = dft.__class__
        seq_args = (encode_default(v) for v in dft)
        return t(*seq_args) if is_namedtuple(t) else t(seq_args)
    elif isinstance(dft, dict):
        return {encode_default(k): encode_default(v) for k, v in dft.items()}
    elif dft is None:
        return None
    else:
        return pydantic_encoder(dft)


def get_field_info_schema(field: ModelField, schema_overrides: bool = False) -> Tuple[Dict[str, Any], bool]:

    # If no title is explicitly set, we don't set title in the schema for enums.
    # The behaviour is the same as `BaseModel` reference, where the default title
    # is in the definitions part of the schema.
    schema_: Dict[str, Any] = {}
    print(field.field_info)
    if field.field_info.title or not lenient_issubclass(field.type_, Enum):
        schema_['title'] = field.field_info.title or field.alias.title().replace('_', ' ')

    if field.field_info.title:
        schema_overrides = True

    if field.field_info.description:
        schema_['description'] = field.field_info.description
        schema_overrides = True

    if not field.required and field.default is not None and not is_callable_type(field.outer_type_):
        schema_['default'] = encode_default(field.default)
        schema_overrides = True

    return schema_, schema_overrides

def field_schema(
    field: ModelField,
    *,
    by_alias: bool = True,
    model_name_map: Dict[TypeModelOrEnum, str],
    ref_prefix: Optional[str] = None,
    ref_template: str = default_ref_template,
    known_models: TypeModelSet = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], Set[str]]:
    """
    Process a Pydantic field and return a tuple with a JSON Schema for it as the first item.
    Also return a dictionary of definitions with models as keys and their schemas as values. If the passed field
    is a model and has sub-models, and those sub-models don't have overrides (as ``title``, ``default``, etc), they
    will be included in the definitions and referenced in the schema instead of included recursively.

    :param field: a Pydantic ``ModelField``
    :param by_alias: use the defined alias (if any) in the returned schema
    :param model_name_map: used to generate the JSON Schema references to other models included in the definitions
    :param ref_prefix: the JSON Pointer prefix to use for references to other schemas, if None, the default of
      #/definitions/ will be used
    :param ref_template: Use a ``string.format()`` template for ``$ref`` instead of a prefix. This can be useful for
      references that cannot be represented by ``ref_prefix`` such as a definition stored in another file. For a
      sibling json file in a ``/schemas`` directory use ``"/schemas/${model}.json#"``.
    :param known_models: used to solve circular references
    :return: tuple of the schema for this field and additional definitions
    """
    s, schema_overrides = get_field_info_schema(field)

    validation_schema = get_field_schema_validations(field)
    if validation_schema:
        s.update(validation_schema)
        schema_overrides = True
    print(field)
    f_schema, f_definitions, f_nested_models = field_type_schema(
        field,
        by_alias=by_alias,
        model_name_map=model_name_map,
        schema_overrides=schema_overrides,
        ref_prefix=ref_prefix,
        ref_template=ref_template,
        known_models=known_models or set(),
    )

    # $ref will only be returned when there are no schema_overrides
    if '$ref' in f_schema:
        return f_schema, f_definitions, f_nested_models
    else:
        s.update(f_schema)
        return s, f_definitions, f_nested_models
def model_type_schema(
    model: Type['BaseModel'],
    *,
    by_alias: bool,
    model_name_map: Dict[TypeModelOrEnum, str],
    ref_template: str,
    ref_prefix: Optional[str] = None,
    known_models: TypeModelSet,
) -> Tuple[Dict[str, Any], Dict[str, Any], Set[str]]:
    """
    You probably should be using ``model_schema()``, this function is indirectly used by that function.

    Take a single ``model`` and generate the schema for its type only, not including additional
    information as title, etc. Also return additional schema definitions, from sub-models.
    """
    properties = {}
    required = []
    definitions: Dict[str, Any] = {}
    nested_models: Set[str] = set()
    for k, f in model.__fields__.items():
        try:
            f_schema, f_definitions, f_nested_models = field_schema(
                f,
                by_alias=by_alias,
                model_name_map=model_name_map,
                ref_prefix=ref_prefix,
                ref_template=ref_template,
                known_models=known_models,
            )
        except SkipField as skip:
            warnings.warn(skip.message, UserWarning)
            continue
        definitions.update(f_definitions)
        nested_models.update(f_nested_models)
        if by_alias:
            properties[f.alias] = f_schema
            if f.required:
                required.append(f.alias)
        else:
            properties[k] = f_schema
            if f.required:
                required.append(k)
    if ROOT_KEY in properties:
        out_schema = properties[ROOT_KEY]
        out_schema['title'] = model.__config__.title or model.__name__
    else:
        out_schema = {'type': 'object', 'properties': properties}
        if required:
            out_schema['required'] = required
    if model.__config__.extra == 'forbid':
        out_schema['additionalProperties'] = False
    return out_schema, definitions, nested_models

def model_process_schema(
    model: TypeModelOrEnum,
    *,
    by_alias: bool = True,
    model_name_map: Dict[TypeModelOrEnum, str],
    ref_prefix: Optional[str] = None,
    ref_template: str = default_ref_template,
    known_models: TypeModelSet = None,
    field: Optional[ModelField] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], Set[str]]:
    """
    Used by ``model_schema()``, you probably should be using that function.

    Take a single ``model`` and generate its schema. Also return additional schema definitions, from sub-models. The
    sub-models of the returned schema will be referenced, but their definitions will not be included in the schema. All
    the definitions are returned as the second value.
    """
    from inspect import getdoc, signature

    known_models = known_models or set()
    if lenient_issubclass(model, Enum):
        model = cast(Type[Enum], model)
        s = enum_process_schema(model, field=field)
        return s, {}, set()
    model = cast(Type['BaseModel'], model)
    s = {'title': model.__config__.title or model.__name__}
    doc = getdoc(model)
    if doc:
        s['description'] = doc
    known_models.add(model)
    m_schema, m_definitions, nested_models = model_type_schema(
        model,
        by_alias=by_alias,
        model_name_map=model_name_map,
        ref_prefix=ref_prefix,
        ref_template=ref_template,
        known_models=known_models,
    )
    s.update(m_schema)
    schema_extra = model.__config__.schema_extra
    if callable(schema_extra):
        if len(signature(schema_extra).parameters) == 1:
            schema_extra(s)
        else:
            schema_extra(s, model)
    else:
        s.update(schema_extra)
    return s, m_definitions, nested_models

def model_schema(
    model: Union[Type['BaseModel'], Type['Dataclass']],
    by_alias: bool = True,
    ref_prefix: Optional[str] = None,
    ref_template: str = default_ref_template,
) -> Dict[str, Any]:
    """
    Generate a JSON Schema for one model. With all the sub-models defined in the ``definitions`` top-level
    JSON key.

    :param model: a Pydantic model (a class that inherits from BaseModel)
    :param by_alias: generate the schemas using the aliases defined, if any
    :param ref_prefix: the JSON Pointer prefix for schema references with ``$ref``, if None, will be set to the
      default of ``#/definitions/``. Update it if you want the schemas to reference the definitions somewhere
      else, e.g. for OpenAPI use ``#/components/schemas/``. The resulting generated schemas will still be at the
      top-level key ``definitions``, so you can extract them from there. But all the references will have the set
      prefix.
    :param ref_template: Use a ``string.format()`` template for ``$ref`` instead of a prefix. This can be useful for
      references that cannot be represented by ``ref_prefix`` such as a definition stored in another file. For a
      sibling json file in a ``/schemas`` directory use ``"/schemas/${model}.json#"``.
    :return: dict with the JSON Schema for the passed ``model``
    """
    model = get_model(model)
    flat_models = get_flat_models_from_model(model)
    model_name_map = get_model_name_map(flat_models)
    model_name = model_name_map[model]
    m_schema, m_definitions, nested_models = model_process_schema(
        model, by_alias=by_alias, model_name_map=model_name_map, ref_prefix=ref_prefix, ref_template=ref_template
    )
    if model_name in nested_models:
        # model_name is in Nested models, it has circular references
        m_definitions[model_name] = m_schema
        m_schema = get_schema_ref(model_name, ref_prefix, ref_template, False)
    if m_definitions:
        m_schema.update({'definitions': m_definitions})
    return m_schema

class ParamsModel(BaseModel, metaclass=_ReteMetaModel):

    # custom_widgets: Optional[CustomWidgetDataType] = Field(custom_widgets=None, repr=False, exclude=True)
    custom_widgets: Optional[CustomWidgetDataType] = Field(custom_widgets=None, repr=False, exclude=True)

    @classmethod
    def schema(cls, by_alias: bool = True, ref_template: str = default_ref_template) -> 'DictStrAny':
        cached = cls.__schema_cache__.get((by_alias, ref_template))
        if cached is not None:
            return cached
        s = model_schema(cls, by_alias=by_alias, ref_template=ref_template)
        cls.__schema_cache__[(by_alias, ref_template)] = s
        return s

    class Config:

        arbitrary_types_allowed = True
        fields = {
            'custom_widgets': {
                'exclude': True
            }
        }


    @classmethod
    def _validate_custom_widgets(cls, value: Any) -> bool:
        if isinstance(value, dict) and all(
                [x in inner.keys() for x in ["widget", "serialize", "parse"] for inner in value.values()]
        ):
            return True
        return False

    @validator("*")
    def validate_subiterable(cls, value: Any) -> Any:
        if cls._validate_custom_widgets(value):
            return value
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

    def _parse_schemas(self) -> dict[str, Any]:
        params_schema: dict[str, Any] = self.schema()
        properties: dict[str, dict] = params_schema.get("properties", {})
        required: list[str] = params_schema.get("required", [])
        optionals = {name: name not in required for name in properties.keys()}
        definitions = params_schema.get("definitions", {})
        widgets = {}
        print(f'{self.custom_widgets=}')
        for name, params in properties.items():
            widget = None
            if name == "custom_widgets":
                pass
            # CustomWidget inherits from dict, but may be None
            elif name in self.custom_widgets:  # type: ignore
                widget = self._parse_custom_widget(name=name, optional=optionals[name])
            elif "$ref" in params:
                widget = self._parse_schema_definition(params, definitions, optional=optionals[name])
            elif "allOf" in params:
                default = params["default"]
                widget = self._parse_schema_definition(params, definitions, default=default, optional=optionals[name])

            else:
                widget = self._parse_simple_widget(name, params, optional=optionals[name])

            if widget:
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
        value = getattr(self, name, None)
        try:
            widget: Callable = WIDGET_MAPPING[widget_type]  # type: ignore
            return widget(optional=optional, name=name, widget=widget_type, value=value)

        except KeyError:
            raise Exception("Not found widget. Define new widget for %s and add it to mapping." % widget_type)

    def _parse_custom_widget(self, name: str, optional: bool = False) -> WIDGET_TYPE:
        custom_widget = self.custom_widgets[name]  # type: ignore
        _widget = WIDGET_MAPPING[custom_widget["widget"]]
        current_value = getattr(self, name)
        serialized_value = custom_widget["serialize"](current_value)
        return _widget(optional=optional, name=name, widget=custom_widget["widget"], value=serialized_value)

    def get_widgets(self):
        return self._parse_schemas()
