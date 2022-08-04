from __future__ import annotations
from abc import abstractmethod
import json
from typing import Any, Callable, Dict, Generic, List, Optional, Type, TypeVar, TypedDict, Union, cast
from utils.list import find_item

T = TypeVar("T")


class AvailableType(Generic[T]):
    optional: Optional[bool]
    default: Optional[T]

    def __init__(self, params: Any = None, optional: bool = False, default: Optional[T] = None) -> None:
        self.default = default
        self.optional = optional

    @abstractmethod
    def is_valid(self, value: Any) -> bool:
        raise ValueError("is_valid implementation not found")

    def params(self) -> Any:
        return None


class String(AvailableType[str]):
    def is_valid(self, value: str):
        return type(value) is str


class Enum(AvailableType[str]):
    values: List[str]

    def __init__(self, values: List[str], optional: bool = False, default: Optional[str] = None):
        super().__init__(params=values, optional=optional, default=default)
        self.values = values

    def is_valid(self, value: str):
        return value in self.values

    def params(self):
        return self.values


class Numeric(AvailableType[Union[int, float]]):
    def is_valid(self, value: Any):
        return type(value) is int or type(value) is float


class Func(AvailableType[Callable[[Any], Any]]):
    def is_valid(self, value: Any) -> bool:
        return callable(value)


DEFAULT_TYPES: List[Type[AvailableType[Any]]] = [String, Enum, Numeric, Func]
available_types: List[Type[AvailableType[Any]]] = DEFAULT_TYPES.copy()


def register_type(new_type: Type[AvailableType[Any]]):
    if (new_type in available_types):
        return
    available_types.append(new_type)


def has_type(some_type: Type[AvailableType[Any]]):
    return some_type in available_types


def reset_types():
    available_types.clear()
    available_types.extend(DEFAULT_TYPES)


class TypeDefinition(TypedDict):
    type_name: str
    type_params: Any


ModelDefinition = Dict[str, TypeDefinition]


class SerializedModel(TypedDict):
    definition: ModelDefinition
    fields: TypedDict


# TODO добавить поддержку  сериализации
V = TypeVar('V', bound=TypedDict)


class ParamsModel(Generic[V]):
    __fields: V
    fields_schema: Dict[str, AvailableType[Any]]

    @property
    def fields(self) -> V:
        return self.__fields

    @fields.setter
    def fields(self, fields: V):
        if not self.is_valid(fields):
            raise ValueError("params model: invalid fields")
        self.__fields = fields
        self.__write_default_fields

    def __init__(self, fields: V, fields_schema: Dict[str, AvailableType[Any]]):
        self.fields_schema = fields_schema
        self.fields = fields
        self.__write_default_fields()

    def get_definition(self):
        definition: ModelDefinition = {}

        for field_name, type_instance in self.fields_schema.items():
            definition[field_name] = {
                "type_name": type(type_instance).__name__,
                "type_params": type_instance.params(),
            }

        return definition

    @staticmethod
    def from_definition(fields: V, definition: ModelDefinition):
        fields_schema: Dict[str, AvailableType[Any]] = {}

        for field_name, type_definition in definition.items():
            type_name = type_definition["type_name"]
            type_params = type_definition["type_params"]
            MatchedType = find_item(available_types, lambda t: t.__name__ ==
                                    type_name)

            if MatchedType is None:
                raise ValueError(f"unknown type: {type_name}")

            fields_schema[field_name] = MatchedType(type_params)

        return ParamsModel[V](fields=fields, fields_schema=fields_schema)

    def to_json(self):
        return json.dumps({
            "definition": self.get_definition(),
            "fields": self.fields,
        })

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        definition = data["definition"]
        fields = data["fields"]

        # TODO: validate
        return ParamsModel.from_definition(fields=fields, definition=definition)

    def is_valid(self, value: Union[Dict[str, Any], TypedDict]):
        if type(value) is not dict:
            raise ValueError("value is not dict")

        for feild_name, type_instance in self.fields_schema.items():
            if not type_instance.optional and feild_name not in value:
                return False

        for field_name, field_value in value.items():
            if field_name not in self.fields_schema:
                return False

            is_valid = self.fields_schema[field_name].is_valid(field_value)

            if not is_valid:
                return False

        return True

    def __write_default_fields(self):
        for field_name, type_instance in self.fields_schema.items():
            if type_instance.default and field_name not in self.__fields:
                cast(Dict[str, Any], self.__fields)[
                    field_name] = type_instance.default
