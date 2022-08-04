
from typing import TypedDict
import unittest
from .params_model import Numeric, ParamsModel, ModelDefinition, String, Enum, AvailableType, register_type, has_type, reset_types


class Zero(AvailableType[int]):
    def is_vaid(self, value: int):
        return value == 0

    @staticmethod
    def create(params: None):
        return Zero()


class TestRegisterType(unittest.TestCase):
    def tearDown(self):
        reset_types()

    def test_add_type(self):
        register_type(Zero)
        self.assertTrue(has_type(Zero))

    def test_reset_types(self):
        register_type(Zero)
        reset_types()
        self.assertFalse(has_type(Zero))


class TestParamsModel(unittest.TestCase):
    def test_get_definition(self):
        class Fields(TypedDict):
            a: str
            b: int
            c: str

        model = ParamsModel[Fields](
            fields={
                "a": "test",
                "b": 333,
                "c": "first"
            },
            fields_schema={
                "a": String(),
                "b": Numeric(),
                "c": Enum(["first", "second"])
            }
        )

        definition = model.get_definition()

        self.assertEqual(definition, {
            "a": {
                "type_name": "String",
                "type_params": None
            },
            "b": {
                "type_name": "Numeric",
                "type_params": None
            },
            "c": {
                "type_name": "Enum",
                "type_params": ["first", "second"]
            }
        })

    def test_from_definition(self):
        class Fields(TypedDict):
            a: str
            b: int
            c: str

        definition: ModelDefinition = {
            "a": {
                "type_name": "String",
                "type_params": None
            },
            "b": {
                "type_name": "Numeric",
                "type_params": None
            },
            "c": {
                "type_name": "Enum",
                "type_params": ["first", "second"]
            }
        }

        fields: Fields = {
            "a": "test",
            "b": 123,
            "c": "first"
        }

        model = ParamsModel.from_definition(
            fields=fields,
            definition=definition,
        )
        fields_schema = model.fields_schema

        # обратимость
        self.assertEqual(model.get_definition(), definition)

        self.assertTrue(
            set(("a", "b", "c")) == set(fields_schema.keys())
        )
        self.assertIsInstance(fields_schema["a"], String)
        self.assertIsInstance(fields_schema["b"], Numeric)
        self.assertIsInstance(fields_schema["c"], Enum)
        self.assertEqual(fields_schema["c"].params(), ["first", "second"])

    def test_is_valid(self):
        class Fields(TypedDict, total=False):
            a: str
            b: int
            c: str
            d: str

        fields: Fields = {
            "a": "test",
            "b": 3,
            "c": "first"
        }

        model = ParamsModel[Fields](
            fields=fields,
            fields_schema={
                "a": String(),
                "b": Numeric(),
                "c": Enum(["first", "second"]),
                "d": String(optional=True),
            }
        )

        self.assertTrue(model.is_valid({
            "a": "test",
            "b": 3,
            "c": "first",
            "d": "optional_parameter"
        }))
        self.assertTrue(model.is_valid({
            "a": "test",
            "b": 3,
            "c": "first"
        }))
        self.assertFalse(model.is_valid({
            "a": "test",
            "b": 3,
            "c": "unknown",
        }))
        self.assertFalse(model.is_valid({
            "a": "test",
            "b": 3,
            "c": "first",
            "unknown_key": 234234324
        }))
        self.assertFalse(model.is_valid({
            "a": "test",
            "b": 3
        }))

    def test_set_fields(self):
        class Fields(TypedDict, total=False):
            a: str
            b: int
            c: str
            d: str

        fields: Fields = {
            "a": "test",
            "b": 3,
            "c": "first"
        }

        model = ParamsModel[Fields](
            fields=fields,
            fields_schema={
                "a": String(),
                "b": Numeric(),
                "c": Enum(["first", "second"]),
                "d": String(optional=True),
            }
        )

        imvalid_fields: Fields = {
            "a": "test",
            "b": 3,
            "c": "unknown",
        }
        valid_fields: Fields = {
            "a": "test",
            "b": 3,
            "c": "first",
            "d": "optional_parameter"
        }

        def set_invalid_fields():
            model.fields = imvalid_fields

        self.assertRaises(ValueError, set_invalid_fields)
        model.fields = valid_fields
        self.assertEqual(model.fields, valid_fields)
