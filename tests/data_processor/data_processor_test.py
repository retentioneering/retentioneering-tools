import unittest
from typing import Literal, Union, cast

from pydantic import ValidationError

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class StubProcessorParams(ParamsModel):
    a: Union[Literal["a"], Literal["b"]]


class StubProcessor(DataProcessor):
    params: StubProcessorParams

    def __init__(self, params: StubProcessorParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        return eventstream.copy()


class TestDataProcessor(unittest.TestCase):
    def test_set_valid_params(self):
        valid_params: StubProcessorParams = StubProcessorParams(**{
            "a": "a"
        })
        stub = StubProcessor(params=valid_params)
        self.assertEqual(stub.params.dict(), valid_params)

    def test_set_params(self):
        with self.assertRaises(ValidationError):
            invalid_params: StubProcessorParams = StubProcessorParams(**{'a': 'd'})
            StubProcessor(params=invalid_params)
