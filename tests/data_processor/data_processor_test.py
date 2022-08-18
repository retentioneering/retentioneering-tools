import unittest
from typing import Literal, Union, cast, List

import pytest
from pydantic import ValidationError, BaseModel

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


def test_params_not_subclasses() -> None:
    with pytest.raises(TypeError):
        class PydanticModel(BaseModel):
            a: List[int]

        model = PydanticModel(a=[1, 2, 3])

        class StubPydanticProcessor(DataProcessor):

            def __init__(self, params: PydanticModel):
                super().__init__(params=params)

        processor = StubPydanticProcessor(params=model)
