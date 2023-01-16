import unittest
from typing import List, Literal, Union

import pytest
from pydantic import BaseModel, ValidationError

from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.params_model import ParamsModel


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
        valid_params: StubProcessorParams = StubProcessorParams(a="a")  # type: ignore
        stub = StubProcessor(params=valid_params)
        self.assertEqual(stub.params.dict(), valid_params)

    def test_set_params(self):
        with self.assertRaises(ValidationError):
            invalid_params: StubProcessorParams = StubProcessorParams(a="d")  # type: ignore
            StubProcessor(params=invalid_params)

    def test_update_params(self):
        valid_params: StubProcessorParams = StubProcessorParams(a="a")  # type: ignore
        stub = StubProcessor(params=valid_params)
        stub.params(**{"a": "b"})
        assert stub.params.dict()["a"] == "b"


def test_params_not_subclasses() -> None:
    with pytest.raises(KeyError):

        class PydanticModel(BaseModel):
            a: List[int]

            def get_widgets(self) -> dict:
                return {}

        model = PydanticModel(a=[1, 2, 3])

        class StubPydanticProcessor(DataProcessor):
            def __init__(self, params: PydanticModel):
                super().__init__(params=params)  # type: ignore

        processor = StubPydanticProcessor(params=model)
