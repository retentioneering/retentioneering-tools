from typing import List

import pytest
from pydantic import BaseModel, ValidationError

from retentioneering.data_processor import DataProcessor
from retentioneering.data_processor.registry import unregister_dataprocessor
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.params_model import ParamsModel
from tests.data_processor.fixtures.stub_processor import stub_processor


class TestDataProcessor:
    def test_set_valid_params(self, stub_processor):
        StubProcessorParams: ParamsModel = stub_processor["params"]
        StubProcessor: DataProcessor = stub_processor["processor"]

        valid_params: StubProcessorParams = StubProcessorParams(a="a")  # type: ignore
        stub = StubProcessor(params=valid_params)
        assert stub.params.dict() == valid_params

    def test_set_params(self, stub_processor):
        StubProcessorParams: ParamsModel = stub_processor["params"]
        StubProcessor: DataProcessor = stub_processor["processor"]
        with pytest.raises(ValidationError):
            invalid_params: StubProcessorParams = StubProcessorParams(a="d")  # type: ignore
            StubProcessor(params=invalid_params)

    def test_update_params(self, stub_processor):
        StubProcessorParams: ParamsModel = stub_processor["params"]
        StubProcessor: DataProcessor = stub_processor["processor"]

        valid_params: StubProcessorParams = StubProcessorParams(a="a")  # type: ignore
        stub = StubProcessor(params=valid_params)
        stub.params(**{"a": "b"})
        assert stub.params.dict()["a"] == "b"

    def test_params_not_subclasses(self) -> None:
        with pytest.raises(KeyError):

            class PydanticModel(BaseModel):
                a: List[int]

                def get_widgets(self) -> dict:
                    return {}

            model = PydanticModel(a=[1, 2, 3])

            class StubPydanticProcessor(DataProcessor):
                def __init__(self, params: PydanticModel):
                    super().__init__(params=params)  # type: ignore

            unregister_dataprocessor(StubPydanticProcessor)
            processor = StubPydanticProcessor(params=model)
