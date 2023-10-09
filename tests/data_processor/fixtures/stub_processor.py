from typing import Literal, Union

import pandas as pd
import pytest

from retentioneering.data_processor import DataProcessor
from retentioneering.data_processor.registry import unregister_dataprocessor
from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from retentioneering.params_model import ParamsModel
from retentioneering.params_model.registry import unregister_params_model


@pytest.fixture
def stub_processor():
    class StubProcessorParams(ParamsModel):
        a: Union[Literal["a"], Literal["b"]]

    class StubProcessor(DataProcessor):
        params: StubProcessorParams

        def __init__(self, params: StubProcessorParams):
            super().__init__(params=params)

        def apply_diff(self, eventstream: Eventstream) -> Eventstream:
            return eventstream.copy()

    yield {"params": StubProcessorParams, "processor": StubProcessor}

    unregister_dataprocessor(StubProcessor)
    unregister_params_model(StubProcessorParams)
