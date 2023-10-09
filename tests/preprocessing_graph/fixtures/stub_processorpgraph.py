from typing import Literal, Union

import pandas as pd
import pytest

from retentioneering.data_processor import DataProcessor
from retentioneering.data_processor.registry import unregister_dataprocessor
from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from retentioneering.params_model import ParamsModel
from retentioneering.params_model.registry import unregister_params_model


@pytest.fixture
def stub_processorpgraph():
    class StubPProcessorParams(ParamsModel):
        a: Union[Literal["a"], Literal["b"]]

    class StubProcessorPreprocessingGraph(DataProcessor):
        params: StubPProcessorParams

        def __init__(self, params: StubPProcessorParams):
            super().__init__(params=params)

        def apply_diff(self, eventstream: Eventstream) -> Eventstream:
            return eventstream.copy()

    yield {"params": StubPProcessorParams, "processor": StubProcessorPreprocessingGraph}

    unregister_dataprocessor(StubProcessorPreprocessingGraph)
    unregister_params_model(StubPProcessorParams)
