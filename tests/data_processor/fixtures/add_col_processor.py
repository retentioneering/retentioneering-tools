import pandas as pd
import pytest

from retentioneering.data_processor import DataProcessor
from retentioneering.data_processor.registry import unregister_dataprocessor
from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from retentioneering.params_model import ParamsModel
from retentioneering.params_model.registry import unregister_params_model


@pytest.fixture
def add_col_processor():
    class AddColParamsModel(ParamsModel):
        event_name: str
        column_name: str

    class HelperAddColProcessor(DataProcessor):
        params: AddColParamsModel

        def __init__(self, params: AddColParamsModel) -> None:
            super().__init__(params=params)

        def apply(self, df: pd.DataFrame, schema: EventstreamSchema) -> pd.DataFrame:
            new_data = df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"])
            df[self.params.column_name] = new_data
            return df

    yield {
        "AddColParamsModel": AddColParamsModel,
        "HelperAddColProcessor": HelperAddColProcessor,
    }

    unregister_dataprocessor(HelperAddColProcessor)
    unregister_params_model(AddColParamsModel)
