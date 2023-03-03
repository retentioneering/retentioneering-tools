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

        def apply(self, eventstream: Eventstream) -> Eventstream:
            df = eventstream.to_dataframe(copy=True)
            new_data = df[eventstream.schema.event_name].isin(["cart_btn_click", "plus_icon_click"])
            df[self.params.column_name] = new_data
            df["ref"] = df[eventstream.schema.event_id]

            eventstream.add_custom_col(name=self.params.column_name, data=new_data)
            eventstream = Eventstream(
                raw_data_schema=eventstream.schema.to_raw_data_schema(),
                raw_data=df,
                relations=[{"raw_col": "ref", "eventstream": eventstream}],
            )

            return eventstream

    class NoHelperAddColProcessor(DataProcessor):
        params: AddColParamsModel

        def __init__(self, params: AddColParamsModel) -> None:
            super().__init__(params=params)

        def apply(self, eventstream: Eventstream) -> Eventstream:
            df = eventstream.to_dataframe(copy=True)
            new_data = df[eventstream.schema.event_name].isin(["cart_btn_click", "plus_icon_click"])
            df[self.params.column_name] = new_data
            df["ref"] = df[eventstream.schema.event_id]

            raw_data_schema = eventstream.schema.to_raw_data_schema()
            raw_data_schema.custom_cols.append(
                {"custom_col": self.params.column_name, "raw_data_col": self.params.column_name}
            )
            eventstream = Eventstream(
                schema=EventstreamSchema(custom_cols=[self.params.column_name]),
                raw_data_schema=raw_data_schema,
                raw_data=df,
                relations=[{"raw_col": "ref", "eventstream": eventstream}],
            )

            return eventstream

    yield {
        "AddColParamsModel": AddColParamsModel,
        "HelperAddColProcessor": HelperAddColProcessor,
        "NoHelperAddColProcessor": NoHelperAddColProcessor,
    }

    unregister_dataprocessor(HelperAddColProcessor)
    unregister_dataprocessor(NoHelperAddColProcessor)
    unregister_params_model(AddColParamsModel)
