import pytest

from retentioneering.data_processor import DataProcessor
from retentioneering.data_processor.registry import unregister_dataprocessor
from retentioneering.eventstream import Eventstream
from retentioneering.params_model import ParamsModel
from retentioneering.params_model.registry import unregister_params_model


@pytest.fixture
def delete_processor():
    class DeleteParamsModel(ParamsModel):
        name: str

    class DeleteProcessor(DataProcessor):
        params: DeleteParamsModel

        def __init__(self, params: DeleteParamsModel) -> None:
            super().__init__(params=params)

        def apply(self, eventstream: Eventstream) -> Eventstream:
            df = eventstream.to_dataframe(copy=True)
            event_name = eventstream.schema.event_name
            data_for_delete = df.loc[df[event_name] == self.params.name]
            df["ref"] = df[eventstream.schema.event_id]

            eventstream = Eventstream(
                raw_data_schema=eventstream.schema.to_raw_data_schema(),
                raw_data=df,
                relations=[{"raw_col": "ref", "eventstream": eventstream}],
            )

            eventstream._soft_delete(data_for_delete)

            return eventstream

    yield {"params": DeleteParamsModel, "processor": DeleteProcessor}

    unregister_dataprocessor(DeleteProcessor)
    unregister_params_model(DeleteParamsModel)
