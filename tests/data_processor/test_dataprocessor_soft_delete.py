import pandas as pd
import pytest

from retentioneering.data_processor import DataProcessor
from retentioneering.data_processor.registry import unregister_dataprocessor
from retentioneering.eventstream import Eventstream, RawDataSchema
from retentioneering.graph.p_graph import EventsNode, PGraph
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


class TestGraphDelete:
    def test_soft_delete_in_graph(self, delete_processor) -> None:
        DeleteParamsModel: ParamsModel = delete_processor["params"]
        DeleteProcessor: DataProcessor = delete_processor["processor"]

        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"event_name": "cart_btn_click", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"event_name": "pageview", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "trash_event", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"event_name": "exit_btn_click", "event_timestamp": "2021-10-26 12:04", "user_id": "2"},
                {"event_name": "plus_icon_click", "event_timestamp": "2021-10-26 12:05", "user_id": "1"},
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
        )
        graph = PGraph(source)
        delete_node = EventsNode(DeleteProcessor(params=DeleteParamsModel(name="pageview")))
        graph.add_node(node=delete_node, parents=[graph.root])
        result = graph.combine(delete_node)
        result_df = result.to_dataframe()
        assert 4 == len(result_df)
