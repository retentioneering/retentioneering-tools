import pandas as pd

from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream import Eventstream, RawDataSchema
from retentioneering.graph.p_graph import EventsNode, PGraph
from retentioneering.params_model import ParamsModel


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


class TestGraphDelete:
    def test_soft_delete_in_graph(self) -> None:
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
