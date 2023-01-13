import pandas as pd

from retentioneering.data_processor.data_processor import DataProcessor
from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from retentioneering.graph.p_graph import EventsNode, PGraph
from retentioneering.params_model import ParamsModel


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


class TestGraphAddColumn:
    def test_add_column_in_graph_with_helper(self) -> None:
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
        add_col_node = EventsNode(
            HelperAddColProcessor(params=AddColParamsModel(event_name="pageview", column_name="bucket"))
        )
        graph.add_node(node=add_col_node, parents=[graph.root])
        result = graph.combine(add_col_node)
        result_df = result.to_dataframe()
        columns = list(result_df.columns)
        required_columns = [
            "event_id",
            "event_type",
            "event_index",
            "event",
            "timestamp",
            "user_id",
            "bucket",
        ]
        assert required_columns == columns
        assert 6 == len(result_df)

    def test_add_column_in_graph_without_helper(self) -> None:
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
        add_col_node = EventsNode(
            NoHelperAddColProcessor(params=AddColParamsModel(event_name="pageview", column_name="bucket"))
        )
        graph.add_node(node=add_col_node, parents=[graph.root])
        result = graph.combine(add_col_node)
        result_df = result.to_dataframe()
        columns = list(result_df.columns)
        required_columns = [
            "event_id",
            "event_type",
            "event_index",
            "event",
            "timestamp",
            "user_id",
            "bucket",
        ]
        assert 6 == len(result_df)
        assert required_columns == columns
