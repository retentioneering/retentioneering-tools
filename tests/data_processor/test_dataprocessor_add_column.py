import pandas as pd

from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from retentioneering.params_model import ParamsModel
from retentioneering.preprocessing_graph import EventsNode, PreprocessingGraph
from tests.data_processor.fixtures.add_col_processor import add_col_processor


class TestGraphAddColumn:
    def test_add_column_in_graph_with_helper(self, add_col_processor) -> None:
        AddColParamsModel: ParamsModel = add_col_processor["AddColParamsModel"]
        HelperAddColProcessor: DataProcessor = add_col_processor["HelperAddColProcessor"]

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
        graph = PreprocessingGraph(source)
        add_col_node = EventsNode(
            HelperAddColProcessor(params=AddColParamsModel(event_name="pageview", column_name="bucket"))  # type: ignore
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

    def test_add_column_in_graph_without_helper(self, add_col_processor) -> None:
        AddColParamsModel: ParamsModel = add_col_processor["AddColParamsModel"]
        NoHelperAddColProcessor: DataProcessor = add_col_processor["NoHelperAddColProcessor"]

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
        graph = PreprocessingGraph(source)
        add_col_node = EventsNode(
            NoHelperAddColProcessor(params=AddColParamsModel(event_name="pageview", column_name="bucket"))  # type: ignore
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
