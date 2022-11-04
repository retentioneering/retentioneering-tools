import pandas as pd

from src.params_model.params_model import ParamsModel
from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import (
    RawDataSchema,
    EventstreamSchema,
)
from src.graph.p_graph import (
    PGraph,
    EventsNode,
)


_default_raw_data_schema = RawDataSchema(
    user_id="user_id",
    event_name="event_name",
    event_type="event_type",
    event_timestamp="event_timestamp",
)


def apply_processor(data_processor: DataProcessor, source_df: pd.DataFrame, raw_data_schema: RawDataSchema = _default_raw_data_schema) -> (pd.DataFrame, pd.DataFrame):
    stream = Eventstream(
        raw_data_schema=raw_data_schema,
        raw_data=source_df.copy(),
        schema=EventstreamSchema(),
    )
    original_df = stream.to_dataframe(show_deleted=True).reset_index(drop=True)
    result = data_processor.apply(stream)
    result_df = result.to_dataframe(show_deleted=True).reset_index(drop=True)
    return original_df, result_df


def apply_processor_with_graph(data_processor: DataProcessor, source_df: pd.DataFrame, raw_data_schema: RawDataSchema = _default_raw_data_schema) -> (pd.DataFrame, pd.DataFrame):
    stream = Eventstream(
        raw_data_schema=raw_data_schema,
        raw_data=source_df.copy(),
        schema=EventstreamSchema(),
    )
    original_df = stream.to_dataframe().reset_index(drop=True)
    graph = PGraph(source_stream=stream)
    node = EventsNode(data_processor)
    graph.add_node(node=node, parents=[graph.root])
    result = graph.combine(node=node)
    result_df = result.to_dataframe().reset_index(drop=True)
    return original_df, result_df


class ApplyTestBase:
    def _apply(self, params: ParamsModel, source_df: pd.DataFrame=None, return_with_original=False):
        original, actual = apply_processor(
            self._Processor(params),
            self._source_df if source_df is None else source_df,
            raw_data_schema=self._raw_data_schema,
        )
        if return_with_original:
            return original, actual
        else:
            return actual


class GraphTestBase:
    def _apply(self, params: ParamsModel, source_df: pd.DataFrame=None, return_with_original=False):
        original, actual = apply_processor_with_graph(
            self._Processor(params),
            self._source_df if source_df is None else source_df,
            raw_data_schema=self._raw_data_schema,
        )
        if return_with_original:
            return original, actual
        else:
            return actual
