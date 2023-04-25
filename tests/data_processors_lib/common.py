import pandas as pd

from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import EventstreamSchema, RawDataSchema
from retentioneering.params_model.params_model import ParamsModel
from retentioneering.preprocessing_graph import EventsNode, PreprocessingGraph

_default_raw_data_schema = RawDataSchema(
    user_id="user_id",
    event_name="event",
    event_type="event_type",
    event_timestamp="timestamp",
)


def apply_processor(
    data_processor: DataProcessor, source_df: pd.DataFrame, raw_data_schema: RawDataSchema = _default_raw_data_schema
) -> (pd.DataFrame, pd.DataFrame):
    stream = Eventstream(
        raw_data_schema=raw_data_schema,
        raw_data=source_df.copy(),
        schema=EventstreamSchema(),
    )
    original_df = stream.to_dataframe(show_deleted=True).reset_index(drop=True)
    result = data_processor.apply(stream)
    result_df = result.to_dataframe(show_deleted=True).reset_index(drop=True)
    return original_df, result_df


def apply_processor_with_graph(
    data_processor: DataProcessor, source_df: pd.DataFrame, raw_data_schema: RawDataSchema = _default_raw_data_schema
) -> (pd.DataFrame, pd.DataFrame):
    stream = Eventstream(
        raw_data_schema=raw_data_schema,
        raw_data=source_df.copy(),
        schema=EventstreamSchema(),
    )
    original_df = stream.to_dataframe().reset_index(drop=True)
    graph = PreprocessingGraph(source_stream=stream)
    node = EventsNode(data_processor)
    graph.add_node(node=node, parents=[graph.root])
    result = graph.combine(node=node)
    result_df = result.to_dataframe().reset_index(drop=True)
    return original_df, result_df


class ApplyTestBase:
    _Processor: DataProcessor

    def _apply(self, params: ParamsModel, source_df: pd.DataFrame = None, return_with_original: bool = False):
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
    _Processor: DataProcessor

    def _apply(self, params: ParamsModel, source_df: pd.DataFrame = None, return_with_original: bool = False):
        original, actual = apply_processor_with_graph(
            self._Processor(params),
            self._source_df if source_df is None else source_df,
            raw_data_schema=self._raw_data_schema,
        )
        if return_with_original:
            return original, actual
        else:
            return actual
