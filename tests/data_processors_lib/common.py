from typing import Optional

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


def apply_processor_with_graph(
    data_processor: DataProcessor, source_df: pd.DataFrame, raw_data_schema: RawDataSchema = _default_raw_data_schema
) -> (pd.DataFrame, pd.DataFrame):
    stream = Eventstream(
        raw_data_schema=raw_data_schema,
        raw_data=source_df.copy(),
        schema=EventstreamSchema(),
        add_start_end_events=False,
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
    _source_df: Optional[pd.DataFrame]
    _raw_data_schema: Optional[RawDataSchema]

    def _apply_dataprocessor(
        self,
        params: ParamsModel,
        source_df: Optional[pd.DataFrame] = None,
        raw_data_schema: Optional[RawDataSchema] = None,
    ) -> pd.DataFrame:
        df = source_df if source_df is not None else getattr(self, "_source_df", None)
        raw_schema = (
            raw_data_schema if raw_data_schema is not None else getattr(self, "_raw_data_schema", RawDataSchema())
        )

        if df is None:
            raise ValueError("You should explicitly pass source_df or define a property in the inherited class")

        stream = Eventstream(
            raw_data=df,
            raw_data_schema=raw_schema,
            add_start_end_events=False,
        )

        dataprocessor = self._Processor(
            params=params,
        )
        result = dataprocessor.apply(
            df=stream.to_dataframe(copy=True),
            schema=stream.schema,
        )

        raw_schema = stream.schema.to_raw_data_schema(event_index=True)
        raw_schema.event_type = "event_type"

        result_stream = Eventstream(
            raw_data=result,
            raw_data_schema=raw_schema,
            add_start_end_events=False,
        )
        return result_stream.to_dataframe()


class GraphTestBase:
    _Processor: DataProcessor

    def _apply(
        self,
        params: ParamsModel,
        source_df: pd.DataFrame = None,
        return_with_original: bool = False,
        raw_data_schema: RawDataSchema = None,
    ):
        original, actual = apply_processor_with_graph(
            self._Processor(params),
            self._source_df if source_df is None else source_df,
            self._raw_data_schema if raw_data_schema is None else raw_data_schema,
        )
        if return_with_original:
            return original, actual
        else:
            return actual
