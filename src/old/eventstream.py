from copyreg import constructor
import pandas as pd
import numpy as np
from pandas import DataFrame, Series
from typing import List, Sequence, Mapping, Tuple, Callable, Type, Optional, Union, MutableMapping, MutableSequence, cast
from typing_extensions import Literal, TypedDict

from retentioneering.new.processing_graph.rete_config import ReteConfig, EventstreamSchema

Mapper = Mapping[str, str]

class SourceDataSchema(TypedDict):
    event_name: str
    event_timestamp: str
    user_id: str
    custom_cols: List[str]




def get_priority(priority: Sequence[Union[str, None]], event_type: Union[str, None]):
    if event_type in priority:
        return priority.index(event_type)
    return 8


DELETE_COL_NAME = "rete_event_deleted"
GROUPED_EVENT_COL_NAME = "rete_event_grouped"




class Eventstream():
    schema: EventstreamSchema
    __config: ReteConfig
    __source_data_schema: SourceDataSchema
    __data: DataFrame
  
    def __init__(self,
        source_data_schema: SourceDataSchema,
        source: DataFrame,
        config: ReteConfig = ReteConfig(),
        prepare: bool = True
    ):
        self.schema = config.eventstream_schema
        self.__config = config
        self.__source_data_schema = source_data_schema
  
        self.__data = self._prepare_eventstream(source) if prepare else source

    def _prepare_eventstream(self, source: DataFrame, index=True):
        source_event_col = self.__get_col_from_source_data(
            source=source, colname=self.__source_data_schema["event_name"]
        )
        source_timestamp_col = pd.to_datetime(
            self.__get_col_from_source_data(
                source=source, colname=self.__source_data_schema["event_timestamp"]
            )
        )
        source_user_col = self.__get_col_from_source_data(
            source=source, colname=self.__source_data_schema["user_id"]
        )

        prepared_df = source.copy()
        prepared_df.rename(
            lambda col: f"raw_{col}", axis='columns', inplace=True)

        prepared_df[self.schema.event_type] = "raw"
        prepared_df[self.schema.event_name] = source_event_col
        prepared_df[self.schema.event_timestamp] = source_timestamp_col
        prepared_df[self.schema.user_id] = source_user_col

        # add custom
        for custom_col_name in self.schema.custom_cols:
            custom_col = self.__get_col_from_source_data(
                source=source, colname=custom_col_name, create=True
            )
            prepared_df[custom_col_name] = custom_col

        if (index):
            prepared_df = self.index(prepared_df)

        return prepared_df


    # TODO добавить третью сортировку по алфавиту
    def index(self, target_df: DataFrame = None):
        order = self.__config.events_order
        type_col_name = self.schema.event_type
        id_col_col_name = self.schema.event_id
        event_time_col_name = self.schema.event_timestamp
        order_temp_col_name = 'order'

        indexed = target_df if target_df is not None else self.__data

        indexed[order_temp_col_name] = indexed[type_col_name].apply(
            lambda e: get_priority(order, e)
        )
        indexed = indexed.sort_values([event_time_col_name, order_temp_col_name])
        indexed = indexed.drop([order_temp_col_name], axis=1)
        indexed[id_col_col_name] = range(1, len(indexed) + 1)
        
        indexed.reset_index(inplace=True, drop=True)

        if target_df is None:
            self.__data = indexed

        return indexed


    def copy(self):
        return Eventstream(
            source_data_schema=self.__source_data_schema.copy(),
            source=self.__data.copy(),
            prepare=False
        )

    # "видит" исходные колонки
    def map(self, query: str, mapper: Mapper):
        self._validate_mapper(mapper)
        view = self.to_dataframe()

        matched = view.query(query, engine="python")
        mapped = matched.assign(**mapper)

        for col in mapper:
            mapped_col = mapped[col]
            source_col = self._source_df[col]
            source_col.update(mapped_col)

        self._source_df[GROUPED_EVENT_COL_NAME] = self._source_df[self.config.event_col]

    def get_subset_by_users(self, users: Series, inplace=False):
        if not inplace:
            new_eventstream = self.copy()
            new_eventstream.get_subset_by_users(
                users, inplace=True)
            return new_eventstream

        user_col = self.config.user_col

        self._source_df = self._source_df[self._source_df[user_col].isin(
            users)]
        return self

    def filter(self, query: str, hard=False, index=False, inplace=False):
        if not inplace:
            new_eventstream = self.copy()
            new_eventstream.filter(
                query=query, hard=hard, index=index, inplace=True)
            return new_eventstream

        # фильтрация должна выполняться на уже группированных событиях, внешние функции не должны видеть
        view = self.to_dataframe()

        if hard:
            self._source_df = view.query(query, engine="python")
        else:
            matched = view.query(query, engine="python")
            self._soft_filter(matched=matched)

        if not index:
            self._source_df.reset_index(inplace=True, drop=True)

        if (index):
            self.index()

    def delete_events(self, query: str, hard=False, index=False, inplace=False):
        if not inplace:
            new_eventstream = self.copy()
            new_eventstream.delete_events(
                query=query, hard=hard, index=index, inplace=True)
            return new_eventstream

        id_col = self.config.id_col
        view = self.to_dataframe()
        deleted_events = view.query(query, engine="python").copy()

        if hard:
            self._source_df = self._source_df[self._source_df[id_col].isin(
                deleted_events[id_col]) == False]
        else:
            self._soft_delete(deleted_events)

        if not index:
            self._source_df.reset_index(inplace=True, drop=True)

        if (index):
            self.index()

    def add_raw_custom_col(self, colname: str, column: pd.Series):
        # проверка размерности
        if len(column) != len(self._source_df):
            raise ValueError(f"add raw column error")

        self.config.add_custom_col(colname)
        self._source_df[colname] = column

    def append_raw_events(self, df: DataFrame, save_raw_cols=True):
        prepared_events = self._prepare_eventstream(
            df=df, index=False, save_raw_cols=save_raw_cols)
        self._source_df = pd.concat([self._source_df, prepared_events])
        self.index()

    def to_dataframe(self):
        cols = [
            self.config.id_col,
            self.config.type_col,
            self.config.event_col,
            self.config.user_col,
            self.config.event_time_col
        ] + self.config.get_custom_cols()
        not_delted_events = self._get_not_deleted_source_events()

        df = pd.DataFrame(not_delted_events, columns=cols).copy()
        df[self.config.event_col] = not_delted_events[GROUPED_EVENT_COL_NAME]
        return df

    def _soft_filter(self, matched: DataFrame):
        id = self.config.id_col
        self._source_df[DELETE_COL_NAME] = ~self._source_df[id].isin(
            matched[id])

    def _soft_delete(self, events: DataFrame):
        id = self.config.id_col
        events[DELETE_COL_NAME] = True
        self._source_df[DELETE_COL_NAME].update(events[DELETE_COL_NAME])

    def _validate_mapper(self, mapper: Mapper):
        available_cols = [self.config.event_col] + \
            self.config.get_custom_cols()

        for col in mapper:
            if col not in available_cols:
                raise ValueError(
                    f'eventstream mapper validation: invalid column: {col}')

    def _get_not_deleted_source_events(self):
        return self._source_df[self._source_df[DELETE_COL_NAME] == False]

    def get_shift(self):
        user_col = self.config.user_col
        event_col = self.config.event_col
        id_col = self.config.id_col

        data = self.to_dataframe()
        data.sort_values([user_col, id_col], inplace=True)
        shift = data.groupby(user_col).shift(-1)

        data['next_' + event_col] = shift[event_col]
        data['next_' + str(id_col)] = shift[id_col]

        return data



    def __get_col_from_source_data(self, source: DataFrame, colname: str, create=False):
        if colname in source.columns:
            return source[colname]
        else:
            if create:
                return np.nan
            else:
                raise ValueError(
                    f'invald dataframe. column {colname} does not exists!')

    def _get_cols_from_raw_df(self, df: DataFrame, colnames: List[str], create=False):
        cols = []
        for colname in colnames:
            cols.append(self._get_col_from_raw_df(
                df=df, colname=colname, create=create))
        return cols
