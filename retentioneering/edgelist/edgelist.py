from __future__ import annotations

from typing import Callable

import pandas as pd

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.typing.transition_graph import NormType

NormFunc = Callable[[pd.DataFrame, pd.DataFrame, pd.DataFrame], pd.Series]


class Edgelist:
    edgelist_df: pd.DataFrame
    eventstream: EventstreamType | None

    norm_type: NormType = None
    _weight_col: str
    _event_col: str
    _time_col: str
    _user_col: str

    def __init__(
        self,
        eventstream: EventstreamType | None = None,
    ) -> None:
        self.__extract_init_data_from_eventstream(eventstream)

    def __extract_init_data_from_eventstream(self, eventstream):
        if eventstream is not None:
            self.eventstream = eventstream
            self.event_col = eventstream.schema.event_name
            self.event_id_col = eventstream.schema.event_id
            self.time_col = eventstream.schema.event_timestamp
            self.user_col = eventstream.schema.user_id
        else:
            self.eventstream = None
            self.event_id_col = 'event_id'

    @property
    def weight_col(self) -> str:
        return self._weight_col

    @weight_col.setter
    def weight_col(self, value: str) -> None:
        if not value:
            raise ValueError("Weight col cannot be empty")
        self._weight_col = value

    @property
    def event_col(self) -> str:
        return self._event_col

    @event_col.setter
    def event_col(self, value: str) -> None:
        if not value:
            raise ValueError("Event col cannot be empty")
        self._event_col = value

    @property
    def time_col(self) -> str:
        return self._time_col

    @time_col.setter
    def time_col(self, value: str) -> None:
        if not value:
            raise ValueError("Time col cannot be empty")
        self._time_col = value

    @property
    def user_col(self) -> str:
        return self._user_col

    @user_col.setter
    def user_col(self, value: str) -> None:
        # if not value:
        #     raise ValueError("User col cannot be empty")
        self._user_col = value

    @property
    def group_col(self) -> str:
        if self.weight_col in (self.event_id_col, self.user_col):
            return self.user_col
        else:
            return self.weight_col

    @property
    def next_event_col(self) -> str:
        return f"next_{self.event_col}"

    def calculate_edgelist(
        self,
        weight_col: str,
        data: EventstreamType | pd.DataFrame | None = None,
        norm_type: NormType | None = None,
        event_col: str | None = None,
        time_col: str | None = None,
        user_col: str | None = None,
    ) -> pd.DataFrame:

        if norm_type not in (None, "full", "node"):
            raise ValueError(f"unknown normalization type: {norm_type}")

        if self.eventstream is not None and data is None:
            data = self.eventstream.to_dataframe()

        elif isinstance(data, EventstreamType):
            self.__extract_init_data_from_eventstream(eventstream=data)
            data: pd.DataFrame = self.eventstream.to_dataframe()

        elif isinstance(data, pd.DataFrame):
            self.event_col = event_col
            self.time_col = time_col
            self.user_col = user_col

        self.norm_type = norm_type
        self.weight_col = weight_col
        edge_from, edge_to = self.event_col, self.next_event_col

        possible_transitions = data.assign(**{edge_to: lambda _df: _df.groupby(self.user_col)[edge_from].shift(-1)}) \
            .dropna(subset=[edge_to]) \
            .groupby([edge_from, edge_to]) \
            .size() \
            .index

        bigrams = data.assign(**{edge_to: lambda _df: _df.groupby(self.group_col)[edge_from].shift(-1)})\
            .dropna(subset=[edge_to])

        abs_values = bigrams.groupby([edge_from, edge_to])[self.weight_col].nunique()

        if self.weight_col != self.event_id_col:
            abs_values = abs_values.reindex(possible_transitions)
        edgelist = abs_values

        # denumerator_full = total number of transitions/users/sessions
        if self.norm_type == 'full':
            denumerator_full = bigrams[self.weight_col].nunique()
            edgelist = abs_values / denumerator_full

        # denumerator_node = total number of transitions/users/sessions that started with edge_from event
        if self.norm_type == 'node':
            denumerator_node = bigrams.groupby([edge_from])[self.weight_col].nunique()
            edgelist = abs_values / denumerator_node

        if weight_col not in [self.event_id_col, self.user_col]:
            edgelist = edgelist.fillna(0)

            if self.norm_type is None:
                edgelist = edgelist.astype(int)

        edgelist = edgelist.reset_index(allow_duplicates=True)
        self.edgelist_df = edgelist
        return edgelist
