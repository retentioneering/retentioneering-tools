from __future__ import annotations

import pandas as pd

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.typing.transition_graph import NormType


class Edgelist:
    edgelist_df: pd.DataFrame
    eventstream: EventstreamType

    norm_type: NormType = None
    _weight_col: str
    event_col: str
    event_id_col: str
    time_col: str
    user_col: str

    def __init__(
        self,
        eventstream: EventstreamType,
    ) -> None:
        self.__extract_init_data_from_eventstream(eventstream)

    def __extract_init_data_from_eventstream(self, eventstream: EventstreamType) -> None:
        self.eventstream = eventstream
        self.event_col = eventstream.schema.event_name
        self.event_id_col = eventstream.schema.event_id
        self.time_col = eventstream.schema.event_timestamp
        self.user_col = eventstream.schema.user_id

    @property
    def weight_col(self) -> str:
        return self._weight_col

    @weight_col.setter
    def weight_col(self, value: str) -> None:
        if not value:
            raise ValueError("Weight col cannot be empty")
        self._weight_col = value

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
        self, weight_cols: list[str], norm_type: NormType | None = None, rename_cols: dict[str, str] | None = None
    ) -> pd.DataFrame:
        if norm_type not in (None, "full", "node"):
            raise ValueError(f"unknown normalization type: {norm_type}")

        self.norm_type = norm_type
        edge_from, edge_to = self.event_col, self.next_event_col
        df = self.eventstream.to_dataframe()
        calculated_edgelist: pd.DataFrame = pd.DataFrame()
        for weight_col in weight_cols:
            self.weight_col = weight_col
            edgelist = self._calculate_edgelist_for_selected_weight(df, edge_from, edge_to)
            if calculated_edgelist.empty:
                calculated_edgelist = edgelist
            else:
                calculated_edgelist = self._merge_edgelist(calculated_edgelist, edge_from, edge_to, edgelist)

        if rename_cols is not None:
            calculated_edgelist = calculated_edgelist.rename(columns=rename_cols)

        self.edgelist_df = calculated_edgelist
        return calculated_edgelist

    def _merge_edgelist(
        self, calculated_edgelist: pd.DataFrame, edge_from: str, edge_to: str, edgelist: pd.DataFrame
    ) -> pd.DataFrame:
        calculated_edgelist = pd.merge(
            calculated_edgelist,
            edgelist,
            how="outer",
            left_on=[edge_from, edge_to],
            right_on=[edge_from, edge_to],
            suffixes=["", "_del"],
        )
        calculated_edgelist = calculated_edgelist[
            [c for c in calculated_edgelist.columns if not c.endswith("_del")]  # type: ignore
        ]
        return calculated_edgelist

    def _calculate_edgelist_for_selected_weight(self, df: pd.DataFrame, edge_from: str, edge_to: str) -> pd.DataFrame:
        possible_transitions = (
            df.assign(**{edge_to: lambda _df: _df.groupby(self.user_col)[edge_from].shift(-1)})
            .dropna(subset=[edge_to])
            .groupby([edge_from, edge_to])
            .size()
            .index
        )
        bigrams = df.assign(**{edge_to: lambda _df: _df.groupby(self.group_col)[edge_from].shift(-1)}).dropna(
            subset=[edge_to]
        )
        abs_values = bigrams.groupby([edge_from, edge_to])[self.weight_col].nunique()
        if self.weight_col != self.event_id_col:
            abs_values = abs_values.reindex(possible_transitions)
        edgelist = abs_values
        # denumerator_full = total number of transitions/users/sessions
        if self.norm_type == "full":
            denumerator_full = bigrams[self.weight_col].nunique()
            edgelist = abs_values / denumerator_full
        # denumerator_node = total number of transitions/users/sessions that started with edge_from event
        if self.norm_type == "node":
            denumerator_node = bigrams.groupby([edge_from])[self.weight_col].nunique()
            edgelist = abs_values / denumerator_node
        if self.weight_col not in [self.event_id_col, self.user_col]:
            edgelist = edgelist.fillna(0)

            if self.norm_type is None:
                edgelist = edgelist.astype(int)
        edgelist = edgelist.reset_index(allow_duplicates=True)
        return edgelist
