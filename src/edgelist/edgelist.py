from __future__ import annotations

from typing import Callable, MutableMapping, MutableSequence

import pandas as pd

from src.transition_graph.typing import NormType

NormFunc = Callable[[pd.DataFrame, pd.DataFrame, pd.DataFrame], pd.Series]


class Edgelist:
    edgelist_norm_functions: MutableMapping[str, NormFunc] | None
    edgelist_df: pd.DataFrame

    def __init__(
        self,
        event_col: str,
        time_col: str,
        default_weight_col: str,
        index_col: str,
        nodelist: pd.DataFrame,
        edgelist_norm_functions: MutableMapping[str, NormFunc] | None = None,
    ) -> None:
        self.event_col = event_col
        self.time_col = time_col
        self.default_weight_col = default_weight_col
        self.nodelist = nodelist
        self.index_col = index_col
        self.edgelist_norm_functions = edgelist_norm_functions

    def calculate_edgelist(
        self, data: pd.DataFrame, norm_type: NormType = None, custom_cols: MutableSequence[str] | None = None
    ) -> pd.DataFrame:

        if norm_type not in [None, "full", "node"]:
            raise ValueError(f"unknown normalization type: {norm_type}")

        cols = [self.event_col, "next_" + str(self.event_col)]
        data = self._get_shift(data=data, event_col=self.event_col, time_col=self.time_col, index_col=self.index_col)

        edgelist = data.groupby(cols)[self.time_col].count().reset_index()
        edgelist.rename(columns={self.time_col: self.default_weight_col}, inplace=True)

        if custom_cols is not None:
            for weight_col in custom_cols:
                agg_i = data.groupby(cols)[weight_col].nunique().reset_index()
                edgelist = edgelist.join(agg_i[weight_col])

        # apply default norm func
        if norm_type == "full":
            edgelist[self.default_weight_col] /= edgelist[self.default_weight_col].sum()
            if custom_cols is not None:
                for weight_col in custom_cols:
                    edgelist[weight_col] /= data[weight_col].nunique()

        elif norm_type == "node":
            event_transitions_counter = data.groupby(self.event_col)[cols[1]].count().to_dict()

            edgelist[self.default_weight_col] /= edgelist[cols[0]].map(event_transitions_counter)

            if custom_cols is not None:
                for weight_col in custom_cols:
                    user_counter = data.groupby(cols[0])[weight_col].nunique().to_dict()
                    edgelist[weight_col] /= edgelist[cols[0]].map(user_counter)

        # @TODO: подумать над этим (legacy from private by Alexey). Vladimir Makhanov
        # apply custom norm func for event col
        if self.edgelist_norm_functions is not None:
            if self.default_weight_col in self.edgelist_norm_functions:
                edgelist[self.default_weight_col] = self.edgelist_norm_functions[self.default_weight_col](
                    data, self.nodelist, edgelist
                )

            if custom_cols is not None:
                for weight_col in custom_cols:
                    if weight_col in self.edgelist_norm_functions:
                        edgelist[weight_col] = self.edgelist_norm_functions[weight_col](data, self.nodelist, edgelist)

        self.edgelist_df = edgelist
        return edgelist

    def _get_shift(self, data: pd.DataFrame, index_col: str, event_col: str, time_col: str) -> pd.DataFrame:
        data.sort_values([index_col, time_col], inplace=True)
        shift = data.groupby(index_col).shift(-1)

        data["next_" + event_col] = shift[event_col]
        data["next_" + str(time_col)] = shift[time_col]

        return data
