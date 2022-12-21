from __future__ import annotations

import pandas as pd


class Nodelist:

    nodelist_df: pd.DataFrame

    def __init__(self, event_col: str, time_col: str, nodelist_default_col: str, custom_cols: list[str]) -> None:
        self.event_col = event_col
        self.time_col = time_col
        self.nodelist_default_col = nodelist_default_col
        self.custom_cols = custom_cols

    def calculate_nodelist(self, data: pd.DataFrame) -> None:

        res: pd.DataFrame = data.groupby([self.event_col])[self.time_col].count().reset_index()
        if self.custom_cols is not None:
            for weight_col in self.custom_cols:
                by_col = data.groupby([self.event_col])[weight_col].nunique().reset_index()
                res = res.join(by_col[weight_col])

        res = res.sort_values(by=self.time_col, ascending=False)
        res.rename(columns={self.time_col: self.nodelist_default_col}, inplace=True)

        res["active"] = True
        res["alias"] = False
        res["parent"] = None
        res["changed_name"] = None

        self.nodelist_df = res
