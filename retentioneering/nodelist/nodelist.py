from __future__ import annotations

import pandas as pd


class Nodelist:
    nodelist_df: pd.DataFrame

    def __init__(self, event_col: str, time_col: str, weight_cols: list[str]) -> None:
        self.event_col = event_col
        self.time_col = time_col
        self.weight_cols = weight_cols

    def calculate_nodelist(self, data: pd.DataFrame) -> pd.DataFrame:
        res: pd.DataFrame = data.groupby([self.event_col])[self.time_col].count().reset_index()
        if self.weight_cols is not None:
            for weight_col in self.weight_cols:
                if weight_col == self.event_col:
                    continue
                by_col = data.groupby([self.event_col])[weight_col].nunique().reset_index()
                res = res.join(by_col[weight_col])

        res = res.sort_values(by=self.time_col, ascending=False)
        res = res.drop(columns=[self.time_col], axis=1)
        res["active"] = True
        res["alias"] = False
        res["parent"] = None
        res["changed_name"] = None

        self.nodelist_df = res
        return res
