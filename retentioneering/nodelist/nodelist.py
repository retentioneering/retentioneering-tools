from __future__ import annotations

import warnings

import pandas as pd

from retentioneering.tooling.transition_graph.interface import RenameRule


class Nodelist:
    nodelist_df: pd.DataFrame

    def __init__(self, event_col: str, time_col: str, weight_cols: list[str] | None) -> None:
        self.event_col = event_col
        self.time_col = time_col
        self.weight_cols = weight_cols

    def calculate_nodelist(self, data: pd.DataFrame, rename_rules: list[RenameRule] | None = None) -> pd.DataFrame:
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

        if rename_rules is not None and self.weight_cols is not None:
            for rename_rule in rename_rules:
                parent = rename_rule["group_name"]
                for child in rename_rule["child_events"]:
                    for weight_col in self.weight_cols:
                        with warnings.catch_warnings():
                            # disable warning for pydantic schema Callable type
                            warnings.simplefilter(action="ignore", category=FutureWarning)
                            res.append(  # type: ignore
                                {
                                    "event": child,
                                    self.event_col: 0,
                                    weight_col: 0,
                                    "active": False,
                                    "parent": parent,
                                    "changed_name": parent,
                                },
                                ignore_index=True,
                                sort=False,
                            )

        self.nodelist_df = res
        return res
