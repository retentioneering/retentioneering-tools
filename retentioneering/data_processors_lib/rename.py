from __future__ import annotations

from typing import List

import pandas as pd
from pydantic.dataclasses import dataclass

from retentioneering.backend.tracker import track
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.widget.widgets import RenameRulesWidget


@dataclass
class RenameRule:
    group_name: str
    child_events: List[str]


class RenameParams(ParamsModel):
    """
    [{
        "group_name": "some_group",
        "child_events": ["eventA", "eventB"]
    }]
    """

    rules: List[RenameRule]

    _widgets = {
        "rules": RenameRulesWidget(),
    }


class RenameProcessor(DataProcessor):
    params: RenameParams

    def __init__(self, params: RenameParams):
        super().__init__(params=params)

    @track(  # type: ignore
        tracking_info={"event_name": "apply"},
        scope="rename",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        event_col = schema.event_name

        rename_rules: dict[str, str] = dict()
        for rule in self.params.rules:
            to_ = rule.group_name
            for from_ in rule.child_events:
                rename_rules[from_] = to_

        df[event_col] = df[event_col].replace(rename_rules)
        return df
