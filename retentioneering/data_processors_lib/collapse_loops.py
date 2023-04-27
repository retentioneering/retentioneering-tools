from __future__ import annotations

import warnings
from typing import Any, Callable, Literal, Optional

import numpy as np
import pandas as pd

from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamType
from retentioneering.params_model import ParamsModel


def _custom_formatwarning(msg: str, *args: Any, **kwargs: Any) -> str:
    # ignore everything except the message
    return str(msg) + "\n"


def _numeric_values_processing(x: pd.Series) -> int | float:
    return x.mean()


def _string_values_processing(x: pd.Series) -> str | None:
    if x.nunique() == 1:
        return x.max()
    else:
        return None


class CollapseLoopsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.CollapseLoops` class.
    """

    suffix: Optional[Literal["loop", "count"]]
    time_agg: Literal["max", "min", "mean"] = "min"


class CollapseLoops(DataProcessor):
    """
    Find ``loops`` and create new synthetic events in the paths of all users having such sequences.

    A ``loop`` - is a sequence of repetitive events.
    For example *"event1 -> event1"*

    Parameters
    ----------
    suffix : {"loop", "count"}, optional

        - If ``None``, event_name will be event_name without any changes.\n
        For example *"event1 - event1 - event1"* --> event1.

        - If ``loop``, event_name will be event_name_loop.\n
        For example *"event1 - event1 - event1"* --> event1_loop.

        - If ``count``, event_name will be event_name_loop_{number of events}.\n
        For example *"event1 - event1 - event1"* --> event1_loop_3.

    time_agg : {"max", "min", "mean"}, default "min"
        Aggregation method to calculate timestamp values for new groups.


    Returns
    -------
    Eventstream
        Eventstream with:

        - raw events: the returned events will be marked ``_deleted=True`` and soft-deleted from input eventstream.
        - new synthetic events: the returned events will be added to the input eventstream with columns below.

        +------------------------+----------------+--------------------------------------------+
        | **event_name**         | **event_type** | **timestamp**                              |
        +------------------------+----------------+--------------------------------------------+
        | event_name_loop        | group_alias    | min/max/mean(group of repetitive events))  |
        +------------------------+----------------+--------------------------------------------+
        | event_name_loop_{count}| group_alias    | (min/max/mean(group of repetitive events)) |
        +------------------------+----------------+--------------------------------------------+
        | event_name             | group_alias    | (min/max/mean(group of repetitive events)) |
        +------------------------+----------------+--------------------------------------------+


    See Also
    --------
    .StepMatrix

    Notes
    -----
    If an eventstream contains custom_cols they will be aggregating in the following way:

    - for numeric columns the mean value for each group will be calculated. ``None`` values are ignored.
    - for string columns, ``None`` - if aggregated values are not equal in the group.

    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.
    """

    params: CollapseLoopsParams
    NUMERIC_DTYPES = "bifc"

    def __init__(self, params: CollapseLoopsParams):
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from retentioneering.eventstream.eventstream import Eventstream

        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name
        custom_cols: list = eventstream.schema.custom_cols

        suffix = self.params.suffix
        time_agg = self.params.time_agg
        agg: dict[str, Callable] = {}
        full_agg: dict[str, Any] = {}

        df = eventstream.to_dataframe(copy=True)
        if len(custom_cols) > 0:
            df_custom_cols = df[custom_cols]
            df_dtypes = np.array(df_custom_cols.dtypes)
            cols_agg: list = []

            for x in df_dtypes:
                if x.kind in self.NUMERIC_DTYPES:
                    cols_agg.append(_numeric_values_processing)
                else:
                    cols_agg.append(_string_values_processing)

            default_agg: dict = dict(zip(custom_cols, cols_agg))
            full_agg = {**default_agg, **agg}  # type: ignore
        else:
            full_agg = {}

        df["ref"] = df[eventstream.schema.event_id]

        df["grp"] = df[event_col] != df.groupby(user_col)[event_col].shift(1)
        df["cumgroup"] = df.groupby(user_col)["grp"].cumsum()
        df["count"] = df.groupby([user_col, "cumgroup"]).cumcount() + 1
        df["collapsed"] = df.groupby([user_col, "cumgroup", event_col])["count"].transform(max)
        df["collapsed"] = df["collapsed"].apply(lambda x: False if x == 1 else True)

        loops = (
            df[df["collapsed"] == 1]
            .groupby([user_col, "cumgroup", event_col])
            .agg({time_col: time_agg, "count": "max", **full_agg})
            .reset_index()
        )

        if len(custom_cols) > 0:
            if loops[custom_cols].isna().sum().sum() > 0:
                warnings.formatwarning = _custom_formatwarning  # type: ignore
                warnings.warn(
                    f"There are NaN values in aggregated custom_cols!\n "
                    f"Please see the total amount of NaN values in each column:\n\n"
                    f"{loops[custom_cols].isna().sum()} "
                    f"\n\n"
                    f"And an example of events with NaN values in aggregated custom_cols:\n\n"
                    f"{loops[loops.isnull().any(axis=1)].head(3)} ",
                    stacklevel=1,
                )

        if suffix == "loop":
            loops[event_col] = loops[event_col].map(str) + "_loop"
        elif suffix == "count":
            loops[event_col] = loops[event_col].map(str) + "_loop_" + loops["count"].map(str)
        loops[type_col] = "group_alias"
        loops["ref"] = None

        df_to_del = df[df["collapsed"] == 1]

        df_loops = pd.concat([loops, df_to_del])
        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=df_loops,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        if not df_to_del.empty:
            eventstream._soft_delete(df_to_del)

        return eventstream
