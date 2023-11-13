from __future__ import annotations

import uuid
import warnings
from typing import Any, Callable, Literal, Optional

import pandas as pd

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
from retentioneering.widget.widgets import EnumWidget


def _numeric_values_processing(x: pd.Series) -> int | float:
    return x.mean()


def _string_values_processing(x: pd.Series) -> str | None:
    # check if all the values in the collapsing group are equal
    # NaN values are ignored
    if x.nunique() == 1:
        return x.dropna().max()
    else:
        return None


class CollapseLoopsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.CollapseLoops` class.
    """

    suffix: Optional[Literal["loop", "count"]]
    time_agg: Literal["max", "min", "mean"] = "min"
    _widgets = {
        "time_agg": EnumWidget(optional=False, params=["max", "min", "mean"], default="min"),
    }


@docstrings.get_sections(base="CollapseLoops")  # type: ignore
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
    .StepMatrix : This class provides methods for step matrix calculation and visualization.
    .RawDataSchema : Define schema for ``raw_data`` columns names.

    Notes
    -----
    If an eventstream contains custom columns they will be aggregated in the following way:

    - for numeric columns the mean value will be calculated for each collapsed group. ``None`` values are ignored.
      Supported numeric types are: ``bool``, ``int``, ``float``.
    - for string columns, if all the values to be aggregated in the
      collapsing group are equal, this single value will be returned, otherwise - ``None``.
      ``None`` values in the input data will be ignored.

    See :doc:`Data processors user guide</user_guides/dataprocessors>` and :ref:`Eventstream custom columns'
    explanation<eventstream_raw_data_schema>` for the details.
    """

    params: CollapseLoopsParams
    NUMERIC_DTYPES = ["integer", "floating", "boolean", "mixed-integer-float"]

    @time_performance(
        scope="collapse_loops",
        event_name="init",
    )
    def __init__(self, params: CollapseLoopsParams):
        super().__init__(params=params)

    @time_performance(
        scope="collapse_loops",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        source = df.copy()

        user_col = schema.user_id
        time_col = schema.event_timestamp
        type_col = schema.event_type
        event_col = schema.event_name
        event_index_col = schema.event_index
        custom_cols: list = schema.custom_cols
        event_id = schema.event_id

        suffix = self.params.suffix
        time_agg = self.params.time_agg
        full_agg: dict[str, Any] = {}

        if len(custom_cols) > 0:
            df_custom_cols = df[custom_cols]
            default_agg: dict[str, Callable] = {}

            for col in df_custom_cols.columns:
                if pd.api.types.infer_dtype(df_custom_cols[col]) in self.NUMERIC_DTYPES:
                    default_agg[col] = _numeric_values_processing  # type: ignore
                elif pd.api.types.infer_dtype(df_custom_cols[col]) == "string":
                    default_agg[col] = _string_values_processing  # type: ignore
                else:
                    doc_link = "https://pandas.pydata.org/docs/reference/api/pandas.api.types.infer_dtype.html"
                    message = (
                        f"Column '{col}' with "
                        f"'{pd.api.types.infer_dtype(df_custom_cols[col])}'"
                        f" data type is not supported for collapsing. See {doc_link}"
                    )

                    raise TypeError(message)

            full_agg = default_agg
        else:
            full_agg = {}

        df["grp"] = df[event_col] != df.groupby(user_col)[event_col].shift(1)
        df["cumgroup"] = df.groupby(user_col)["grp"].cumsum()
        df["count"] = df.groupby([user_col, "cumgroup"]).cumcount() + 1
        df["collapsed"] = df.groupby([user_col, "cumgroup", event_col])["count"].transform(max)
        df["collapsed"] = df["collapsed"].apply(lambda x: False if x == 1 else True)

        loops = (
            df[df["collapsed"] == 1]
            .groupby([user_col, "cumgroup", event_col])
            .agg({time_col: time_agg, "count": "max", **full_agg, event_index_col: time_agg})
            .reset_index()
        )
        loops[event_index_col] = loops[event_index_col].astype("int64")
        if suffix == "loop":
            loops[event_col] = loops[event_col].map(str) + "_loop"
        elif suffix == "count":
            loops[event_col] = loops[event_col].map(str) + "_loop_" + loops["count"].map(str)
        loops[type_col] = "group_alias"

        df_to_del = df[df["collapsed"] == 1]

        if len(custom_cols) > 0:
            cols_to_show = [user_col, time_col, type_col, event_col] + custom_cols
            link_url = (
                "https://doc.retentioneering.com/stable/doc/api/preprocessing/data_processors/collapse_loops.html"
            )
            message_template = """
                                    \nThere are NaN values in the {where_nans} columns!
                                    \nThe total amount of NaN values in each column:
                                    \n{total_nan_amount}
                                    \nAs a reference, here are some rows where NaNs occurred:
                                    \n{nan_example}{input_data_add_info}
                                    \nFor more information, see collapse_loops documentation {link}\n
                                """

            if df_to_del[custom_cols].isna().values.sum() > 0:
                cols_with_na = df_to_del[custom_cols].isna().sum()
                rows_to_show = df_to_del[df_to_del[custom_cols].isnull().any(axis=1)].head(3)

                warning_message = message_template.format(
                    where_nans="input",
                    total_nan_amount=cols_with_na,
                    nan_example=rows_to_show[cols_to_show],
                    input_data_add_info="\n\nThese NaN values will be ignored in the further calculation",
                    link=link_url,
                )

                warnings.warn(warning_message)

            if loops[custom_cols].isna().values.sum() > 0:
                cols_with_na = loops[custom_cols].isna().sum()
                rows_to_show = loops[loops[custom_cols].isnull().any(axis=1)].head(3)

                warning_message = message_template.format(
                    where_nans="custom",
                    total_nan_amount=cols_with_na,
                    nan_example=rows_to_show[cols_to_show],
                    input_data_add_info="",
                    link=link_url,
                )

                warnings.warn(warning_message)

        updated = source
        if not df_to_del.empty:
            updated = source[~source[event_id].isin(df_to_del[event_id])]

        loops[schema.event_id] = [uuid.uuid4() for x in range(len(loops))]

        result = pd.concat([updated, loops])
        result = result.drop(["cumgroup", "count"], axis=1, errors="ignore")

        collect_data_performance(
            scope="collapse_loops",
            event_name="metadata",
            called_params=self.to_dict()["values"],
            not_hash_values=["suffix", "time_agg"],
            performance_data={
                "parent": {
                    "shape": df.shape,
                    "hash": hash_dataframe(df),
                },
                "child": {
                    "shape": result.shape,
                    "hash": hash_dataframe(result),
                },
            },
        )

        return result
