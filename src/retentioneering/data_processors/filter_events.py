from typing import Callable, Dict, Tuple

import duckdb
import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import (
    PreprocessingConfigError,
    PreprocessingColumnNotFoundError,
)

PROCESSOR_NAME = "filter_events"


def _sql_literal(value) -> str:
    """Render a Python value as a safe DuckDB SQL literal."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _validate_column_filter(arg_name: str, value: Dict) -> None:
    if not isinstance(value, dict) or not value:
        raise PreprocessingConfigError(
            PROCESSOR_NAME,
            f"Argument '{arg_name}' must be a non-empty {{column: values}} dictionary.",
        )
    for column, values in value.items():
        if not isinstance(column, str):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"Column names in '{arg_name}' must be strings."
            )
        if not isinstance(values, (list, tuple, set)):
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"Values for column '{column}' in '{arg_name}' must be a list.",
            )


class FilterEvents(DataProcessor):
    keep: Dict | None
    drop: Dict | None
    func: Callable[[pd.DataFrame], pd.Series] | None
    sql: str | None

    def __init__(self, keep=None, drop=None, func=None, sql=None) -> None:
        self.keep = keep
        self.drop = drop
        self.func = func
        self.sql = sql

        arg_is_not_none = [
            keep is not None,
            drop is not None,
            func is not None,
            sql is not None,
        ]
        if sum(arg_is_not_none) != 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "One and only one of the arguments must be provided: keep, drop, func, sql.",
            )

        if func is not None and not isinstance(func, Callable):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Argument 'func' must be a callable function."
            )

        if keep is not None:
            _validate_column_filter("keep", keep)
        if drop is not None:
            _validate_column_filter("drop", drop)

        if sql is not None and not isinstance(sql, str):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Argument 'sql' must be a string."
            )

        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        if self.func is not None:
            mask = self.func(df)
            if len(mask) != len(df):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    "The filter function must return a boolean mask of the same length as the eventstream.",
                )
            df = df[mask].copy()

        elif self.keep is not None or self.drop is not None:
            column_filter = self.keep if self.keep is not None else self.drop
            for column in column_filter:
                if column not in df.columns:
                    raise PreprocessingColumnNotFoundError(
                        PROCESSOR_NAME, column, df.columns.tolist()
                    )

            conditions = []
            for column, values in column_filter.items():
                values_str = ", ".join(_sql_literal(v) for v in values)
                conditions.append(f"{column} in ({values_str})")

            if self.keep is not None:
                # keep: a row must match every entry (AND)
                where = " and ".join(conditions)
            else:
                # drop: a row is removed if it matches any entry (OR) —
                # the exact complement of keep
                where = "not (" + " or ".join(conditions) + ")"

            query = f"""
                select * from df
                where {where}
                order by {schema.path_col}, {schema.index}, {schema.subindex}
            """
            df = duckdb.sql(query).df()

        elif self.sql is not None:
            columns_old = df.columns
            eventstream = df  # noqa: F841 -- exposed to user SQL as `eventstream` (DuckDB replacement scan)
            df = duckdb.sql(self.sql).df()

            if set(df.columns) != set(columns_old):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    "The SQL query must return the same columns as the eventstream.",
                )

            query = f"select * from df order by {schema.path_col}, {schema.index}, {schema.subindex}"
            df = duckdb.sql(query).df()

        else:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "Either 'keep', 'drop', 'func', or 'sql' must be provided.",
            )

        # duckdb sets all pandas categorical columns as ordered; setting them back to unordered
        for col in schema.event_cols + schema.segment_cols:
            df[col] = df[col].astype("category")
            df[col] = df[col].cat.remove_unused_categories()
            df[col] = df[col].cat.as_unordered()

        return df, schema
