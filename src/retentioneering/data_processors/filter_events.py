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


class FilterEvents(DataProcessor):
    values: Dict | None
    func: Callable[[pd.DataFrame], pd.Series] | None
    sql: str | None

    def __init__(self, values=None, func=None, sql=None) -> None:
        self.values = values
        self.func = func
        self.sql = sql

        func_arg_name = f"{func=}".split("=")[0]
        values_arg_name = f"{values=}".split("=")[0]
        sql_arg_name = f"{sql=}".split("=")[0]

        arg_is_not_none = [func is not None, values is not None, sql is not None]
        if sum(arg_is_not_none) != 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"One and only one of the arguments must be provided: {func_arg_name}, {values_arg_name}, {sql_arg_name}.",
            )

        if func is not None and not isinstance(func, Callable):
            arg_name = f"{func=}".split("=")[0]
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"Argument '{arg_name}' must be a callable function."
            )

        if values is not None:
            if not isinstance(values, dict):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Argument 'values' must be a dictionary."
                )
            if "column" not in values or "values" not in values:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    "Argument 'values' must have 'column' and 'values' keys.",
                )
            if "exclude" in values and not isinstance(values["exclude"], bool):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Key 'exclude' in 'values' must be a boolean."
                )

        if sql is not None and not isinstance(sql, str):
            arg_name = f"{sql=}".split("=")[0]
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"Argument '{arg_name}' must be a string."
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

        elif self.values is not None:
            filter_column = self.values["column"]
            filter_values = self.values["values"]
            exclude = self.values.get("exclude", False)

            if filter_column not in df.columns:
                raise PreprocessingColumnNotFoundError(
                    PROCESSOR_NAME, filter_column, df.columns.tolist()
                )
            filter_values_str = ", ".join(_sql_literal(v) for v in filter_values)
            operator = "not in" if exclude else "in"
            query = f"""
                select * from df
                where {filter_column} {operator} ({filter_values_str})
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
                PROCESSOR_NAME, "Either 'values', 'func', or 'sql' must be provided."
            )

        # duckdb sets all pandas categorical columns as ordered; setting them back to unordered
        for col in schema.event_cols + schema.segment_cols:
            df[col] = df[col].astype("category")
            df[col] = df[col].cat.remove_unused_categories()
            df[col] = df[col].cat.as_unordered()

        return df, schema
