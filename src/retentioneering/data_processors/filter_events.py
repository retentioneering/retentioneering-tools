from typing import Callable, Dict, Tuple

import pandas as pd

from retentioneering import engine
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import (
    PreprocessingConfigError,
    PreprocessingColumnNotFoundError,
)

PROCESSOR_NAME = "filter_events"


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
        if not values:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"Values for column '{column}' in '{arg_name}' must not be empty.",
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

            # This loop still walks every value in Python, exactly as before.
            # The tables built below keep the ids out of the SQL text, not out
            # of the process.
            for column, values in column_filter.items():
                available_values = set(df[column].unique().tolist())
                unknown = [v for v in values if v not in available_values]
                if unknown:
                    message = f"Value(s) {unknown} not found in column '{column}'."
                    if column not in schema.path_cols:
                        message += (
                            f" Available values: {sorted(available_values, key=str)}"
                        )
                    raise PreprocessingConfigError(PROCESSOR_NAME, message)

            # The values for each column go to DuckDB as a small table, instead
            # of being written into the SQL text. Written inline, the query grew
            # with the number of values: 200k path ids made about 8 MB of SQL.
            # DuckDB then spent most of the query rewriting that huge expression,
            # which is work that grows with the number of ids and says nothing
            # about the data. Each table name is fixed to the position of its
            # column, so it can never clash with `df`, or with a table a caller
            # left behind through `sql=`. If a name does clash, the table we
            # register here is the one that wins (see `engine.run`).
            tables: Dict[str, pd.DataFrame] = {"df": df}
            conditions = []
            for i, (column, values) in enumerate(column_filter.items()):
                # Missing values are removed before the table is built, for two
                # reasons. First, `not in` over a list that holds a NULL gives
                # back NULL instead of TRUE, so one missing value would drop
                # every row, and `drop` would stop being the opposite of `keep`.
                # Second, a list of only missing values makes the table a DOUBLE
                # column, and DuckDB will not compare that with a text column.
                # We lose nothing by removing them, because `= NULL` is never
                # true, so a missing value never matched anything anyway.
                present = [v for v in values if not pd.isna(v)]
                if not present:
                    # Nothing in this column can match. So `keep` finds no rows,
                    # and `drop`, which is the same test negated, keeps them all.
                    conditions.append("false")
                    continue
                values_table = f"_filter_values_{i}"
                tables[values_table] = pd.DataFrame({"v": pd.Series(present)})
                conditions.append(
                    f"{engine.quote_ident(column)} in (select v from {values_table})"
                )

            if self.keep is not None:
                # keep: a row must match every entry (AND)
                where = " and ".join(conditions)
            else:
                # drop: a row is removed if it matches any entry (OR) —
                # the exact complement of keep
                where = "not (" + " or ".join(conditions) + ")"

            order_by = (
                f"{engine.quote_ident(schema.path_col)}, "
                f"{engine.quote_ident(schema.index)}, "
                f"{engine.quote_ident(schema.subindex)}"
            )
            query = f"""
                select * from df
                where {where}
                order by {order_by}
            """
            df = engine.run(query, **tables)

        elif self.sql is not None:
            columns_old = df.columns
            eventstream = df
            df = engine.run(self.sql, eventstream=eventstream)

            if set(df.columns) != set(columns_old):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    "The SQL query must return the same columns as the eventstream.",
                )

            order_by = (
                f"{engine.quote_ident(schema.path_col)}, "
                f"{engine.quote_ident(schema.index)}, "
                f"{engine.quote_ident(schema.subindex)}"
            )
            query = f"select * from df order by {order_by}"
            df = engine.run(query, df=df)

        else:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "Either 'keep', 'drop', 'func', or 'sql' must be provided.",
            )

        # duckdb sets all pandas categorical columns as ordered; setting them back to unordered
        for col in [schema.event_col] + schema.segment_cols:
            df[col] = df[col].astype("category")
            df[col] = df[col].cat.remove_unused_categories()
            df[col] = df[col].cat.as_unordered()

        return df, schema
