from typing import Callable, Collection, Tuple

import duckdb
import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError

PROCESSOR_NAME = "add_segment"

_ROW_IDX_COL = "__retentioneering_row_idx__"


def _inject_row_idx(sql: str) -> str:
    """
    Injects _ROW_IDX_COL into the outermost SELECT of a SQL query.

    DuckDB reorders rows when executing window functions with PARTITION BY,
    so we inject a row index column into the result to restore the original order.

    Works for both plain SELECT and CTEs (WITH ... SELECT ...).
    """
    stripped = sql.strip()
    depth = 0
    i = 0
    in_single_quote = False
    in_double_quote = False

    while i < len(stripped):
        c = stripped[i]

        if in_single_quote:
            if c == "'" and (i == 0 or stripped[i - 1] != "\\"):
                in_single_quote = False
        elif in_double_quote:
            if c == '"' and (i == 0 or stripped[i - 1] != "\\"):
                in_double_quote = False
        elif c == "'":
            in_single_quote = True
        elif c == '"':
            in_double_quote = True
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and stripped[i : i + 6].upper() == "SELECT":
            before_ok = i == 0 or not (
                stripped[i - 1].isalnum() or stripped[i - 1] == "_"
            )
            end_pos = i + 6
            after_ok = end_pos >= len(stripped) or not (
                stripped[end_pos].isalnum() or stripped[end_pos] == "_"
            )
            if before_ok and after_ok:
                return stripped[:end_pos] + f" {_ROW_IDX_COL}," + stripped[end_pos:]

        i += 1

    raise PreprocessingConfigError(
        PROCESSOR_NAME, "Could not find SELECT statement in SQL query."
    )


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _build_funnel_segment_query(
    path_col: str,
    event_col: str,
    index_col: str,
    funnel_events: list[str],
    result_col: str,
) -> str:
    """
    Per-row DuckDB query using PARTITION BY window functions — single pass,
    no JOIN needed.  Mirrors funnel.py: MAX(index) enforces sequential order.

    Segment values (named after the last event reached in sequence):
      funnel_events[k]  — path reached step k but not k+1
      'out_of_funnel'   — path never reached step 0

    Reads from the 'eventstream' variable in caller scope (same as sql= mode).
    Row order is restored via _ROW_IDX_COL.

    Example output for funnel_events=['add_to_cart', 'purchase'], path_id='session',
    event='event', index='__index__':

        SELECT __retentioneering_row_idx__,
            CASE
                WHEN SUM(CASE WHEN event = 'purchase' THEN 1 ELSE 0 END) OVER (PARTITION BY session) > 0
                     AND SUM(CASE WHEN event = 'add_to_cart' THEN 1 ELSE 0 END) OVER (PARTITION BY session) > 0
                     AND MAX(CASE WHEN event = 'add_to_cart' THEN __index__ ELSE 0 END) OVER (PARTITION BY session)
                       < MAX(CASE WHEN event = 'purchase'    THEN __index__ ELSE 0 END) OVER (PARTITION BY session)
                THEN 'purchase'
                WHEN SUM(CASE WHEN event = 'add_to_cart' THEN 1 ELSE 0 END) OVER (PARTITION BY session) > 0
                THEN 'add_to_cart'
                ELSE 'out_of_funnel'
            END AS funnel
        FROM eventstream
    """
    w = f"PARTITION BY {path_col}"

    def _sum(ev: str) -> str:
        return (
            f"SUM(CASE WHEN {event_col} = {_sql_str(ev)} THEN 1 ELSE 0 END) OVER ({w})"
        )

    def _max(ev: str) -> str:
        return f"MAX(CASE WHEN {event_col} = {_sql_str(ev)} THEN {index_col} ELSE 0 END) OVER ({w})"

    def _reached_k(k: int) -> str:
        has = " AND ".join(f"{_sum(funnel_events[i])} > 0" for i in range(k + 1))
        order = " AND ".join(
            f"{_max(funnel_events[i])} < {_max(funnel_events[i + 1])}" for i in range(k)
        )
        return f"{has} AND {order}" if order else has

    whens = "\n        ".join(
        f"WHEN {_reached_k(k)} THEN {_sql_str(funnel_events[k])}"
        for k in range(len(funnel_events) - 1, -1, -1)
    )
    return (
        f"SELECT {_ROW_IDX_COL},\n"
        f"    CASE {whens}\n"
        f"         ELSE 'out_of_funnel'\n"
        f"    END AS {result_col}\n"
        f"FROM eventstream"
    )


class AddSegment(DataProcessor):
    name: str
    rules: Collection | None
    func: Callable | None
    sql: str | None
    funnel_events: list | None
    path_col: str | None

    def __init__(
        self,
        name: str,
        rules: Collection | None = None,
        func: Callable | None = None,
        sql: str | None = None,
        funnel_events: list | None = None,
        path_col: str | None = None,
    ) -> None:
        arg_is_not_none = [
            func is not None,
            rules is not None,
            sql is not None,
            funnel_events is not None,
        ]
        if sum(arg_is_not_none) != 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "One and only one of the arguments must be defined: "
                "rules, func, sql, funnel_events.",
            )
        if funnel_events is not None and len(funnel_events) < 2:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "funnel_events must have at least 2 events."
            )

        self.name = name
        self.rules = rules
        self.func = func
        self.sql = sql
        self.funnel_events = funnel_events
        self.path_col = path_col
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        if self.name in df.columns:
            if self.name in schema.segment_cols:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, f"Segment '{self.name}' already exists."
                )
            else:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"Name '{self.name}' is already reserved in the eventstream.",
                )

        values = None

        if self.rules is not None:
            if not isinstance(self.rules, Collection):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Segment rules must be a collection."
                )

            cases = "CASE"
            for item in self.rules[:-1]:
                column, op, value, segment_value = item
                if isinstance(value, str) and op.lower() != "in":
                    value = _sql_str(value)
                cases += (
                    f"\nWHEN {column} {op} {value} THEN {_sql_str(str(segment_value))}"
                )
            else_segment_value = self.rules[-1][0]
            cases += f"\nELSE {_sql_str(str(else_segment_value))}"
            cases += f"\nEND AS {self.name}"

            sql = f"SELECT {cases} FROM df"
            result = duckdb.sql(sql).df()
            values = result[result.columns[0]].tolist()

        elif self.sql is not None:
            if not isinstance(self.sql, str):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "SQL query must be a string."
                )

            # Copy df and add a row index so we can restore original order after
            # DuckDB reorders rows during window function (PARTITION BY) execution.
            eventstream = df.copy()
            eventstream[_ROW_IDX_COL] = range(len(df))

            tracking_sql = _inject_row_idx(self.sql)
            result = duckdb.sql(tracking_sql).df()

            if len(result.columns) != 2:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "SQL script must return a single column."
                )

            result = result.sort_values(_ROW_IDX_COL).reset_index(drop=True)
            values = result.iloc[:, 1].tolist()

        elif self.func is not None:
            if not isinstance(self.func, Callable):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Function must be callable."
                )
            result = self.func(df)
            if not isinstance(result, Collection):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Function must return a collection."
                )
            values = list(result)

        elif self.funnel_events is not None:
            pid_col = self.path_col or schema.path_col
            # Add row index so we can restore original order after DuckDB execution
            eventstream = df.copy()
            eventstream[_ROW_IDX_COL] = range(len(df))
            query = _build_funnel_segment_query(
                pid_col,
                schema.event_col,
                schema.index,
                self.funnel_events,
                "__funnel_level__",
            )
            result = duckdb.sql(query).df()
            result = result.sort_values(_ROW_IDX_COL).reset_index(drop=True)
            values = result["__funnel_level__"].tolist()

        new_df = df.copy()
        new_df[self.name] = values
        new_df[self.name] = new_df[self.name].astype("category")
        new_schema = schema.copy()
        new_schema.segment_cols.append(self.name)

        return new_df, new_schema
