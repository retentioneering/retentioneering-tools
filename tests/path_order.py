"""The path-order invariant every data processor keeps (ADR-0018).

After any processor:

- no two rows of a path share a position, ``(index, subindex)``;
- the timestamp never decreases along that order;
- the rows come back in that order.

`tests/conftest.py` checks the output of every `DataProcessor.apply` call in
the suite against it.
"""

import pandas as pd

from retentioneering.eventstream.positions import order_problems
from retentioneering.eventstream.schema import EventstreamSchema


def path_order_problems(
    df: pd.DataFrame, schema: EventstreamSchema, rows_in_order: bool = True
) -> list:
    """What breaks the invariant in `df`. `rows_in_order=False` skips the check
    of the returned row order, for a frame that is only an input."""
    position = [schema.path_col, schema.index, schema.subindex]
    if df.empty or not set(position) <= set(df.columns):
        return []
    df = df.reset_index(drop=True)
    ordered = df.sort_values(position, kind="stable")
    problems = order_problems(
        ordered, schema.path_col, schema.index, schema.subindex, schema.timestamp_col
    )
    if not rows_in_order:
        return problems
    # Paths may come back in any order and even interleaved; each path's own
    # rows must come back in path order.
    returned = df.groupby(schema.path_col, observed=True, sort=False).cumcount()
    expected = (
        ordered.groupby(schema.path_col, observed=True, sort=False)
        .cumcount()
        .reindex(df.index)
    )
    if not returned.equals(expected):
        problems.append("the rows of a path are not returned in path order")
    return problems


def assert_path_order(df: pd.DataFrame, schema: EventstreamSchema) -> None:
    problems = path_order_problems(df, schema)
    assert not problems, "; ".join(problems)
