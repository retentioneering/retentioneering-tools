"""Positions of events within a path, and the checks that keep them a path order.

A row's position is ``(index, subindex)``: ``index`` is its place in the path,
counted when the stream is built, and ``subindex`` is the rank of its event type
at that place (see `EventTypes`). Every operation that reads a path in sequence
reads it in this order, so it must be a strict order (ADR-0018):

- no two rows of a path share a position;
- the timestamp never decreases along it, so time arithmetic (gaps, durations,
  calendar bins) agrees with the order.
"""

from typing import List

import numpy as np
import pandas as pd

#: The ranks `EventTypes` used before ADR-0018 (0 to 4); the scale is now a
#: hundred times wider, so the old ranks map onto it exactly.
LEGACY_RANKS = range(5)
LEGACY_SCALE = 100


def upgrade_legacy_ranks(subindex: pd.Series) -> pd.Series:
    """Move ranks saved on the old 0–4 scale onto the current one.

    A frame written by `to_dataframe()` before ADR-0018 carries ranks 0 to 4.
    The current scale never uses 1 to 4, so a column whose ranks all lie in 0–4
    and include one of them can only be on the old scale; it is multiplied by
    100, which keeps every rank's meaning and its order.
    """
    values = subindex.dropna()
    if (
        not values.empty
        and values.isin(LEGACY_RANKS).all()
        and values.isin(LEGACY_RANKS[1:]).any()
    ):
        return subindex * LEGACY_SCALE
    return subindex


def as_positions(column: pd.Series, name: str) -> "tuple[pd.Series, str | None]":
    """The column as int64, or the reason it cannot be a position column."""
    if column.isna().any():
        return column, f"'{name}' has empty values"
    if pd.api.types.is_bool_dtype(column):
        return column, f"'{name}' must hold integers, not booleans"
    if pd.api.types.is_integer_dtype(column):
        return column.astype("int64"), None
    if pd.api.types.is_float_dtype(column):
        if np.all(np.mod(column.to_numpy(), 1) == 0):
            return column.astype("int64"), None
        return column, f"'{name}' must hold whole numbers"
    return column, f"'{name}' must hold integers, got dtype {column.dtype}"


def order_problems(
    df: pd.DataFrame, path_col: str, index_col: str, subindex_col: str, ts_col: str
) -> List[str]:
    """What breaks the path order in `df`; an empty list if nothing does.

    `df` must already be sorted by ``(path_col, index_col, subindex_col)``.
    """
    if len(df) < 2:
        return []
    path = df[path_col].to_numpy()
    index = df[index_col].to_numpy()
    subindex = df[subindex_col].to_numpy()
    ts = df[ts_col].to_numpy()
    same_path = path[1:] == path[:-1]
    problems = []
    shared = same_path & (index[1:] == index[:-1]) & (subindex[1:] == subindex[:-1])
    if shared.any():
        first = int(np.flatnonzero(shared)[0]) + 1
        problems.append(
            f"{int(shared.sum())} row(s) share a position ('{index_col}', "
            f"'{subindex_col}') with another row of their path, e.g. "
            f"{path_col}={path[first]!r} at ({index[first]}, {subindex[first]})"
        )
    earlier = same_path & (ts[1:] < ts[:-1])
    if earlier.any():
        first = int(np.flatnonzero(earlier)[0]) + 1
        problems.append(
            f"{int(earlier.sum())} row(s) have an earlier timestamp than the row "
            f"before them in ('{index_col}', '{subindex_col}') order, e.g. "
            f"{path_col}={path[first]!r} at ({index[first]}, {subindex[first]})"
        )
    return problems
