"""Path-level resolution of the ``diff`` convention.

The library's ``diff`` argument has three forms (see ``tools/types.py::T_Diff``):

* ``("segment_col", value1, value2)`` with the sentinels ``<REST>`` and ``<MISSING>``;
* ``(ids1, ids2)`` — two collections of path ids;
* ``None`` — no comparison.

The existing widgets split *events* by segment value, so a path whose segment changes
mid-path contributes events to both groups. Inferential methods (``get_ab_test``,
``get_sample_ratio``, ``funnel(ci_level=...)``, ``get_conversion_rate(diff=...)``) need
disjoint *paths*; this module resolves a ``diff`` into two disjoint path-id sets and
refuses overlapping or non-constant paths with a message that counts them.

A path belongs to a segment group when at least one of its events carries one of the
group's levels (the ``in_segment`` metric's ``mode="any"``); ``<MISSING>`` matches events
with no value and ``<REST>`` (second value only) means every other level.

Examples
--------
>>> groups = resolve_diff(stream, ("exp_group", "treatment", "control"))
>>> groups.n1(), groups.n2(), groups.excluded_paths
(19954, 20046, 0)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from retentioneering.exceptions import (
    DiffConfigError,
    EmptyEventstreamError,
    PathIdNotFoundError,
    SegmentLevelNotFoundError,
)

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream
    from retentioneering.tools.types import T_Diff

__all__ = ["DiffGroups", "resolve_diff", "normalize_assignment", "path_segment_values"]

REST = "<REST>"
MISSING = "<MISSING>"
MULTI = "<MULTI>"

_HINT = (
    "An experiment compares paths, so every path must sit in exactly one group: "
    "use a finer grain (e.g. path_col='session_id') if the value changes between "
    "sessions, or a path-level segment (add_segment(name, path_values=...))."
)


@dataclass(frozen=True)
class DiffGroups:
    """Two path populations produced by :func:`resolve_diff`.

    Attributes
    ----------
    ids1, ids2 : pandas.Index
        Path ids of group 1 and group 2 (sorted, unique). Disjoint whenever
        ``require_constant=True``; with ``require_constant=False`` a path that matched
        both groups is kept in both.
    label1, label2 : str
        Human labels (segment values, ``"group1"``/``"group2"`` for id lists, or arm
        names from an assignment table; ``<REST>`` for the complement).
    excluded_paths : int
        Paths that cannot be attributed to one group. Segment and id forms: paths that
        matched both groups or carry a non-constant segment value (always 0 with
        ``require_constant=True``, which raises instead). Assignment form: paths of the
        stream that the assignment table does not list (outside the ITT population).
    basis : str
        ``"segment"``, ``"ids"`` or ``"assignment"``.
    """

    ids1: pd.Index
    ids2: pd.Index
    label1: str
    label2: str
    excluded_paths: int
    basis: str

    def n1(self) -> int:
        """Number of paths in group 1."""
        return len(self.ids1)

    def n2(self) -> int:
        """Number of paths in group 2."""
        return len(self.ids2)


def _sorted_index(values) -> pd.Index:
    index = pd.Index(pd.unique(pd.Index(list(values))))
    try:
        return index.sort_values()
    except TypeError:
        return pd.Index(sorted(index, key=str))


def coerce_level(value: Any, available) -> Any:
    """Match a value that arrived as a string back to a typed segment level.

    The JS widget, MCP and any JSON round-trip stringify everything:
    ``'false' -> False``, ``'5' -> 5``. Returns the value unchanged when there is no
    unambiguous match; the caller then raises its normal error.
    """
    if value in available or not isinstance(value, str):
        return value
    matches = []
    for candidate in available:
        if isinstance(candidate, str):
            continue
        try:
            candidate_json = json.dumps(candidate)
        except (TypeError, ValueError):
            candidate_json = None
        if value in (candidate_json, str(candidate)):
            matches.append(candidate)
    return matches[0] if len(matches) == 1 else value


def path_segment_values(
    df: pd.DataFrame, segment_col: str, path_col: str
) -> tuple[pd.DataFrame, list]:
    """Distinct ``(path, level)`` pairs of a segment column.

    Returns
    -------
    tuple
        ``(pairs, categories)``: ``pairs`` has columns ``path`` and ``code`` (one row per
        distinct pair; ``code == -1`` is a missing value) and ``categories`` maps codes to
        levels.
    """
    series = df[segment_col]
    if not isinstance(series.dtype, pd.CategoricalDtype):
        series = series.astype("category")
    pairs = pd.DataFrame(
        {"path": df[path_col].to_numpy(), "code": series.cat.codes.to_numpy()}
    ).drop_duplicates(ignore_index=True)
    return pairs, series.cat.categories.tolist()


def per_path_label(df: pd.DataFrame, segment_col: str, path_col: str) -> pd.Series:
    """One value per path: the level, ``<MULTI>`` if not constant, ``<MISSING>`` if null."""
    pairs, categories = path_segment_values(df, segment_col, path_col)
    sizes = pairs.groupby("path", sort=False).size()
    first = pairs.drop_duplicates("path").set_index("path")["code"]
    lookup = np.array(list(categories) + [MISSING], dtype=object)
    labels = pd.Series(lookup[first.to_numpy()], index=first.index, dtype=object)
    labels[sizes.reindex(first.index).to_numpy() > 1] = MULTI
    labels.index.name = path_col
    try:
        return labels.sort_index()
    except TypeError:
        return labels.iloc[np.argsort([str(i) for i in labels.index], kind="stable")]


def normalize_assignment(assignment: pd.Series | pd.DataFrame) -> pd.DataFrame:
    """Validate an assignment table and return it as ``DataFrame(arm[, assigned_at])``.

    Accepts a Series ``path_id -> arm`` or a DataFrame indexed by path id with a column
    ``arm`` and optionally ``assigned_at`` (parsed with :func:`pandas.to_datetime`).
    Missing arms become ``<MISSING>``.

    Raises
    ------
    DiffConfigError
        Not a Series/DataFrame, no ``arm`` column, empty, or a non-unique index.
    """
    if isinstance(assignment, pd.Series):
        table = assignment.to_frame("arm")
    elif isinstance(assignment, pd.DataFrame):
        if "arm" not in assignment.columns:
            raise DiffConfigError(
                "assignment DataFrame needs an 'arm' column (index = path id); "
                f"got columns {list(assignment.columns)}"
            )
        cols = ["arm"] + (
            ["assigned_at"] if "assigned_at" in assignment.columns else []
        )
        table = assignment[cols].copy()
    else:
        raise DiffConfigError(
            "assignment must be a pandas Series (path id -> arm) or a DataFrame "
            f"with an 'arm' column, got {type(assignment).__name__}"
        )
    if table.empty:
        raise DiffConfigError("assignment table is empty")
    if not table.index.is_unique:
        dup = table.index[table.index.duplicated()].unique()
        raise DiffConfigError(
            f"assignment index must be unique (one arm per path); {len(dup)} path id(s) "
            f"repeat, e.g. {list(dup[:5])}"
        )
    arm = table["arm"].astype(object)
    table["arm"] = arm.where(arm.notna(), MISSING)
    if "assigned_at" in table.columns:
        table["assigned_at"] = pd.to_datetime(table["assigned_at"])
    return table


def _segment_groups(stream, segment_col, v1, v2, path_col):
    if segment_col not in stream.schema.segment_cols:
        raise DiffConfigError(f"'{segment_col}' is not a segment column")
    all_vals = set(stream.get_segment_levels().get(segment_col, []))
    v1 = coerce_level(v1, all_vals)
    if v1 not in all_vals:
        raise SegmentLevelNotFoundError(
            segment_level=v1,
            segment_col=segment_col,
            available_levels=sorted(all_vals, key=str),
        )
    if v2 == REST:
        v2_vals = list(all_vals - {v1})
        if not v2_vals:
            raise DiffConfigError(
                f"'{segment_col}' has no other levels besides '{v1}'; "
                "'<REST>' requires at least one complementary level."
            )
    else:
        v2 = coerce_level(v2, all_vals)
        if v2 not in all_vals:
            raise SegmentLevelNotFoundError(
                segment_level=v2,
                segment_col=segment_col,
                available_levels=sorted(all_vals, key=str),
            )
        v2_vals = [v2]

    pairs, categories = path_segment_values(stream.df, segment_col, path_col)
    code_of = {level: code for code, level in enumerate(categories)}
    code_of[MISSING] = -1
    codes1 = [code_of[v1]]
    codes2 = [code_of[v] for v in v2_vals]
    in1 = pairs["code"].isin(codes1)
    in2 = pairs["code"].isin(codes2)
    paths1 = pd.unique(pairs.loc[in1, "path"])
    paths2 = pd.unique(pairs.loc[in2, "path"])
    union = pd.unique(np.concatenate([paths1, paths2]))
    n_values = pairs.groupby("path", sort=False).size()
    non_constant = int((n_values.reindex(union) > 1).sum())
    overlap = len(set(paths1) & set(paths2))
    return paths1, paths2, str(v1), str(v2), non_constant, overlap


def _assignment_groups(stream, diff, path_col, assignment):
    if diff is None or len(diff) != 3:
        raise DiffConfigError(
            "with assignment=, diff must be ('segment_col', arm1, arm2) naming two "
            "arms of the assignment table"
        )
    segment_col, v1, v2 = diff[0], diff[1], diff[2]
    table = normalize_assignment(assignment)
    arms = set(table["arm"].unique().tolist())
    v1 = coerce_level(v1, arms)
    if v1 not in arms:
        raise SegmentLevelNotFoundError(
            segment_level=v1,
            segment_col="assignment",
            available_levels=sorted(arms, key=str),
        )
    if v2 == REST:
        v2_vals = arms - {v1}
        if not v2_vals:
            raise DiffConfigError(
                f"the assignment table has no arm besides '{v1}'; '<REST>' requires "
                "at least one complementary arm."
            )
    else:
        v2 = coerce_level(v2, arms)
        if v2 not in arms:
            raise SegmentLevelNotFoundError(
                segment_level=v2,
                segment_col="assignment",
                available_levels=sorted(arms, key=str),
            )
        v2_vals = {v2}
    paths1 = table.index[table["arm"] == v1]
    paths2 = table.index[table["arm"].isin(v2_vals)]

    stream_paths = pd.Index(pd.unique(stream.df[path_col]))
    excluded = int((~stream_paths.isin(table.index)).sum())

    if segment_col in stream.schema.segment_cols:
        labels = per_path_label(stream.df, segment_col, path_col)
        labels = labels[labels != MISSING]
        multi = labels.index[labels == MULTI]
        if len(multi):
            raise DiffConfigError(
                f"'{segment_col}' is not constant within {len(multi)} path(s) of the "
                f"stream, e.g. {list(multi[:5])}. {_HINT}"
            )
        common = labels.index.intersection(table.index)
        stream_arm = labels.reindex(common).astype(str)
        table_arm = table["arm"].reindex(common).astype(str)
        disagree = common[(stream_arm != table_arm).to_numpy()]
        if len(disagree):
            raise DiffConfigError(
                f"'{segment_col}' disagrees with the assignment table for "
                f"{len(disagree)} path(s), e.g. {list(disagree[:5])}: the arm a path "
                "was assigned to must be the arm its events carry."
            )
    return paths1, paths2, str(v1), str(v2), excluded


def resolve_diff(
    stream: "Eventstream",
    diff: "T_Diff",
    path_col: str | None = None,
    *,
    require_constant: bool = True,
    assignment: pd.Series | pd.DataFrame | None = None,
) -> DiffGroups:
    """Resolve a ``diff`` into two disjoint path-id sets.

    Parameters
    ----------
    stream : Eventstream
        Stream whose paths are split.
    diff : tuple
        ``("segment_col", v1, v2)`` or ``(ids1, ids2)``. ``None`` raises
        ``DiffConfigError`` (callers that accept ``diff=None`` never call this).
    path_col : str, optional
        Path grain; defaults to the stream's primary path column.
    require_constant : bool, default True
        When True, a path that belongs to both groups (overlapping id lists, or a
        segment that is not constant per path) raises ``DiffConfigError`` with the
        counts and the hint to use ``path_col="session_id"`` or a path-level segment.
        When False (legacy event-level behaviour used by ``Eventstream._split_two``),
        such paths are counted in ``excluded_paths`` and kept in both groups.
    assignment : pandas.Series or pandas.DataFrame, optional
        Per-path arm table (index = path id; values or column ``arm``). When given, the
        populations are the table's rows for the two labels (including paths with no
        events in ``stream``), and ``diff`` must be the segment form whose values are the
        two arms; a path whose segment disagrees with the table raises ``DiffConfigError``.
        The agreement check runs only when ``diff[0]`` is a segment column of ``stream``.

    Returns
    -------
    DiffGroups

    Raises
    ------
    DiffConfigError
        Malformed ``diff``; overlapping groups with ``require_constant=True``; assignment
        index not unique; assignment and segment disagree.
    SegmentLevelNotFoundError
        A segment value does not occur (and is not ``<REST>``/``<MISSING>``).
    PathIdNotFoundError
        An id in an id list is not in the stream (unless ``assignment`` is given).
    EmptyEventstreamError
        Either group is empty.
    """
    path_col = path_col or stream.schema.path_col
    if path_col not in stream.schema.path_cols:
        raise DiffConfigError(
            f"path_col '{path_col}' is not a path column; available: "
            f"{stream.schema.path_cols}"
        )

    if assignment is not None:
        paths1, paths2, label1, label2, excluded = _assignment_groups(
            stream, diff, path_col, assignment
        )
        basis = "assignment"
    else:
        if diff is None or isinstance(diff, (str, bytes)) or len(diff) not in (2, 3):
            raise DiffConfigError("diff must be (seg, v1, v2) or (ids1, ids2)")
        if len(diff) == 3:
            segment_col = diff[0]
            paths1, paths2, label1, label2, non_constant, overlap = _segment_groups(
                stream, segment_col, diff[1], diff[2], path_col
            )
            if require_constant and non_constant:
                raise DiffConfigError(
                    f"'{segment_col}' is not constant within {non_constant} of the "
                    f"compared paths ({path_col}); {overlap} of them fall in both "
                    f"groups ({label1!r} and {label2!r}). {_HINT}"
                )
            excluded = non_constant
            basis = "segment"
        else:
            ids1, ids2 = list(diff[0]), list(diff[1])
            available = set(pd.unique(stream.df[path_col]).tolist())
            missing = [i for i in ids1 + ids2 if i not in available]
            if missing:
                raise PathIdNotFoundError(sorted(set(missing), key=str), path_col)
            paths1, paths2 = pd.unique(pd.Index(ids1)), pd.unique(pd.Index(ids2))
            overlap = len(set(paths1) & set(paths2))
            if require_constant and overlap:
                raise DiffConfigError(
                    f"{overlap} path id(s) appear in both groups "
                    f"(group1: {len(paths1)}, group2: {len(paths2)}). {_HINT}"
                )
            excluded = overlap
            label1, label2 = "group1", "group2"
            basis = "ids"

    if len(paths1) == 0:
        raise EmptyEventstreamError("first diff group is empty")
    if len(paths2) == 0:
        raise EmptyEventstreamError("second diff group is empty")
    return DiffGroups(
        ids1=_sorted_index(paths1),
        ids2=_sorted_index(paths2),
        label1=label1,
        label2=label2,
        excluded_paths=int(excluded),
        basis=basis,
    )
