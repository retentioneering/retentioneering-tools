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
>>> groups.n1(), groups.n2(), groups.unassigned_paths
(19954, 20046, 0)
"""

from __future__ import annotations

import json
import os
import sys
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from retentioneering.exceptions import (
    DiffConfigError,
    EmptyEventstreamError,
    InvalidParameterError,
    PathIdNotFoundError,
    SegmentLevelNotFoundError,
)

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream
    from retentioneering.tools.types import T_Diff

__all__ = [
    "DiffGroups",
    "resolve_diff",
    "normalize_assignment",
    "align_assignment",
    "path_segment_values",
    "warn_user",
]

REST = "<REST>"
MISSING = "<MISSING>"
MULTI = "<MULTI>"
#: The arm column of an assignment table; also a valid ``diff[0]`` with ``assignment``.
ASSIGNMENT_ARM = "arm"

_HINT = (
    "An experiment compares paths, so every path must sit in exactly one group: "
    "use a finer grain (e.g. path_col='session_id') if the value changes between "
    "sessions, or a path-level segment (add_segment(name, path_values=...))."
)


_PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def warn_user(message: str, category: type[Warning] = UserWarning) -> None:
    """Emit a warning attributed to the first frame outside the library.

    A fixed ``stacklevel`` breaks whenever a decorator or a helper adds a frame; this
    walks up the stack instead, so the warning points at the caller's line.
    """
    frame = sys._getframe(1)
    level = 2
    while frame is not None and os.path.abspath(frame.f_code.co_filename).startswith(
        _PACKAGE_DIR + os.sep
    ):
        frame = frame.f_back
        level += 1
    warnings.warn(message, category, stacklevel=level)


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
    unassigned_paths : int
        Paths of the stream (at ``path_col``) that fall in neither group: another
        segment level, ids in neither list, or (assignment form) paths the table does
        not list or lists under a third arm. They are outside the compared population.
    basis : str
        ``"segment"``, ``"ids"`` or ``"assignment"``.
    """

    ids1: pd.Index
    ids2: pd.Index
    label1: str
    label2: str
    unassigned_paths: int
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
    InvalidParameterError
        Not a Series/DataFrame, no ``arm`` column, empty, or a non-unique index.
    """
    shape = [
        "a Series path id -> arm, or a DataFrame indexed by path id with an 'arm' column"
    ]
    if isinstance(assignment, pd.Series):
        table = assignment.to_frame("arm")
    elif isinstance(assignment, pd.DataFrame):
        if "arm" not in assignment.columns:
            raise InvalidParameterError(
                "assignment",
                f"a DataFrame with columns {list(assignment.columns)} and no 'arm'",
                shape,
            )
        cols = ["arm"] + (
            ["assigned_at"] if "assigned_at" in assignment.columns else []
        )
        table = assignment[cols].copy()
    else:
        raise InvalidParameterError("assignment", type(assignment).__name__, shape)
    if table.empty:
        raise InvalidParameterError("assignment", "an empty table", shape)
    if not table.index.is_unique:
        dup = table.index[table.index.duplicated()].unique()
        raise InvalidParameterError(
            "assignment",
            f"{len(dup)} path id(s) repeat in the index, e.g. {list(dup[:5])}",
            ["an index with one row per path (one arm per path)"],
        )
    arm = table["arm"].astype(object)
    table["arm"] = arm.where(arm.notna(), MISSING)
    if "assigned_at" in table.columns:
        table["assigned_at"] = pd.to_datetime(table["assigned_at"])
    return table


#: Below this share of table ids found among the stream's paths, ``align_assignment``
#: warns: most assigned paths are silent, or the table is keyed by another column.
MIN_MATCHED_SHARE = 0.5


def align_assignment(
    table: pd.DataFrame, stream: "Eventstream", path_col: str
) -> pd.DataFrame:
    """Match the ids of a normalised assignment table to the stream's path ids.

    When no table id matches a path id directly, the ids are matched by their string
    form (as ``add_segment(path_values=...)`` does: ids that went through JSON or a CSV
    arrive as strings or ints), and the matched rows take the stream's id.

    Raises
    ------
    InvalidParameterError
        No table id matches a path id of ``path_col`` even by string form: the table
        is keyed by another column or grain (a session table analysed per user), or
        the ids are spelled differently.

    Warns
    -----
    UserWarning
        Fewer than half of the table's ids match a path: either most assigned paths
        are silent (valid, e.g. an e-mail campaign) or the ids are partly misspelled.
        The warning names the counts so the two cases can be told apart.
    """
    stream_ids = pd.Index(pd.unique(stream.df[path_col]))
    matched = table.index.isin(stream_ids)
    if not matched.any() and len(stream_ids):
        as_str = pd.Index(stream_ids.astype(str))
        if as_str.is_unique:
            pos = as_str.get_indexer(pd.Index(table.index.astype(str)))
            if (pos >= 0).any():
                ids = [
                    stream_ids[p] if p >= 0 else own for p, own in zip(pos, table.index)
                ]
                aligned = pd.Index(ids, name=table.index.name)
                if aligned.is_unique:
                    table = table.set_axis(aligned, axis=0)
                    matched = pos >= 0
    n_matched, n_table = int(matched.sum()), len(table)
    detail = (
        f"only {n_matched} of {n_table} assignment ids "
        f"({n_matched / n_table:.0%}) match a '{path_col}' id of the stream "
        f"(table ids e.g. {list(table.index[:3])}, stream ids e.g. "
        f"{list(stream_ids[:3])})"
    )
    if n_matched == 0 and n_table:
        raise InvalidParameterError(
            "assignment",
            f"{detail}; every path would count as silent",
            [f"a table indexed by '{path_col}' ids, the grain the analysis runs at"],
        )
    if n_matched < MIN_MATCHED_SHARE * n_table:
        warn_user(
            f"{detail}; the rest count as silent paths (0 for every metric). That is "
            "right when most assigned paths never produced an event, and wrong when "
            f"the ids are spelled differently: index the table by '{path_col}'."
        )
    return table


def _segment_groups(stream, segment_col, v1, v2, path_col, require_constant=True):
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
        if require_constant and v2 == v1:
            raise DiffConfigError(
                f"diff compares '{segment_col}' level {v1!r} with itself: name two "
                "different levels."
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
    if diff is None or isinstance(diff, (str, bytes)) or len(diff) != 3:
        raise DiffConfigError(
            "with assignment=, diff must be ('segment_col', group1, group2) naming "
            "two arms of the assignment table, e.g. ('arm', 'treatment', 'control'); "
            f"got {diff!r}"
        )
    segment_col, v1, v2 = diff[0], diff[1], diff[2]
    segment_cols = list(stream.schema.segment_cols)
    if segment_col != ASSIGNMENT_ARM and segment_col not in segment_cols:
        # a typo here would silently skip the check that the stream agrees with the table
        raise DiffConfigError(
            f"with assignment=, diff[0] must be {ASSIGNMENT_ARM!r} (the table's arm "
            "column) or a segment column of the stream whose arm must agree with "
            f"the table; got {segment_col!r}, segment columns: {segment_cols}"
        )
    table = normalize_assignment(assignment)
    arms = set(table["arm"].unique().tolist())
    where = f"the assignment table's arm column '{ASSIGNMENT_ARM}'" + (
        f" (diff[0] is {segment_col!r}; diff[1] and diff[2] name the table's arms)"
        if segment_col != ASSIGNMENT_ARM
        else ""
    )
    v1 = coerce_level(v1, arms)
    if v1 not in arms:
        raise SegmentLevelNotFoundError(
            segment_level=v1,
            segment_col=ASSIGNMENT_ARM,
            available_levels=sorted(arms, key=str),
            where=where,
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
                segment_col=ASSIGNMENT_ARM,
                available_levels=sorted(arms, key=str),
                where=where,
            )
        if v2 == v1:
            raise DiffConfigError(
                f"diff compares arm '{v1}' with itself: name two different arms."
            )
        v2_vals = {v2}
    paths1 = table.index[table["arm"] == v1]
    paths2 = table.index[table["arm"].isin(v2_vals)]

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
    return paths1, paths2, str(v1), str(v2)


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
        such paths are kept in both groups.
    assignment : pandas.Series or pandas.DataFrame, optional
        Per-path arm table (index = path id; values or column ``arm``). When given, the
        populations are the table's rows for the two labels (including paths with no
        events in ``stream``), and ``diff`` must be the segment form whose values are the
        two arms; a path whose segment disagrees with the table raises ``DiffConfigError``.
        ``diff[0]`` is ``"arm"`` (the table alone) or a segment column of ``stream``,
        whose per-path value must then agree with the table.

    Returns
    -------
    DiffGroups

    Raises
    ------
    DiffConfigError
        Malformed ``diff``; overlapping groups or both values naming the same level
        (or arm) with ``require_constant=True``; with ``assignment``, ``diff[0]``
        neither ``"arm"`` nor a segment column, or a segment that disagrees with the
        table.
    InvalidParameterError
        A malformed assignment table (see :func:`normalize_assignment`).
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
        paths1, paths2, label1, label2 = _assignment_groups(
            stream, diff, path_col, assignment
        )
        basis = "assignment"
    else:
        if diff is None or isinstance(diff, (str, bytes)) or len(diff) not in (2, 3):
            raise DiffConfigError(
                "diff must be ('segment_col', group1, group2) or (ids1, ids2); "
                f"got {diff!r}"
            )
        if len(diff) == 2 and any(
            isinstance(side, (str, bytes)) or not hasattr(side, "__iter__")
            for side in diff
        ):
            # ["exp_group", "treatment"] would otherwise be read as two id lists
            # and iterated character by character
            raise DiffConfigError(
                f"diff {list(diff)!r} has two elements, so it is read as (ids1, ids2), "
                "two collections of path ids. To compare segment levels pass three: "
                "('segment_col', group1, group2), e.g. "
                "('exp_group', 'treatment', 'control')."
            )
        if len(diff) == 3:
            segment_col = diff[0]
            paths1, paths2, label1, label2, non_constant, overlap = _segment_groups(
                stream, segment_col, diff[1], diff[2], path_col, require_constant
            )
            if require_constant and non_constant:
                raise DiffConfigError(
                    f"'{segment_col}' is not constant within {non_constant} of the "
                    f"compared paths ({path_col}); {overlap} of them fall in both "
                    f"groups ({label1!r} and {label2!r}). {_HINT}"
                )
            if require_constant and overlap:
                raise DiffConfigError(
                    f"{overlap} path(s) fall in both groups ({label1!r} and "
                    f"{label2!r}). {_HINT}"
                )
            basis = "segment"
        else:
            ids1, ids2 = list(diff[0]), list(diff[1])
            available = set(pd.unique(stream.df[path_col]).tolist())
            missing = [i for i in ids1 + ids2 if i not in available]
            if missing:
                raise PathIdNotFoundError(
                    sorted(set(missing), key=str),
                    path_col,
                    hint=(
                        "An id list names paths of the stream; assigned paths with "
                        "no events are not in it. For an experiment whose arms list "
                        "silent paths too, pass the table as assignment= with "
                        "diff=('arm', group1, group2)"
                    ),
                )
            paths1, paths2 = pd.unique(pd.Index(ids1)), pd.unique(pd.Index(ids2))
            overlap = len(set(paths1) & set(paths2))
            if require_constant and overlap:
                raise DiffConfigError(
                    f"{overlap} path id(s) appear in both groups "
                    f"(group1: {len(paths1)}, group2: {len(paths2)}). {_HINT}"
                )
            label1, label2 = "group1", "group2"
            basis = "ids"

    if len(paths1) == 0:
        raise EmptyEventstreamError("first diff group is empty")
    if len(paths2) == 0:
        raise EmptyEventstreamError("second diff group is empty")
    stream_paths = pd.Index(pd.unique(stream.df[path_col]))
    grouped = pd.Index(paths1).append(pd.Index(paths2))
    return DiffGroups(
        ids1=_sorted_index(paths1),
        ids2=_sorted_index(paths2),
        label1=label1,
        label2=label2,
        unassigned_paths=int((~stream_paths.isin(grouped)).sum()),
        basis=basis,
    )
