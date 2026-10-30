"""Sample ratio mismatch (SRM) check.

An A/B test whose arms do not have the expected sizes is broken before any metric is
read: the assignment, the logging or a filter lost paths from one arm. ``get_sample_ratio``
compares the number of paths per arm against expected shares with a chi-square
goodness-of-fit test and also flags paths that carry more than one arm value.

The check is a *design* check: run it on the assignment table (``assignment=``) or on the
full stream before any post-exposure filter. On a subsample defined by behaviour (for
example users who reached the cart) a share difference is a treatment effect, not a
mismatch, and the chi-square statistic is algebraically the conversion z-test squared
(``z^2 = chi2 / (1 - p_pooled)``), so the function is documented as not applicable there.

Examples
--------
>>> stream.get_sample_ratio("exp_group")
{'levels': ..., 'chi2': 1.04, 'dof': 1, 'p_value': 0.308, 'alpha': 0.001,
 'mismatch': False, 'multi_valued_paths': 0, 'silent_paths': 0, 'basis': 'paths in stream'}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _stats
from retentioneering.tools._diff import (
    ASSIGNMENT_ARM,
    MISSING,
    MULTI,
    coerce_level,
    normalize_assignment,
    per_path_label,
)
from retentioneering.tools.ab_test import (
    _calendar_period,
    check_segment_unit,
    prepare_assignment,
)

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream

__all__ = ["SampleRatio", "ALL", "sample_ratio_reading"]

#: ``stratum`` / ``period`` label of the overall rows in ``levels``.
ALL = "<ALL>"
#: Names of the calendar units ``period`` accepts, for ``reading``.
_UNIT_NAMES = {"D": "day", "W": "week", "M": "month", "Q": "quarter", "Y": "year"}

LEVEL_COLUMNS = ["level", "paths", "share", "expected_share", "expected_paths"]


def _p_text(p: float) -> str:
    return "< 0.0001" if p < 1e-4 else ("1" if p >= 0.995 else f"{p:#.2g}")


def _share_text(share: float) -> str:
    return f"{round(100 * share, 1):g}"


#: The consequence of a mismatch, in ``reading``.
INVALID = "the test is invalid until the assignment is fixed"


def sample_ratio_reading(result: dict[str, Any]) -> str:
    """One sentence that reads a ``get_sample_ratio`` result from its own numbers.

    ``"expected 50/50, observed 4960/5040 (control/treatment), p 0.42: no mismatch"``,
    or ``"mismatch: ... — the test is invalid until the assignment is fixed"``; paths
    carrying two arms, uncounted paths and the strata or periods that mismatch are
    added when present.
    """
    levels = result["levels"]
    for key in ("stratum", "period"):
        if key in levels.columns:
            levels = levels[levels[key] == ALL]
    names = "/".join(str(v) for v in levels["level"])
    split = (
        f"expected {'/'.join(_share_text(v) for v in levels['expected_share'])}, "
        f"observed {'/'.join(str(int(v)) for v in levels['paths'])} ({names})"
    )
    p = result["p_value"]
    multi = int(result["multi_valued_paths"])
    alpha = result["alpha"]
    problems = []
    if not np.isfinite(p):
        problems_text = f"{split}: no path counted, the check cannot run"
    else:
        if p < alpha:
            problems.append(f"{split}, p {_p_text(p)} (alpha {alpha:g})")
        if multi:
            problems.append(
                f"{multi} path(s) carry more than one arm or disagree with the "
                "assignment table"
            )
        if problems:
            if p >= alpha:
                problems.insert(0, f"{split}, p {_p_text(p)}")
            problems_text = f"mismatch: {'; '.join(problems)} — {INVALID}"
        else:
            problems_text = f"{split}, p {_p_text(p)}: no mismatch"
    parts = [problems_text]
    unassigned = int(result.get("unassigned_paths", 0))
    if unassigned:
        parts.append(f"{unassigned} path(s) with no arm in the split are not counted")
    for table_key, key, name in (
        ("strata", "stratum", result.get("strata_col")),
        ("periods", "period", _period_name(result.get("period"))),
    ):
        groups = result.get(table_key)
        if groups is None:
            continue
        bad = groups[groups["mismatch"].astype(bool)]
        if len(bad):
            shown = ", ".join(
                f"{label} (adjusted p {_p_text(p)})"
                for label, p in zip(bad[key].head(3), bad["p_value_adjusted"].head(3))
            )
            more = f" and {len(bad) - 3} more" if len(bad) > 3 else ""
            parts.append(f"by {name}: mismatch in {shown}{more} — {INVALID} there")
        elif np.isfinite(p) and p < alpha:
            parts.append(f"by {name}: no single {name} mismatches on its own")
        else:
            parts.append(f"by {name}: no group mismatches")
    return "; ".join(parts)


def _period_name(period: Any) -> str:
    """``"day"``, ``"week"``, ``"month"``; the value itself for another spelling."""
    if period is None:
        return ""
    text = str(period)
    return _UNIT_NAMES.get(text.split("-")[0].rstrip("SE") or text, text)


def _period_freq(period: Any) -> str:
    """The pandas period frequency of one calendar unit (``"D"``, ``"W-SUN"``, ``"M"``)."""
    allowed = ["one calendar unit: 'D', 'W' (or 'W-MON'...), 'M', 'Q', 'Y'"]
    length, freq, n = _calendar_period(period)
    if length is not None:
        if length != pd.Timedelta(days=1):
            raise InvalidParameterError("period", period, allowed)
        return "D"
    if n != 1:
        raise InvalidParameterError("period", period, allowed)
    return str(freq)


def _counts_table(arms: pd.Series, levels: list, shares: np.ndarray) -> pd.DataFrame:
    counts = arms.value_counts().reindex(levels, fill_value=0).to_numpy()
    total = counts.sum()
    with np.errstate(invalid="ignore", divide="ignore"):
        share = counts / total if total else np.full(len(levels), np.nan)
    return pd.DataFrame(
        {
            "level": levels,
            "paths": counts.astype(int),
            "share": share,
            "expected_share": shares,
            "expected_paths": shares * total,
        }
    )


@dataclass
class SampleRatio:
    """Headless SRM computation (used by ``Eventstream.get_sample_ratio`` and the MCP tool)."""

    stream: "Eventstream"

    def fit(
        self,
        segment_col: str | None = None,
        expected_shares: dict[str, float] | None = None,
        alpha: float = 0.001,
        path_col: str | None = None,
        assignment: pd.Series | pd.DataFrame | None = None,
        strata_col: str | None = None,
        period: Any = None,
    ) -> dict[str, Any]:
        """Compare arm sizes against expected shares.

        Parameters
        ----------
        segment_col : str, optional
            Segment column holding the arm (``schema.segment_cols``). Required without
            ``assignment``. With ``assignment`` it is optional (``None`` or ``"arm"``:
            the table alone); a segment column of the stream must then agree with the
            table path by path.
        expected_shares : dict, optional
            ``{level: share}``; shares must sum to 1 within 1e-9. Default: equal split over
            the column's levels (or over the assignment table's arms). Levels of the
            column that are not named here are not arms: their paths are reported in
            ``unassigned_paths``.
        alpha : float, default 0.001
            Threshold for ``mismatch``. SRM alarms are usually set stricter than 0.05.
        path_col : str, optional
            Path grain; default primary path column.
        assignment : pandas.Series or pandas.DataFrame, optional
            Per-path arm table; when given the counts are the table's rows (silent paths
            included) and ``silent_paths`` reports how many have no events in the stream.
        strata_col : str, optional
            Repeat the check within strata: a column of the assignment DataFrame (used
            first) or a segment column constant per path. Valid only for a variable
            fixed at assignment time: per-stratum shares on a variable the treatment
            can change are an effect, not a bug check.
        period : str, optional
            Repeat the check per calendar unit of ``assigned_at`` (or of the path's
            first event without an assignment table): ``"D"``, ``"W"``, ``"M"``,
            ``"Q"``, ``"Y"``, spelled as in ``get_ab_test``. Not with ``strata_col``.

        Returns
        -------
        dict
            See :meth:`Eventstream.get_sample_ratio`.
        """
        stream = self.stream
        schema = stream.schema
        path_col = path_col or schema.path_col
        if path_col not in schema.path_cols:
            raise InvalidParameterError("path_col", path_col, list(schema.path_cols))
        if not isinstance(alpha, (int, float)) or not 0 < alpha < 1:
            raise InvalidParameterError("alpha", alpha, ["a float in (0, 1)"])
        segment_cols = list(schema.segment_cols)
        if assignment is None:
            if segment_col is None:
                raise InvalidParameterError(
                    "segment_col",
                    segment_col,
                    segment_cols,
                    reason=(
                        "name the stream's arm column, or pass assignment= (a "
                        "per-path arm table, as for get_ab_test) to check the "
                        "table's arms"
                    ),
                )
            if segment_col not in segment_cols:
                raise InvalidParameterError("segment_col", segment_col, segment_cols)
        elif segment_col is not None and segment_col not in segment_cols:
            if segment_col != ASSIGNMENT_ARM:
                raise InvalidParameterError(
                    "segment_col", segment_col, [None, ASSIGNMENT_ARM] + segment_cols
                )
        if strata_col is not None and period is not None:
            raise InvalidParameterError(
                "period",
                period,
                ["None when strata_col is given (one split per check)"],
            )
        freq = _period_freq(period) if period is not None else None
        in_stream = segment_col is not None and segment_col in segment_cols

        stream_labels = (
            per_path_label(stream.df, segment_col, path_col) if in_stream else None
        )
        silent = 0
        multi = 0
        outside = 0
        if assignment is None:
            check_segment_unit(stream, path_col, segment_col)
            labels = stream_labels
            multi = int((labels == MULTI).sum())
            candidates = [
                v
                for v in stream.get_segment_levels().get(segment_col, [])
                if v != MISSING
            ]
            arms = labels[(labels != MULTI) & (labels != MISSING)]
            n_missing = int((labels == MISSING).sum())
            table = None
            basis = f"paths in stream ({path_col})"
        else:
            table = prepare_assignment(
                stream, path_col, normalize_assignment(assignment)
            )
            if isinstance(assignment, pd.DataFrame):
                # `strata_col` reads the caller's columns: keep them on the matched ids
                assignment = assignment.set_axis(table.index, axis=0)
            stream_paths = pd.Index(pd.unique(stream.df[path_col]))
            silent = int((~table.index.isin(stream_paths)).sum())
            # stream paths the table does not list have no arm
            outside = int((~stream_paths.isin(table.index)).sum())
            if stream_labels is not None:
                common = stream_labels.index.intersection(table.index)
                seen = stream_labels.reindex(common)
                expected = table["arm"].reindex(common)
                bad = (seen == MULTI) | (
                    (seen != MISSING) & (seen.astype(str) != expected.astype(str))
                )
                multi = int(bad.sum())
            arms = table["arm"][table["arm"] != MISSING]
            n_missing = int((table["arm"] == MISSING).sum())
            candidates = list(pd.unique(arms))
            basis = (
                f"assignment table ({len(table)} paths, {silent} without events in "
                f"the stream)"
            )

        levels, shares = self._levels_and_shares(candidates, expected_shares)
        arms = arms.astype(object)
        counted = arms[arms.isin(levels)]
        unassigned = int(len(arms) - len(counted)) + n_missing + outside

        overall = _counts_table(counted, levels, shares)
        chi2, p_value, dof = _stats.srm_chi2(overall["paths"].to_numpy(), shares)
        result: dict[str, Any] = {
            "levels": overall,
            "chi2": chi2,
            "dof": dof,
            "p_value": p_value,
            "alpha": float(alpha),
            "mismatch": bool((np.isfinite(p_value) and p_value < alpha) or multi > 0),
            "multi_valued_paths": multi,
            "silent_paths": silent,
            "unassigned_paths": unassigned,
            "basis": basis,
        }
        if strata_col is not None:
            groups = self._strata(strata_col, path_col, counted.index, assignment)
            result.update(
                self._split_tables(
                    counted, groups, levels, shares, alpha, overall, "stratum", "strata"
                )
            )
            result["strata_col"] = strata_col
        if freq is not None:
            groups = self._periods(freq, path_col, table, counted.index)
            result.update(
                self._split_tables(
                    counted, groups, levels, shares, alpha, overall, "period", "periods"
                )
            )
            result["period"] = period
        result["reading"] = sample_ratio_reading(result)
        return result

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _levels_and_shares(candidates, expected_shares):
        if expected_shares is None:
            levels = sorted(candidates, key=str)
            if len(levels) < 2:
                raise InvalidParameterError(
                    "segment_col",
                    f"{len(levels)} level(s)",
                    ["a column with at least two arms"],
                )
            return levels, np.full(len(levels), 1.0 / len(levels))
        if not isinstance(expected_shares, dict) or len(expected_shares) < 2:
            raise InvalidParameterError(
                "expected_shares",
                expected_shares,
                ["a dict {level: share} with >= 2 levels"],
            )
        available = set(candidates)
        levels, shares = [], []
        for key, share in expected_shares.items():
            level = coerce_level(key, available)
            if level not in available:
                raise InvalidParameterError(
                    "expected_shares", key, sorted(map(str, available))
                )
            levels.append(level)
            shares.append(float(share))
        shares_arr = np.asarray(shares)
        if np.any(shares_arr < 0) or abs(shares_arr.sum() - 1.0) > 1e-9:
            raise InvalidParameterError(
                "expected_shares", expected_shares, ["non-negative shares summing to 1"]
            )
        return levels, shares_arr

    def _periods(self, freq, path_col, table, ids) -> pd.Series:
        stream = self.stream
        if table is not None and "assigned_at" in table.columns:
            at = table["assigned_at"]
        else:
            ts = stream.schema.timestamp_col
            at = stream.df.groupby(path_col, observed=True)[ts].min()
            if table is not None:
                at = at.reindex(table.index)
        # calendar units in the timestamps' own timezone: bin the local wall time
        if getattr(at.dt, "tz", None) is not None:
            at = at.dt.tz_localize(None)
        known = at.notna()
        labels = pd.Series(MISSING, index=at.index, dtype=object)
        labels[known] = at[known].dt.to_period(freq).astype(str)
        return labels.reindex(ids).fillna(MISSING).astype(object)

    def _strata(self, strata_col, path_col, ids, assignment) -> pd.Series:
        stream = self.stream
        if isinstance(assignment, pd.DataFrame) and strata_col in assignment.columns:
            labels = assignment[strata_col].astype(object)
            labels = labels.where(labels.notna(), MISSING)
            return labels.reindex(ids).fillna(MISSING).astype(object)
        allowed = list(stream.schema.segment_cols)
        if isinstance(assignment, pd.DataFrame):
            allowed += [c for c in assignment.columns if c not in allowed]
        if strata_col not in stream.schema.segment_cols:
            raise InvalidParameterError("strata_col", strata_col, allowed)
        labels = per_path_label(stream.df, strata_col, path_col)
        multi = labels.index[labels == MULTI]
        if len(multi):
            raise InvalidParameterError(
                "strata_col",
                f"'{strata_col}' changes within {len(multi)} path(s), "
                f"e.g. {list(multi[:3])}",
                ["a segment constant per path and fixed at assignment"],
            )
        return labels.reindex(ids).fillna(MISSING).astype(object)

    @staticmethod
    def _split_tables(counted, groups, levels, shares, alpha, overall, key, name):
        frames = [overall.assign(**{key: ALL})]
        rows = []
        labels = sorted(pd.unique(groups), key=str)
        for label in labels:
            part = counted[(groups == label).to_numpy()]
            table = _counts_table(part, levels, shares)
            frames.append(table.assign(**{key: label}))
            chi2, p, dof = _stats.srm_chi2(table["paths"].to_numpy(), shares)
            rows.append(
                {
                    key: label,
                    "paths": int(len(part)),
                    "chi2": chi2,
                    "dof": dof,
                    "p_value": p,
                }
            )
        split = pd.DataFrame(rows, columns=[key, "paths", "chi2", "dof", "p_value"])
        split["p_value_adjusted"] = _stats.holm(split["p_value"].to_numpy())
        split["mismatch"] = split["p_value_adjusted"] < alpha
        levels_df = pd.concat(frames, ignore_index=True)[[key] + LEVEL_COLUMNS]
        return {"levels": levels_df, name: split}
