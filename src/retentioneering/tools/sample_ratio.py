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
    MISSING,
    MULTI,
    coerce_level,
    normalize_assignment,
    per_path_label,
)
from retentioneering.tools.ab_test import prepare_assignment

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream

__all__ = ["SampleRatio", "BY_DAY", "ALL"]

#: ``by`` value that splits the check by the day of assignment.
BY_DAY = "day"
#: ``by`` label of the overall rows in ``levels``.
ALL = "<ALL>"

LEVEL_COLUMNS = ["level", "paths", "share", "expected_share", "expected_paths"]


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
        segment_col: str,
        expected_shares: dict[str, float] | None = None,
        alpha: float = 0.001,
        path_col: str | None = None,
        assignment: pd.Series | pd.DataFrame | None = None,
        by: str | None = None,
    ) -> dict[str, Any]:
        """Compare arm sizes against expected shares.

        Parameters
        ----------
        segment_col : str
            Segment column holding the arm (must be in ``schema.segment_cols``; with
            ``assignment`` it may be absent from the stream and only labels the check).
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
        by : str, optional
            Repeat the check within groups: a segment column that is constant per path
            (a column of the assignment DataFrame is used first), or ``"day"`` — the
            calendar day of ``assigned_at`` (or of the path's first event without an
            assignment table). Valid only for a variable fixed at assignment time:
            per-group shares on a variable the treatment can change are an effect, not
            a bug check.

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
        in_stream = segment_col in schema.segment_cols
        if assignment is None and not in_stream:
            raise InvalidParameterError(
                "segment_col", segment_col, list(schema.segment_cols)
            )

        stream_labels = (
            per_path_label(stream.df, segment_col, path_col) if in_stream else None
        )
        silent = 0
        multi = 0
        if assignment is None:
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
                # `by` reads the caller's columns: keep them on the matched ids
                assignment = assignment.set_axis(table.index, axis=0)
            stream_paths = pd.Index(pd.unique(stream.df[path_col]))
            silent = int((~table.index.isin(stream_paths)).sum())
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
        unassigned = int(len(arms) - len(counted)) + n_missing

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
        if by is not None:
            groups = self._by_groups(by, path_col, table, counted.index, assignment)
            result.update(
                self._by_tables(counted, groups, levels, shares, alpha, overall)
            )
            result["by"] = by
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

    def _by_groups(self, by, path_col, table, ids, assignment) -> pd.Series:
        stream = self.stream
        if by == BY_DAY:
            if table is not None and "assigned_at" in table.columns:
                day = table["assigned_at"].dt.floor("D")
            else:
                ts = stream.schema.timestamp_col
                first = stream.df.groupby(path_col, observed=True)[ts].min()
                day = first.dt.floor("D")
                if table is not None:
                    day = day.reindex(table.index)
            labels = day.dt.strftime("%Y-%m-%d").where(day.notna(), MISSING)
            return labels.reindex(ids).fillna(MISSING).astype(object)
        if isinstance(assignment, pd.DataFrame) and by in assignment.columns:
            labels = assignment[by].astype(object)
            labels = labels.where(labels.notna(), MISSING)
            return labels.reindex(ids).fillna(MISSING).astype(object)
        allowed = [BY_DAY] + list(stream.schema.segment_cols)
        if by not in stream.schema.segment_cols:
            raise InvalidParameterError("by", by, allowed)
        labels = per_path_label(stream.df, by, path_col)
        multi = labels.index[labels == MULTI]
        if len(multi):
            raise InvalidParameterError(
                "by",
                f"'{by}' changes within {len(multi)} path(s), e.g. {list(multi[:3])}",
                ["a segment constant per path and fixed at assignment", BY_DAY],
            )
        return labels.reindex(ids).fillna(MISSING).astype(object)

    @staticmethod
    def _by_tables(counted, groups, levels, shares, alpha, overall):
        frames = [overall.assign(by=ALL)]
        rows = []
        keys = sorted(pd.unique(groups), key=str)
        for key in keys:
            part = counted[(groups == key).to_numpy()]
            table = _counts_table(part, levels, shares)
            frames.append(table.assign(by=key))
            chi2, p, dof = _stats.srm_chi2(table["paths"].to_numpy(), shares)
            rows.append(
                {
                    "by": key,
                    "paths": int(len(part)),
                    "chi2": chi2,
                    "dof": dof,
                    "p_value": p,
                }
            )
        by_levels = pd.DataFrame(
            rows, columns=["by", "paths", "chi2", "dof", "p_value"]
        )
        by_levels["p_value_adjusted"] = _stats.holm(by_levels["p_value"].to_numpy())
        by_levels["mismatch"] = by_levels["p_value_adjusted"] < alpha
        levels_df = pd.concat(frames, ignore_index=True)[["by"] + LEVEL_COLUMNS]
        return {"levels": levels_df, "by_levels": by_levels}
