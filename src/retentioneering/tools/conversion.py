"""
Conversion rate — tool for "if Y happened, how often does X follow?".

Headless-only: one row per (start anchor, end anchor) pair, carrying the
denominator and the base rate alongside the rate itself, because a conversion
rate on its own is not a statement anyone can act on. 0.5 out of two paths and
0.5 out of five thousand are different claims, and a rate that looks high only
because the event is common everywhere is not a finding — hence
`paths_with_start` and `base_rate`/`lift` ship in every row rather than behind
a flag.

The unit of observation is the **path**, not the occurrence: a path where Y
happened three times contributes one row to the denominator, not three. Per-
occurrence questions ("of 22,986 visits to this page, how many were entrances")
are a different shape of answer and are not expressible here — use a transition
matrix on a subpopulation for those.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence

import pandas as pd

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.exceptions import InvalidParameterError
from retentioneering.paths import anchors
from retentioneering.tools import _stats
from retentioneering.utils.durations import parse_duration

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream

COLUMNS = [
    "start_anchor",
    "end_anchor",
    "paths_with_start",
    "converted",
    "conversion_rate",
    "base_rate",
    "lift",
]

CI_COLUMNS = ["conversion_rate_ci_low", "conversion_rate_ci_high"]

#: Per-group columns of the diff mode, each prefixed with ``group1_``/``group2_``.
GROUP_COLUMNS = [
    "paths_with_start",
    "converted",
    "conversion_rate",
    "conversion_rate_ci_low",
    "conversion_rate_ci_high",
    "base_rate",
    "lift",
]

DIFF_COLUMNS = (
    ["start_anchor", "end_anchor"]
    + [f"group1_{c}" for c in GROUP_COLUMNS]
    + [f"group2_{c}" for c in GROUP_COLUMNS]
    + [
        "delta",
        "delta_ci_low",
        "delta_ci_high",
        "p_value",
        "conditional",
        "excluded_paths",
    ]
)

#: Interval level used in diff mode when `ci_level` is not given.
DEFAULT_CI_LEVEL = 0.95


def _check_ci_level(ci_level) -> float | None:
    """``None`` or a confidence level in [0.5, 1): a lower level would call an
    interval narrower than the point's own coin flip a confidence interval."""
    if ci_level is None:
        return None
    allowed = ["a float in [0.5, 1), e.g. 0.95"]
    if isinstance(ci_level, bool) or not isinstance(ci_level, (int, float)):
        raise InvalidParameterError("ci_level", ci_level, allowed)
    if not 0.5 <= ci_level < 1:
        raise InvalidParameterError("ci_level", ci_level, allowed)
    return float(ci_level)


def _is_path_start(spec) -> bool:
    """An anchor that every path reaches: the path start, with no offset."""
    return spec.pattern == "path_start" and not spec.offset


def parse_within(within) -> tuple[str, float] | None:
    """Normalize `within` into ``("steps", n)`` / ``("seconds", n)`` / None.

    Same dual convention as an anchor spec's `offset`: a bare int counts
    events, a string with a unit (or a `Timedelta`) counts time.
    """
    if within is None:
        return None
    allowed = ["an event count (int) or a duration ('30m', pd.Timedelta)"]
    if isinstance(within, bool):
        raise InvalidParameterError("within", within, allowed)
    if isinstance(within, int):
        if within < 1:
            raise InvalidParameterError(
                "within",
                within,
                ["a positive event count — a window of 0 events holds nothing"],
            )
        return ("steps", within)
    return ("seconds", parse_duration(within, param="within"))


@dataclass
class ConversionRate:
    """Computes per-path conversion from one anchor to another."""

    eventstream: "Eventstream"

    def fit(
        self,
        start_anchor,
        end_anchor,
        within=None,
        path_col: str | None = None,
        ci_level: float | None = None,
        diff=None,
    ) -> pd.DataFrame:
        """
        Conversion from `start_anchor` to `end_anchor`, one row per pair.

        See `Eventstream.get_conversion_rate` for the parameters and the
        returned columns.
        """
        ci_level = _check_ci_level(ci_level)
        if diff is not None:
            return self._fit_diff(
                start_anchor, end_anchor, within, path_col, ci_level, diff
            )
        result = self._fit_single(start_anchor, end_anchor, within, path_col)
        if ci_level is not None:
            low, high = _stats.wilson_ci(
                result["converted"].to_numpy(),
                result["paths_with_start"].to_numpy(),
                ci_level,
            )
            position = COLUMNS.index("conversion_rate") + 1
            result.insert(position, "conversion_rate_ci_low", low)
            result.insert(position + 1, "conversion_rate_ci_high", high)
        return result

    def _fit_diff(self, start_anchor, end_anchor, within, path_col, ci_level, diff):
        """Path-level comparison of two groups: per-group rates and the delta."""
        from retentioneering.tools._diff import resolve_diff

        es = self.eventstream
        path_col = path_col or es.schema.path_col
        if path_col not in es.schema.path_cols:
            raise InvalidParameterError("path_col", path_col, es.schema.path_cols)
        ci_level = ci_level if ci_level is not None else DEFAULT_CI_LEVEL
        # Anchors are validated against the whole stream, so a typo is reported
        # the same way as without diff, and an event absent from one group only
        # counts as zero conversions there.
        available = es.df[es.schema.event_col].unique().tolist()

        groups = resolve_diff(es, diff, path_col, require_constant=True)
        frames = []
        for ids in (groups.ids1, groups.ids2):
            sub = es.filter_events(keep={path_col: list(ids)})
            frame = ConversionRate(sub)._fit_single(
                start_anchor, end_anchor, within, path_col, available=available
            )
            low, high = _stats.wilson_ci(
                frame["converted"].to_numpy(),
                frame["paths_with_start"].to_numpy(),
                ci_level,
            )
            frame["conversion_rate_ci_low"] = low
            frame["conversion_rate_ci_high"] = high
            frames.append(frame)
        g1, g2 = frames
        out = g1[["start_anchor", "end_anchor"]].copy()
        for prefix, frame in (("group1_", g1), ("group2_", g2)):
            for col in GROUP_COLUMNS:
                out[prefix + col] = frame[col].to_numpy()
        k1, n1 = g1["converted"].to_numpy(), g1["paths_with_start"].to_numpy()
        k2, n2 = g2["converted"].to_numpy(), g2["paths_with_start"].to_numpy()
        out["delta"] = out["group1_conversion_rate"] - out["group2_conversion_rate"]
        low, high = _stats.newcombe_ci(k1, n1, k2, n2, ci_level)
        out["delta_ci_low"] = low
        out["delta_ci_high"] = high
        out["p_value"] = _stats.two_proportion_z(k1, n1, k2, n2)[1]
        # the denominator is the paths that reached the start anchor: unless that is
        # the path's start, the groups are selected after exposure (the funnel's
        # step_conditional); the p-value is kept, flagged
        starts = anchors.parse_specs(start_anchor, param="start_anchor")
        n_ends = len(anchors.parse_specs(end_anchor, param="end_anchor"))
        out["conditional"] = [
            not _is_path_start(spec) for spec in starts for _ in range(n_ends)
        ]
        out["excluded_paths"] = groups.excluded_paths
        return out[DIFF_COLUMNS].reset_index(drop=True)

    def _fit_single(
        self, start_anchor, end_anchor, within, path_col, available=None
    ) -> pd.DataFrame:
        """One row per pair over this eventstream's paths.

        `available` overrides the event names the anchors are validated against:
        a diff group is checked against the whole stream, so an event that only
        one group lacks is a zero, not a typo.
        """
        es = self.eventstream
        schema = es.schema
        df = es.df

        path_col = path_col or schema.path_col
        if path_col not in schema.path_cols:
            raise InvalidParameterError("path_col", path_col, schema.path_cols)

        if available is None:
            available = df[schema.event_col].unique().tolist()
        starts = self._parse_side(start_anchor, "start_anchor", available)
        ends = self._parse_side(end_anchor, "end_anchor", available)
        window = parse_within(within)

        total_paths = int(df[path_col].nunique())
        base_rates = [self._base_rate(spec, path_col, total_paths) for spec in ends]

        rows = []
        for start_spec in starts:
            start_pos = anchors.resolve_positions(
                df, schema, start_spec, offset_side="start", path_col=path_col
            )
            # One row per *position* comes back, not per path: `resolve_positions`
            # documents that "with occurrence="all" a path appears once per
            # position its anchor can occupy". The unit of observation here is
            # the path, so collapse to distinct paths before counting.
            paths_with_start = int(start_pos[path_col].nunique())
            # Step space, strictly after: an end anchor at the same position as
            # the start one is the start one (`start_anchor == end_anchor` asks
            # about a *repeat*), and only steps put a `path_end` sentinel after
            # the last real event rather than on it.
            floor = start_pos[[path_col, "step"]].rename(columns={"step": "bound_step"})
            for end_spec, base_rate in zip(ends, base_rates):
                converted = (
                    self._converted(start_pos, floor, end_spec, window, path_col)
                    if paths_with_start
                    else 0
                )
                # No start anchor means no question was asked of these paths,
                # which is data, not an error: the rate is undefined, not zero.
                rate = (
                    converted / paths_with_start if paths_with_start else float("nan")
                )
                rows.append(
                    {
                        "start_anchor": start_spec.pattern,
                        "end_anchor": end_spec.pattern,
                        "paths_with_start": paths_with_start,
                        "converted": converted,
                        "conversion_rate": rate,
                        "base_rate": base_rate,
                        "lift": rate / base_rate if base_rate else float("nan"),
                    }
                )

        result = pd.DataFrame(rows, columns=COLUMNS)
        result["paths_with_start"] = result["paths_with_start"].astype("int64")
        result["converted"] = result["converted"].astype("int64")
        return result

    @staticmethod
    def _parse_side(value, param: str, available: Sequence[str]) -> list:
        """Anchor specs for one side, with their event names validated.

        Unlike `truncate_paths`, a list here is a fan-out — several separate
        questions — so every spec in it has to resolve on its own, and a name
        that exists nowhere in the eventstream is a typo rather than a link in
        a fallback chain.
        """
        specs = anchors.parse_specs(value, param=param)
        for spec in specs:
            anchors.validate_pattern_tokens(
                spec.pattern, available, param=param, stacklevel=6
            )
        return specs

    def _base_rate(self, spec, path_col: str, total_paths: int) -> float:
        """Share of *all* paths the end pattern occurs in, anywhere.

        The comparison a raw conversion rate is missing: `lift` is the only
        column not derivable from the others, because it needs this number,
        which is about the whole eventstream rather than about the paths that
        reached the start anchor.
        """
        if not total_paths:
            return float("nan")
        match = anchors.resolve_anchors(
            self.eventstream.df,
            self.eventstream.schema,
            spec.pattern,
            occurrence=spec.occurrence,
            path_col=path_col,
        )
        return len(match.paths()) / total_paths

    def _converted(
        self,
        start_pos: pd.DataFrame,
        floor: pd.DataFrame,
        end_spec,
        window: tuple[str, float] | None,
        path_col: str,
    ) -> int:
        """Paths whose end anchor lands after the start one and inside `within`.

        `not_before_part=0` puts the *whole* end pattern after the start anchor,
        not just the token it anchors on: "what happened after Y" is a question
        about what follows Y, so a `"cart->.*->purchase"` target whose cart
        precedes Y has not happened after Y.
        """
        end_pos = anchors.resolve_positions(
            self.eventstream.df,
            self.eventstream.schema,
            end_spec,
            offset_side="end",
            path_col=path_col,
            not_before=floor,
            not_before_part=0,
        )
        if end_pos.empty:
            return 0
        if window is None:
            return int(end_pos[path_col].nunique())

        schema = self.eventstream.schema
        path_q = engine.quote_ident(path_col)
        index_q = engine.quote_ident(schema.index)
        subindex_q = engine.quote_ident(schema.subindex)
        ts_q = engine.quote_ident(schema.timestamp_col)

        unit, size = window
        if unit == "steps":
            condition = f"e.step <= s.step + {int(size)}"
        else:
            condition = f"e.ts <= s.ts + {dialect.interval_seconds(size)}"

        # Steps come from the anchors themselves (a boundary sentinel has a step
        # but no row of its own); timestamps come from the row each anchor's
        # index points at, which for a virtual `path_end` is the path's last
        # real event — the only timestamp the end of a path can be said to have.
        query = f"""
            WITH base AS (
                SELECT {path_q} AS p, {index_q} AS idx, {ts_q} AS ts,
                       row_number() OVER (
                           PARTITION BY {path_q} ORDER BY {index_q}, {subindex_q}
                       ) AS step
                FROM df
            ),
            s AS (
                SELECT a.{path_q} AS p, a.step AS step, b.ts AS ts
                FROM starts a JOIN base b ON b.p = a.{path_q} AND b.idx = a.bound
            ),
            e AS (
                SELECT x.{path_q} AS p, x.step AS step, b.ts AS ts
                FROM ends x JOIN base b ON b.p = x.{path_q} AND b.idx = x.bound
            )
            SELECT COUNT(DISTINCT s.p) AS converted
            FROM s JOIN e ON e.p = s.p
            WHERE e.step > s.step AND {condition}
        """
        result = engine.run(
            query, df=self.eventstream.df, starts=start_pos, ends=end_pos
        )
        return int(result.iloc[0]["converted"])
