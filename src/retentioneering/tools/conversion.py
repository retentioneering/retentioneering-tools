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
from retentioneering.utils.durations import parse_duration

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream

COLUMNS = [
    "start_event",
    "end_event",
    "paths_with_start",
    "converted",
    "conversion_rate",
    "base_rate",
    "lift",
]


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
        start_event,
        end_event,
        within=None,
        path_col: str | None = None,
    ) -> pd.DataFrame:
        """
        Conversion from `start_event` to `end_event`, one row per pair.

        See `Eventstream.get_conversion_rate` for the parameters and the
        returned columns.
        """
        es = self.eventstream
        schema = es.schema
        df = es.df

        path_col = path_col or schema.path_col
        if path_col not in schema.path_cols:
            raise InvalidParameterError("path_col", path_col, schema.path_cols)

        available = df[schema.event_col].unique().tolist()
        starts = self._parse_side(start_event, "start_event", available)
        ends = self._parse_side(end_event, "end_event", available)
        window = parse_within(within)

        total_paths = int(df[path_col].nunique())
        base_rates = [self._base_rate(spec, path_col, total_paths) for spec in ends]

        rows = []
        for start_spec in starts:
            start_pos = anchors.resolve_positions(
                df, schema, start_spec, side="start", path_col=path_col
            )
            paths_with_start = int(len(start_pos))
            # Step space, strictly after: an end anchor at the same position as
            # the start one is the start one (`start_event == end_event` asks
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
                        "start_event": start_spec.pattern,
                        "end_event": end_spec.pattern,
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
            side="end",
            path_col=path_col,
            not_before=floor,
            not_before_part=0,
        )
        if end_pos.empty:
            return 0
        if window is None:
            return int(len(end_pos))

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
            SELECT COUNT(*) AS converted
            FROM s JOIN e ON e.p = s.p
            WHERE {condition}
        """
        result = engine.run(
            query, df=self.eventstream.df, starts=start_pos, ends=end_pos
        )
        return int(result.iloc[0]["converted"])
