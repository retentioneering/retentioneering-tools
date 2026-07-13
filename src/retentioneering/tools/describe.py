"""
Describe - tool for computing basic descriptive statistics of an eventstream.

Headless-only summary: dataset shape, schema, date range, event frequency,
and per-path-column length/duration statistics. Meant as a quick sanity
check, not an interactive drill-down (use `SegmentOverview` for that).
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Tuple

import pandas as pd

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream


@dataclass
class Describe:
    """Computes basic descriptive statistics for an eventstream."""

    eventstream: "Eventstream"

    def fit(
        self,
        percentiles: Tuple[float, ...] = (0.25, 0.5, 0.75, 0.9, 0.99),
        top_events: int | None = 20,
    ) -> Dict[str, Any]:
        """
        Compute basic descriptive statistics for the eventstream.

        Args:
            percentiles: Percentiles (0-1) to report in `path_stats`.
            top_events: Number of most frequent events to include in `event_frequency`,
                or `None` to include all events, unranked and unlimited.

        Returns:
            Dict with `schema`, `shape`, `date_range`, `event_frequency`,
            `path_stats`, and `segments` keys. `event_frequency` carries
            `attrs["truncated"]` (bool) and `attrs["n_total_events"]` (the
            full unique event count) so truncation is visible on the
            DataFrame itself, not just derivable via `shape.n_unique_events`.
        """
        es = self.eventstream
        schema = es.schema
        df = es.df

        event_counts = es.get_event_counts()
        total_events = int(sum(event_counts.values()))
        n_unique_events = len(event_counts)
        truncated = top_events is not None and n_unique_events > top_events
        event_frequency = (
            pd.DataFrame(
                {
                    "event": list(event_counts.keys()),
                    "count": list(event_counts.values()),
                }
            )
            .assign(share=lambda d: d["count"] / total_events if total_events else 0.0)
            .sort_values("count", ascending=False)
            .head(top_events)
            .reset_index(drop=True)
        )
        event_frequency.attrs["truncated"] = truncated
        event_frequency.attrs["n_total_events"] = n_unique_events

        # One DataFrame per path_cols entry (rows: count/mean/std/min/.../max,
        # columns: length/duration) - always keyed by path_col name, even when
        # there is only one, so the shape doesn't change with schema config.
        path_stats = {
            path_col: es.get_metrics(
                [{"metric": "length"}, {"metric": "duration"}], path_col=path_col
            ).describe(percentiles=list(percentiles))
            for path_col in schema.path_cols
        }

        segments = pd.DataFrame(
            [
                {"segment_col": col, "value": str(value), "count": int(count)}
                for col in schema.segment_cols
                for value, count in df[col].value_counts().items()
            ],
            columns=["segment_col", "value", "count"],
        )
        if not segments.empty:
            segments["share"] = segments["count"] / segments.groupby("segment_col")[
                "count"
            ].transform("sum")

        ts = df[schema.timestamp_col]

        return {
            "schema": {
                "event_col": schema.event_col,
                "path_col": schema.path_col,
                "path_cols": list(schema.path_cols),
                "segment_cols": list(schema.segment_cols),
                "timestamp_col": schema.timestamp_col,
            },
            "shape": {
                "n_events": len(df),
                "n_paths": int(df[schema.path_col].nunique()),
                "n_unique_events": len(event_counts),
            },
            "date_range": {
                "min": ts.min(),
                "max": ts.max(),
                "span": ts.max() - ts.min(),
            },
            "event_frequency": event_frequency,
            "path_stats": path_stats,
            "segments": segments,
        }
