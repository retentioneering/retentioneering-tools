import json
from dataclasses import dataclass
from typing import Dict, List, Tuple

import duckdb
import pandas as pd

from retentioneering.data_processors.collapse_events import _session_agg_exprs
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema

PROCESSOR_NAME = "daily_states"


@dataclass
class DailyStates(DataProcessor):
    """
    Convert an eventstream into daily lifecycle-state events.

    Each path is expanded to one row per calendar day (from the first event up
    to ``max_dormant_days`` after the last event). Every row is labelled with
    one of six engagement states based on recent activity:

    Active days (``active_today`` is True):
        ``new``          — first-ever active day for this path
        ``current``      — was active within the past 7 days
        ``reactivated``  — was active 8–30 days ago, not in the last 7
        ``resurrected``  — was last active more than 30 days ago

    Inactive days (``active_today`` is False):
        ``at_risk_wau``  — was active within the past 7 days
        ``at_risk_mau``  — was active 8–30 days ago
        ``dormant``      — was last active more than 30 days ago

    Parameters
    ----------
    active_events : list of str, optional
        If provided, only these events count as "activity" when determining
        whether a day is active. If omitted, any event makes the day active.
    max_dormant_days : int, default 30
        How many days after the path's last event to continue generating state
        rows. Capped at the last day in the dataset.
    agg : dict, optional
        Extra column aggregation config passed to ``_session_agg_exprs``.
    path_id_col : str, optional
        Override the path ID column (default: ``schema.path_col``).
    event_col : str, optional
        Override the event column (default: ``schema.event_col``).

    Examples
    --------
    In Python:
        stream.daily_states()
        stream.daily_states(active_events=["purchase", "add_to_cart"], max_dormant_days=60)

    As a preprocessor in MCP update_base_stream:
        {"type": "daily_states"}
        {"type": "daily_states", "active_events": ["purchase"], "max_dormant_days": 60}
    """

    active_events: List[str]
    max_dormant_days: int
    agg: Dict[str, str]
    path_id_col: str | None
    event_col: str | None

    def __init__(
        self,
        active_events: List[str] | None = None,
        max_dormant_days: int = 30,
        agg: Dict[str, str] | None = None,
        path_id_col: str | None = None,
        event_col: str | None = None,
    ) -> None:
        self.active_events = active_events or []
        self.max_dormant_days = max_dormant_days
        self.agg = agg or {}
        self.path_id_col = path_id_col
        self.event_col = event_col
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_id_col = self.path_id_col or schema.path_col
        event_col   = self.event_col   or schema.event_col
        timestamp_col       = schema.timestamp
        event_type_col      = schema.event_type
        collapsed_event_type = EventTypes().COLLAPSED_EVENT.type
        max_dormant_days    = self.max_dormant_days

        if self.active_events:
            quoted = ", ".join("'" + e.replace("'", "''") + "'" for e in self.active_events)
            is_active_today = (
                f"(SUM(CASE WHEN {event_col} IN ({quoted}) THEN 1 ELSE 0 END) > 0)"
            )
        else:
            is_active_today = "TRUE"

        exclude = {path_id_col, schema.event_col, schema.event_type}
        agg_exprs = _session_agg_exprs(df, self.agg, exclude, timestamp_col)
        agg_chunk = (", " + ", ".join(agg_exprs)) if agg_exprs else ""

        other_path_cols = [c for c in schema.path_cols if c != path_id_col]
        other_path_cols_chunk = ", ".join([f"NULL AS {c}" for c in other_path_cols])
        if other_path_cols_chunk:
            other_path_cols_chunk = f", {other_path_cols_chunk}"

        special_cols = schema.segment_cols + schema.custom_cols
        special_cols_chunk = ", ".join([
            f"COALESCE({c}, LAST_VALUE({c} IGNORE NULLS) OVER "
            f"(PARTITION BY {path_id_col} ORDER BY {timestamp_col} "
            f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS {c}"
            for c in special_cols
        ])

        exclude_cols = (
            ["active_today", "last_active", "had_week", "had_month",
             schema.index, schema.subindex]
            + other_path_cols
            + special_cols
        )

        query = f"""
        WITH base AS (
            SELECT *, CAST({timestamp_col} AS DATE) AS day
            FROM df
        ),
        dataset_end AS (
            SELECT MAX(day) AS dataset_end FROM base
        ),
        per_date AS (
            SELECT
                {path_id_col},
                day,
                {is_active_today} AS active_today
                {agg_chunk}
            FROM base
            GROUP BY {path_id_col}, day
        ),
        path_bounds AS (
            SELECT
                b.{path_id_col},
                MIN(b.day) AS start_day,
                LEAST(MAX(b.day) + INTERVAL {max_dormant_days} DAY, ANY_VALUE(de.dataset_end)) AS end_day
            FROM base b
            CROSS JOIN dataset_end de
            GROUP BY b.{path_id_col}
        ),
        cal AS (
            SELECT pb.{path_id_col}, day
            FROM path_bounds AS pb,
            LATERAL (SELECT * FROM range(start_day, end_day + INTERVAL 1 DAY, INTERVAL 1 DAY)) AS r(day)
        ),
        states AS (
            SELECT
                cal.{path_id_col},
                cal.day AS {timestamp_col},
                p.* EXCLUDE ({path_id_col}, day, {timestamp_col}, active_today),
                COALESCE(p.active_today, FALSE) AS active_today,
                (SELECT MAX(d2.day) FROM per_date d2
                 WHERE d2.{path_id_col} = cal.{path_id_col}
                   AND d2.day < cal.day AND d2.active_today) AS last_active,
                EXISTS(SELECT 1 FROM per_date d3
                       WHERE d3.{path_id_col} = cal.{path_id_col}
                         AND d3.day < cal.day
                         AND d3.day >= cal.day - INTERVAL 7 DAY
                         AND d3.active_today) AS had_week,
                EXISTS(SELECT 1 FROM per_date d4
                       WHERE d4.{path_id_col} = cal.{path_id_col}
                         AND d4.day < cal.day
                         AND d4.day >= cal.day - INTERVAL 30 DAY
                         AND d4.active_today) AS had_month
            FROM cal
            LEFT JOIN per_date p USING ({path_id_col}, day)
        )
        SELECT * EXCLUDE ({json.dumps(exclude_cols)[1:-1]})
            {other_path_cols_chunk},
            CASE
                WHEN active_today THEN
                    CASE
                        WHEN last_active IS NULL THEN 'new'
                        WHEN had_week              THEN 'current'
                        WHEN had_month             THEN 'reactivated'
                        ELSE                            'resurrected'
                    END
                ELSE
                    CASE
                        WHEN had_week  THEN 'at_risk_wau'
                        WHEN had_month THEN 'at_risk_mau'
                        ELSE                'dormant'
                    END
            END AS {event_col},
            '{collapsed_event_type}' AS {event_type_col},
            1 AS {schema.subindex},
            ROW_NUMBER() OVER (
                PARTITION BY {path_id_col}
                ORDER BY {timestamp_col}, {schema.subindex}
            ) AS {schema.index}
            {(', ' + special_cols_chunk) if special_cols_chunk else ''}
        FROM states
        ORDER BY {path_id_col}, {timestamp_col}, {schema.subindex}
        """
        res = duckdb.query(query).df()

        for col in schema.event_cols + schema.segment_cols:
            if col in res.columns:
                res[col] = res[col].astype("category")
                res[col] = res[col].cat.remove_unused_categories()
                res[col] = res[col].cat.as_unordered()

        return res, schema
