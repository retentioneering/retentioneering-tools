import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.data_processors.collapse_events import _session_agg_exprs
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.utils.timezones import column_timezone

PROCESSOR_NAME = "to_daily_states"


@dataclass
class ToDailyStates(DataProcessor):
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
    path_col : str, optional
        Daily-state grain (default: ``schema.path_col``). Keep this ID and all
        coarser path IDs; remove finer path columns from the frame and schema.
        New positions are numbered within the retained primary path by day,
        then by the selected path's first source position for same-day ties.

    Examples
    --------
    In Python:
        stream.to_daily_states()
        stream.to_daily_states(active_events=["purchase", "add_to_cart"], max_dormant_days=60)

    As a preprocessor in MCP update_base_stream:
        {"type": "to_daily_states"}
        {"type": "to_daily_states", "active_events": ["purchase"], "max_dormant_days": 60}
    """

    active_events: List[str]
    max_dormant_days: int
    agg: Dict[str, str]
    path_col: str | None

    def __init__(
        self,
        active_events: List[str] | None = None,
        max_dormant_days: int = 30,
        agg: Dict[str, str] | None = None,
        path_col: str | None = None,
    ) -> None:
        self.active_events = active_events or []
        self.max_dormant_days = max_dormant_days
        self.agg = agg or {}
        self.path_col = path_col
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_col = schema.resolve_path_col(self.path_col)
        # ADR-0004 guarantees a coarsest-first nesting hierarchy. The selected
        # path identifies its ancestors on every generated day; descendants do
        # not identify a daily state and must leave both the frame and schema.
        # Use the declared hierarchy, not sample cardinality (also for empties).
        path_level = schema.path_cols.index(path_col)
        result_schema = schema.copy()
        result_schema.path_cols = schema.path_cols[: path_level + 1]
        ancestor_cols = result_schema.path_cols[:-1]
        event_col = schema.event_col
        timestamp_col = schema.timestamp_col
        event_type_col = schema.event_type
        collapsed_event_type = EventTypes().COLLAPSED_EVENT.type
        max_dormant_days = self.max_dormant_days

        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        timestamp_col_q = engine.quote_ident(timestamp_col)
        event_type_col_q = engine.quote_ident(event_type_col)
        index_col_q = engine.quote_ident(schema.index)
        subindex_col_q = engine.quote_ident(schema.subindex)
        primary_col_q = engine.quote_ident(result_schema.path_col)
        order = dialect.event_order(index_col_q, subindex_col_q)
        day_expr = dialect.calendar_date(
            timestamp_col_q, column_timezone(df[timestamp_col])
        )

        if self.active_events:
            quoted = ", ".join(
                "'" + e.replace("'", "''") + "'" for e in self.active_events
            )
            is_active_today = (
                f"(SUM(CASE WHEN {event_col_q} IN ({quoted}) THEN 1 ELSE 0 END) > 0)"
            )
        else:
            is_active_today = "TRUE"

        exclude = {*schema.path_cols, schema.event_col, schema.event_type}
        agg_exprs = _session_agg_exprs(
            df,
            self.agg,
            exclude,
            schema.index,
            schema.subindex,
            processor=PROCESSOR_NAME,
        )
        agg_chunk = (", " + ", ".join(agg_exprs)) if agg_exprs else ""

        # First source position breaks ties between calendars on the same day;
        # path labels (e.g. session IDs) are not evidence of event order.
        path_order_col = "_daily_path_order"
        occupied_names = {c.lower() for c in df.columns}
        while path_order_col.lower() in occupied_names:
            path_order_col += "_"
        path_order_q = engine.quote_ident(path_order_col)
        ancestor_agg = "".join(
            f", ANY_VALUE(b.{engine.quote_ident(c)}) AS {engine.quote_ident(c)}"
            for c in ancestor_cols
        )
        ancestor_select = "".join(
            f", pb.{engine.quote_ident(c)}" for c in ancestor_cols
        )

        special_cols = schema.segment_cols + schema.custom_cols
        special_cols_chunk = ", ".join(
            [
                f"COALESCE({engine.quote_ident(c)}, LAST_VALUE({engine.quote_ident(c)} IGNORE NULLS) OVER "
                f"(PARTITION BY {path_col_q} ORDER BY {timestamp_col_q} "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS {engine.quote_ident(c)}"
                for c in special_cols
            ]
        )

        exclude_cols = [
            "active_today",
            "last_active",
            "had_week",
            "had_month",
            schema.index,
            schema.subindex,
            path_order_col,
        ] + special_cols
        exclude_cols_sql = ", ".join(engine.quote_ident(c) for c in exclude_cols)

        query = f"""
        WITH base AS (
            SELECT *, {day_expr} AS day
            FROM df
        ),
        dataset_end AS (
            SELECT MAX(day) AS dataset_end FROM base
        ),
        per_date AS (
            SELECT
                {path_col_q},
                day,
                {is_active_today} AS active_today
                {agg_chunk}
            FROM base
            GROUP BY {path_col_q}, day
        ),
        path_bounds AS (
            SELECT
                b.{path_col_q},
                MIN(b._rn) AS {path_order_q}
                {ancestor_agg},
                MIN(b.day) AS start_day,
                LEAST(MAX(b.day) + INTERVAL {max_dormant_days} DAY, ANY_VALUE(de.dataset_end)) AS end_day
            FROM base b
            CROSS JOIN dataset_end de
            GROUP BY b.{path_col_q}
        ),
        cal AS (
            SELECT pb.{path_col_q}, pb.{path_order_q}{ancestor_select}, day
            FROM path_bounds AS pb,
            LATERAL (SELECT * FROM range(start_day, end_day + INTERVAL 1 DAY, INTERVAL 1 DAY)) AS r(day)
        ),
        states AS (
            SELECT
                cal.* EXCLUDE (day),
                cal.day AS {timestamp_col_q},
                p.* EXCLUDE ({path_col_q}, day, {timestamp_col_q}, active_today),
                COALESCE(p.active_today, FALSE) AS active_today,
                (SELECT MAX(d2.day) FROM per_date d2
                 WHERE d2.{path_col_q} = cal.{path_col_q}
                   AND d2.day < cal.day AND d2.active_today) AS last_active,
                EXISTS(SELECT 1 FROM per_date d3
                       WHERE d3.{path_col_q} = cal.{path_col_q}
                         AND d3.day < cal.day
                         AND d3.day >= cal.day - INTERVAL 7 DAY
                         AND d3.active_today) AS had_week,
                EXISTS(SELECT 1 FROM per_date d4
                       WHERE d4.{path_col_q} = cal.{path_col_q}
                         AND d4.day < cal.day
                         AND d4.day >= cal.day - INTERVAL 30 DAY
                         AND d4.active_today) AS had_month
            FROM cal
            LEFT JOIN per_date p USING ({path_col_q}, day)
        )
        SELECT * EXCLUDE ({exclude_cols_sql}),
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
            END AS {event_col_q},
            '{collapsed_event_type}' AS {event_type_col_q},
            1 AS {subindex_col_q},
            -- New daily-state positions, unique at the retained primary grain.
            -- A day can contain several selected paths (e.g. sessions/user).
            ROW_NUMBER() OVER (
                PARTITION BY {primary_col_q}
                ORDER BY {timestamp_col_q}, {path_order_q}
            ) AS {index_col_q}
            {(", " + special_cols_chunk) if special_cols_chunk else ""}
        FROM states
        ORDER BY {primary_col_q}, {order}
        """
        # "first"/"last" aggregates read the event order off an integer row
        # number: within a path, physical order is the event order (ADR-0017),
        # and an int key is far cheaper for ARG_MIN/ARG_MAX than the
        # (index, subindex) row value.
        res = engine.run(query, df=df.assign(_rn=np.arange(len(df))))

        for col in [schema.event_col] + schema.segment_cols:
            if col in res.columns:
                res[col] = res[col].astype("category")
                res[col] = res[col].cat.remove_unused_categories()
                res[col] = res[col].cat.as_unordered()

        return res, result_schema
