from typing import List, Tuple

import pandas as pd

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import (
    InvalidParameterError,
    PatternSyntaxError,
    PreprocessingConfigError,
)
from retentioneering.paths import anchors
from retentioneering.utils.sequences import find_delimiter_collisions
from retentioneering.utils.sql_quoting import quote_list

PROCESSOR_NAME = "add_events"


class AddEvents(DataProcessor):
    def __init__(
        self,
        name: str,
        source_events: List[str] | None = None,
        sql: str | None = None,
        anchor: str | dict | None = None,
        path_col: str | None = None,
        churn: dict | None = None,
    ) -> None:
        if not isinstance(name, str) or not name:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Argument 'name' must be a non-empty string."
            )
        if find_delimiter_collisions([name]):
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"Argument 'name' ({name!r}) contains '->', which retentioneering "
                f"uses as the path delimiter in matches_pattern/step_matrix pattern "
                f"matching. Choose a different name.",
            )

        n_modes = sum(
            [
                source_events is not None,
                sql is not None,
                churn is not None,
                anchor is not None,
            ]
        )
        if n_modes != 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "Exactly one of 'source_events', 'sql', 'churn', or 'anchor' "
                "must be provided.",
            )

        if source_events is not None:
            if not isinstance(source_events, list):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Argument 'source_events' must be a list."
                )
            if not all(isinstance(e, str) for e in source_events):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "All elements in 'source_events' must be strings."
                )

        if sql is not None and not isinstance(sql, str):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Argument 'sql' must be a string."
            )

        if churn is not None:
            if not isinstance(churn, dict):
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Argument 'churn' must be a dictionary."
                )
            if "inactivity_days" not in churn:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "Argument 'churn' must contain 'inactivity_days'."
                )
            inactivity_days = churn["inactivity_days"]
            if not isinstance(inactivity_days, (int, float)) or inactivity_days <= 0:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    "Value 'churn.inactivity_days' must be a positive number.",
                )
            active_events = churn.get("active_events")
            if active_events is not None:
                if not isinstance(active_events, list):
                    raise PreprocessingConfigError(
                        PROCESSOR_NAME, "Value 'churn.active_events' must be a list."
                    )
                if not all(isinstance(e, str) for e in active_events):
                    raise PreprocessingConfigError(
                        PROCESSOR_NAME,
                        "All elements in 'churn.active_events' must be strings.",
                    )

        self.anchor_spec = self._parse_anchor(anchor) if anchor is not None else None

        self.name = name
        self.source_events = source_events
        self.sql = sql
        self.churn = churn
        self.path_col = path_col
        super().__init__()

    @staticmethod
    def _parse_anchor(value) -> anchors.AnchorSpec:
        # One anchor, not a list: `truncate_paths` reads a list as a fallback
        # chain narrowing one window, and there is no window here to narrow.
        # Two markers means two calls, which also keeps them independently named.
        if isinstance(value, (list, tuple)):
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "Argument 'anchor' takes a single event name or anchor spec, "
                "not a list. Call add_events once per anchor.",
            )
        try:
            return anchors.parse_spec(value, param="anchor")
        except (InvalidParameterError, PatternSyntaxError) as exc:
            raise PreprocessingConfigError(PROCESSOR_NAME, exc.message) from exc

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        if self.source_events is not None:
            df_source = self._get_by_source_events(df, schema)
        elif self.sql is not None:
            df_source = self._get_by_sql(df, schema)
        elif self.anchor_spec is not None:
            df_source = self._get_by_anchor(df, schema)
        else:
            df_source = self._get_by_churn(df, schema)

        if df_source.empty:
            return df, schema

        event_types = EventTypes()
        # Churn closes a stretch of inactivity, so it sorts after the event that
        # started it; every other marker opens something and sorts before.
        marker = (
            event_types.CHURN_EVENT
            if self.churn is not None
            else event_types.SYNTHETIC_EVENT
        )
        df_new = df_source.copy()
        df_new[schema.event_col] = self.name
        df_new[schema.event_type] = marker.type
        df_new[schema.subindex] = marker.index

        df = (
            pd.concat([df, df_new])
            .sort_values([schema.path_col, schema.timestamp_col, schema.subindex])
            .reset_index(drop=True)
        )

        df[schema.event_col] = df[schema.event_col].astype("category")

        return df, schema

    def _get_by_source_events(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> pd.DataFrame:
        if not self.source_events:
            return df.iloc[0:0]

        existing = set(df[schema.event_col].cat.categories.tolist())
        unknown = set(self.source_events) - existing
        if unknown:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"Unknown event names in 'source_events': {sorted(unknown)}. "
                f"Available events: {sorted(existing)}.",
            )

        return df[df[schema.event_col].isin(self.source_events)].copy()

    def _get_by_anchor(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> pd.DataFrame:
        path_col = self.path_col or schema.path_col
        if path_col not in schema.path_cols:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"path_col '{path_col}' must be one of schema.path_cols: "
                f"{schema.path_cols}.",
            )

        # Unlike `truncate_paths`, which takes a list of anchors of which some
        # are *expected* not to resolve, a single anchor that names nothing can
        # only be a typo — so literals are checked too.
        available = df[schema.event_col].unique().tolist()
        try:
            anchors.validate_pattern_tokens(
                self.anchor_spec.pattern, available, param="anchor"
            )
            positions = anchors.resolve_positions(
                df, schema, self.anchor_spec, path_col=path_col
            )
        except (InvalidParameterError, PatternSyntaxError) as exc:
            raise PreprocessingConfigError(PROCESSOR_NAME, exc.message) from exc

        if positions.empty:
            return df.iloc[0:0]

        keys = positions[[path_col, "bound"]].drop_duplicates()
        keys.columns = [path_col, schema.index]
        matched = df.merge(keys, on=[path_col, schema.index], how="inner")
        # `schema.index` is a position within the path, and synthetic rows carry
        # the index of the row they were derived from — so a path that already
        # has markers can hold several rows at the anchor's index. They share
        # the timestamp and the path's own columns, which is all the new row
        # copies, so any one of them serves; take the first in path order.
        return matched.drop_duplicates(subset=[path_col, schema.index], keep="first")

    def _get_by_sql(self, df: pd.DataFrame, schema: EventstreamSchema) -> pd.DataFrame:
        columns_old = set(df.columns)
        result = engine.run(self.sql, eventstream=df)
        if set(result.columns) != columns_old:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "The SQL query must return the same columns as the eventstream.",
            )
        return result

    def _get_by_churn(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> pd.DataFrame:
        path_col = schema.path_col
        ts_col = schema.timestamp_col
        subindex_col = schema.subindex
        event_col = schema.event_col
        path_col_q = engine.quote_ident(path_col)
        ts_col_q = engine.quote_ident(ts_col)
        subindex_col_q = engine.quote_ident(subindex_col)
        event_col_q = engine.quote_ident(event_col)

        inactivity_days = self.churn["inactivity_days"]
        active_events = self.churn.get("active_events")

        threshold_seconds = inactivity_days * 86400

        # Filter to active events only if specified; otherwise all events count.
        # LEAD looks only within the filtered set, so the "next active event"
        # is found correctly. The overall dataset max comes from the full df.
        active_filter = ""
        if active_events is not None:
            if not active_events:
                return df.iloc[0:0]
            quoted = quote_list(active_events)
            active_filter = f"WHERE {event_col_q} IN ({quoted})"

        query = f"""
            WITH windowed AS (
                SELECT *,
                    LEAD({ts_col_q}) OVER (
                        PARTITION BY {path_col_q} ORDER BY {ts_col_q}, {subindex_col_q}
                    ) AS _hop_next_ts,
                    (SELECT MAX({ts_col_q}) FROM df) AS _hop_dataset_end
                FROM df
                {active_filter}
            )
            SELECT * EXCLUDE (_hop_next_ts, _hop_dataset_end) FROM windowed
            WHERE {dialect.epoch("COALESCE(_hop_next_ts, _hop_dataset_end)")} - {dialect.epoch(ts_col_q)}
                  > {threshold_seconds}
        """
        return engine.run(query, df=df)
