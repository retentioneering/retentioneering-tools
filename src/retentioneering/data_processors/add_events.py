from typing import List, Tuple

import duckdb
import pandas as pd

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError

PROCESSOR_NAME = "add_events"


class AddEvents(DataProcessor):
    def __init__(
        self,
        new_event_name: str,
        source_events: List[str] | None = None,
        sql: str | None = None,
        churn: dict | None = None,
    ) -> None:
        if not isinstance(new_event_name, str) or not new_event_name:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Argument 'new_event_name' must be a non-empty string."
            )

        n_modes = sum([source_events is not None, sql is not None, churn is not None])
        if n_modes != 1:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                "Exactly one of 'source_events', 'sql', or 'churn' must be provided.",
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

        self.new_event_name = new_event_name
        self.source_events = source_events
        self.sql = sql
        self.churn = churn
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        if self.source_events is not None:
            df_source = self._get_by_source_events(df, schema)
        elif self.sql is not None:
            df_source = self._get_by_sql(df, schema)
        else:
            df_source = self._get_by_churn(df, schema)

        if df_source.empty:
            return df, schema

        event_types = EventTypes()
        df_new = df_source.copy()
        df_new[schema.event_col] = self.new_event_name
        df_new[schema.event_type] = event_types.SYNTHETIC_EVENT.type
        df_new[schema.subindex] = event_types.SYNTHETIC_EVENT.index

        df = (
            pd.concat([df, df_new])
            .sort_values([schema.path_col, schema.timestamp, schema.subindex])
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

    def _get_by_sql(self, df: pd.DataFrame, schema: EventstreamSchema) -> pd.DataFrame:
        columns_old = set(df.columns)
        eventstream = df  # noqa: F841  — referenced by user SQL as "eventstream"
        result = duckdb.sql(self.sql).df()
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
        ts_col = schema.timestamp
        subindex_col = schema.subindex
        event_col = schema.event_col

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
            quoted = ", ".join(f"'{e}'" for e in active_events)
            active_filter = f"WHERE {event_col} IN ({quoted})"

        query = f"""
            WITH windowed AS (
                SELECT *,
                    LEAD({ts_col}) OVER (
                        PARTITION BY {path_col} ORDER BY {ts_col}, {subindex_col}
                    ) AS _hop_next_ts,
                    (SELECT MAX({ts_col}) FROM df) AS _hop_dataset_end
                FROM df
                {active_filter}
            )
            SELECT * EXCLUDE (_hop_next_ts, _hop_dataset_end) FROM windowed
            WHERE epoch(COALESCE(_hop_next_ts, _hop_dataset_end)) - epoch({ts_col})
                  > {threshold_seconds}
        """
        return duckdb.sql(query).df()
