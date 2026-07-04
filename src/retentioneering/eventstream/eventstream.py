import json
from dataclasses import asdict
from functools import cached_property

import duckdb
import pandas as pd

from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.tools.types import T_TransitionMatrixValues, T_Diff


def _to_datetime_auto(series: pd.Series) -> pd.Series:
    if pd.api.types.is_integer_dtype(series):
        n = len(str(abs(int(series.iloc[0]))))
        unit = "s" if n <= 10 else "ms" if n <= 13 else "us" if n <= 16 else "ns"
        return pd.to_datetime(series, unit=unit)
    return pd.to_datetime(series)


try:
    from retentioneering._tracking import tracked as _tracked
except Exception:

    def _tracked(event_name, condition=None):  # type: ignore[misc]
        def decorator(func):
            return func

        return decorator


class Eventstream:
    def __init__(
        self, df: "pd.DataFrame | str", schema: dict | None = None, prepare: bool = True
    ):
        self._df = df
        self._schema = schema
        self.prepare = prepare
        self._post_init()

    @cached_property
    def schema(self) -> EventstreamSchema:
        return EventstreamSchema(**(self._schema or {}))

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    @df.setter
    def df(self, v: pd.DataFrame):
        self._df = v

    @_tracked(
        "eventstream_created",
        condition=lambda self: self.prepare,
        props_fn=lambda self: {
            "rows": self._df.shape[0],
            "cols": self._df.shape[1],
            "n_path_cols": len(self.schema.path_cols),
            "n_segment_cols": len(self.schema.segment_cols),
            "n_event_cols": len(self.schema.event_cols),
        },
    )
    def _post_init(self):
        if self.prepare:
            self._prepare()
        else:
            for col in self.schema.event_cols + self.schema.segment_cols:
                self._df[col] = self._df[col].astype("category")

    def _prepare(self):
        if isinstance(self._df, str):
            df = pd.read_csv(self._df)
        elif isinstance(self._df, pd.DataFrame):
            df = self._df.copy()
        else:
            raise ValueError(
                f"_df must be a DataFrame or CSV path, got {type(self._df)}"
            )

        schema = self.schema
        event_types = EventTypes()

        df[schema.timestamp] = _to_datetime_auto(df[schema.timestamp])

        for col in schema.path_cols:
            if df[col].dtype == "float64":
                df[col] = df[col].astype("str")

        for col in schema.event_cols + schema.segment_cols:
            df[col] = df[col].astype("category")

        if schema.event_type not in df.columns:
            df[schema.event_type] = event_types.RAW_EVENT.type
        if schema.subindex not in df.columns:
            df[schema.subindex] = df[schema.event_type].map(event_types.get_order())

        df = df.sort_values(
            [schema.path_col, schema.timestamp, schema.subindex]
        ).reset_index(drop=True)

        if schema.index not in df.columns:
            df[schema.index] = df.groupby(schema.path_col).cumcount() + 1

        self._df = df

    def to_dataframe(self, exclude_start_end: bool = True) -> pd.DataFrame:
        df = self._df.copy()
        if exclude_start_end:
            exclude = [EventTypes().PATH_START.type, EventTypes().PATH_END.type]
            df = df[~df[self.schema.event_type].isin(exclude)]
        return df

    def empty(self, exclude_start_end: bool = True) -> bool:
        # Cheap check on the underlying frame — to_dataframe() would deep-copy
        # the whole eventstream just to test emptiness.
        if not exclude_start_end:
            return self._df.empty
        exclude = [EventTypes().PATH_START.type, EventTypes().PATH_END.type]
        return bool(self._df[self.schema.event_type].isin(exclude).all())

    def equals(
        self,
        other: "Eventstream",
        exclude_start_end: bool = False,
        ignore_technical_columns: bool = True,
    ) -> bool:
        df1 = self.to_dataframe(exclude_start_end=exclude_start_end).reset_index(
            drop=True
        )
        df2 = other.to_dataframe(exclude_start_end=exclude_start_end).reset_index(
            drop=True
        )
        if ignore_technical_columns:
            drop = [self.schema.event_type, self.schema.index, self.schema.subindex]
            df1 = df1.drop(columns=[c for c in drop if c in df1.columns])
            df2 = df2.drop(columns=[c for c in drop if c in df2.columns])
        if set(df1.columns) != set(df2.columns):
            return False
        df2 = df2[df1.columns]
        return pd.DataFrame.equals(df1, df2)

    def get_event_counts(self, event_col: str | None = None) -> dict[str, int]:
        import duckdb

        event_col = event_col or self.schema.event_col
        df = self._df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        query = f"SELECT {event_col}, COUNT(*) AS cnt FROM df GROUP BY {event_col}"
        return duckdb.sql(query).df().set_index(event_col)["cnt"].to_dict()

    @cached_property
    def fingerprint(self) -> str:
        """Stable content-based hash identifying this eventstream's data shape and event distribution.
        Computed once and cached. Not cryptographically unique — collisions are theoretically possible
        but practically unlikely for datasets with different event sets or distributions."""
        import hashlib

        ec = self.schema.event_col
        pc = self.schema.path_col
        counts = sorted(
            (str(k), int(v)) for k, v in self._df[ec].value_counts().items()
        )
        s = self.schema
        payload = json.dumps(
            {
                "n_rows": len(self._df),
                "n_paths": int(self._df[pc].nunique()),
                "event_counts": counts,
                "schema": {
                    "path_cols": sorted(s.path_cols),
                    "event_cols": sorted(s.event_cols),
                    "segment_cols": sorted(s.segment_cols),
                    "custom_cols": sorted(s.custom_cols),
                },
            },
            sort_keys=True,
        )
        return hashlib.md5(payload.encode()).hexdigest()

    def get_all_segment_levels(self) -> dict[str, list[str]]:
        return {
            col: self._df[col].cat.categories.tolist()
            for col in self.schema.segment_cols
        }

    @_tracked("dp_filter_events")
    def filter_events(
        self, by_column: dict | None = None, func=None, sql: str | None = None
    ) -> "Eventstream":
        """
        Keep only rows that match a column filter, a Python predicate, or a SQL WHERE clause.

        Exactly one of `by_column`, `func`, or `sql` must be provided. If all are `None`
        the eventstream is returned unchanged.

        Parameters
        ----------
        by_column : dict, optional
            Dict with keys `"column"` (str), `"values"` (list), and an optional
            `"exclude"` (bool, default `False`). When `exclude` is `False`, keeps rows
            where the named column contains one of the listed values; when `True`,
            removes those rows instead.
        func : callable, optional
            A function that accepts the raw pandas DataFrame and returns a boolean Series.
            Rows where the Series is `True` are kept.
        sql : str, optional
            DuckDB SQL SELECT statement that reads from the `eventstream` table alias and
            returns all original columns. Example: `"SELECT * FROM eventstream WHERE event NOT LIKE 'system_%'"`.

        Examples
        --------
            stream.filter_events(by_column={"column": "event", "values": ["purchase", "add_to_cart"]})
            stream.filter_events(by_column={"column": "event", "values": ["system_event"], "exclude": True})
            stream.filter_events(sql="SELECT * FROM eventstream WHERE event NOT LIKE 'system_%'")
        """
        from retentioneering.data_processors.filter_events import FilterEvents

        if by_column is None and func is None and sql is None:
            return Eventstream(self._df.copy(), asdict(self.schema), prepare=False)
        new_df, new_schema = FilterEvents(values=by_column, func=func, sql=sql).apply(
            self._df, self.schema
        )
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_add_clusters")
    def add_clusters(
        self,
        segment_name: str,
        features: list,
        method: str = "kmeans",
        scaler=None,
        n_clusters=None,
        min_cluster_size=None,
        cluster_selection_epsilon=None,
        nmf_k=None,
        path_id_col=None,
        event_col=None,
    ) -> "Eventstream":
        """
        Cluster paths using ML and add a new segment column with integer cluster labels.

        Per-path metrics are computed from `features`, optionally scaled, then passed to
        the chosen clustering algorithm. The resulting cluster label is broadcast to every
        row of the corresponding path.

        Parameters
        ----------
        segment_name : str
            Name of the new segment column to add.
        features : list of dict
            Metric configurations for `MetricBuilder`. Each dict has a `"metric"` key
            (str) and an optional `"metric_args"` key (dict). Available metrics:
            `"length"`, `"duration"`, `"event_count"`, `"has"`, `"time_between"`,
            `"first_event_dt"`, `"active_days"`, `"matches"`, `"belongs_to"`.
            See `stream.get_metrics()` for the full metric reference.
        method : str, default `"kmeans"`
            Clustering algorithm. One of `"kmeans"` or `"hdbscan"`.
        scaler : str or None, default `None`
            Feature scaler applied before clustering. One of `"minmax"`, `"std"`, or `None`.
        n_clusters : int, optional
            Number of clusters (required for `"kmeans"`).
        min_cluster_size : int, optional
            Minimum cluster size (used by `"hdbscan"`).
        cluster_selection_epsilon : float, optional
            HDBSCAN cluster-selection epsilon.
        nmf_k : int, optional
            When set, reduces features to `nmf_k` NMF components before clustering.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            stream.add_clusters(
                segment_name="cluster",
                features=[
                    {"metric": "length"},
                    {"metric": "event_count", "metric_args": {"events": "purchase"}},
                ],
                method="kmeans",
                n_clusters=4,
                scaler="minmax",
            )
        """
        from retentioneering.data_processors.add_clusters import AddClusters

        new_df, new_schema = AddClusters(
            eventstream=self,
            segment_name=segment_name,
            features=features,
            method=method,
            scaler=scaler,
            n_clusters=n_clusters,
            min_cluster_size=min_cluster_size,
            cluster_selection_epsilon=cluster_selection_epsilon,
            nmf_k=nmf_k,
            path_id_col=path_id_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_url_events")
    def url_events(
        self,
        column: str,
        nodes: list,
        strip_host: bool = True,
        strip_cgi: bool = True,
        strip_locale: bool = True,
        slug_enabled: bool = True,
        host_col=None,
        cgi_col=None,
        locale_col=None,
        slug_col=None,
    ) -> "Eventstream":
        """
        Parse a raw URL column into structured event name labels using a path tree.

        Each URL is matched against the `nodes` tree. Matching cut nodes become the
        event label; pages deeper than a cut node get a `cut_path/slug` label. The
        original URL column is replaced in-place.

        Parameters
        ----------
        column : str
            Name of the column that contains raw URL strings.
        nodes : list of dict
            URL path tree. Each node dict must have a `"path"` key (str) and may
            include `"is_cut"` (bool), `"is_deleted"` (bool), and `"custom_name"` (str).
        strip_host : bool, default `True`
            Remove the scheme and hostname, keeping only the pathname.
        strip_cgi : bool, default `True`
            Remove the query string and URL fragment.
        strip_locale : bool, default `True`
            Remove a leading 2-letter BCP-47 locale segment (e.g. `"en"`, `"fr-ca"`).
        slug_enabled : bool, default `True`
            When `False`, cut nodes are ignored and every URL keeps its normalized path.
        host_col : str, optional
            If provided, save the extracted hostname into this new column.
        cgi_col : str, optional
            If provided, save the extracted query string into this new column.
        locale_col : str, optional
            If provided, save the detected locale prefix into this new column.
        slug_col : str, optional
            If provided, save the sub-page slug into this new column.

        Examples
        --------
            stream.url_events(
                column="page",
                nodes=[
                    {"path": "/catalog", "is_cut": True},
                    {"path": "/checkout", "is_cut": True, "custom_name": "checkout"},
                ],
            )
        """
        from retentioneering.data_processors.url_events import UrlEvents

        new_df, new_schema = UrlEvents(
            column=column,
            nodes=nodes,
            strip_host=strip_host,
            strip_cgi=strip_cgi,
            strip_locale=strip_locale,
            slug_enabled=slug_enabled,
            host_col=host_col,
            cgi_col=cgi_col,
            locale_col=locale_col,
            slug_col=slug_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_filter_paths")
    def filter_paths(
        self,
        ast_condition: dict,
        path_id_col: str | None = None,
        event_col: str | None = None,
    ) -> "Eventstream":
        """
        Keep only paths that satisfy an AST-based metric condition.

        The condition is a tree of comparison nodes connected by `and` / `or` / `not`
        branch nodes. Per-path metrics are computed once and the condition is evaluated
        in SQL.

        Raises `EmptyEventstreamError` when no paths match.

        Parameters
        ----------
        ast_condition : dict
            Condition tree. Leaf nodes have the keys:
              - `op` — comparison operator: `>`, `>=`, `<`, `<=`, `=`, `!=`.
              - `metric` — metric name (see `stream.get_metrics()` for the full list).
              - `value` — threshold value.
              - `metric_args` (optional) — dict of extra arguments for the metric.
            Branch nodes have `op` set to `and`, `or`, or `not` and an `args` list of
            child nodes.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            # Keep paths that contain at least one purchase
            stream.filter_paths({"op": ">", "metric": "event_count", "value": 0, "metric_args": {"events": "purchase"}})

            # Keep paths longer than 3 events that match a funnel pattern
            stream.filter_paths({
                "op": "and",
                "args": [
                    {"op": ">", "metric": "length", "value": 3},
                    {"op": "=", "metric": "matches", "value": True,
                     "metric_args": {"pattern": "registration->.*->purchase"}},
                ]
            })
        """
        from retentioneering.data_processors.filter_paths import FilterPaths
        from retentioneering.exceptions import EmptyEventstreamError

        dp = FilterPaths(ast_condition, path_id_col, event_col)
        path_id_col = path_id_col or self.schema.path_col

        # Extract metric configs
        metric_configs = dp._get_metric_configs(ast_condition)

        # Build metrics
        metrics = self.get_metrics(  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
            metric_configs, path_id_col=path_id_col
        ).reset_index()
        condition = dp._get_where_condition(ast_condition)
        query = f"SELECT {path_id_col} FROM metrics WHERE {condition}"
        path_ids = duckdb.sql(query).df()[path_id_col].tolist()

        if len(path_ids) == 0:
            raise EmptyEventstreamError("no paths match the filter_paths condition")

        result_stream = self.filter_events(
            by_column={"column": path_id_col, "values": path_ids}
        )
        if result_stream.empty():
            raise EmptyEventstreamError("no events remain after filter_paths")
        return result_stream

    def get_metrics(
        self, metrics: list, path_id_col: str | None = None
    ) -> pd.DataFrame:
        """
        Build metrics for each path in the eventstream.

        Args:
            metrics: List of metric configuration dicts with 'metric' and optional 'metric_args' fields
            path_id_col: Path ID column (if None, taken from schema)

        Returns:
            DataFrame with path_id as index and metrics as columns
        """
        from retentioneering.metrics.metric_builder import MetricBuilder

        builder = MetricBuilder(self)
        return builder.build_metrics(metrics, path_id_col)

    @_tracked("dp_add_events")
    def add_events(
        self, new_event_name: str, source_events=None, sql=None, churn=None
    ) -> "Eventstream":
        """
        Insert synthetic events derived from existing events or a SQL query.

        Exactly one of `source_events`, `sql`, or `churn` must be provided.
        The new event rows are appended to the eventstream; original rows are kept.

        Parameters
        ----------
        new_event_name : str
            Name of the synthetic event to create.
        source_events : list of str, optional
            List of existing event names. For each path, a synthetic event is inserted
            at the timestamp of the first matching source event.
        sql : str, optional
            DuckDB SQL SELECT statement that reads from the `eventstream` table alias
            and returns rows in the eventstream schema. Each returned row is added as a
            new synthetic event.
        churn : dict, optional
            Creates a churn event after a period of inactivity. Required key:
              - `inactivity_days` (int or float) — gap in days after which a churn event
                is inserted.
            Optional key:
              - `active_events` (list of str) — only these events count as activity;
                defaults to all events.

        Examples
        --------
            stream.add_events("session_start", source_events=["login", "app_open"])
            stream.add_events("churned", churn={"inactivity_days": 30})
            stream.add_events("churned", churn={"inactivity_days": 30, "active_events": ["purchase"]})
        """
        from retentioneering.data_processors.add_events import AddEvents

        new_df, new_schema = AddEvents(
            new_event_name, source_events=source_events, sql=sql, churn=churn
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_add_segment")
    def add_segment(
        self,
        name: str,
        values=None,
        func=None,
        sql=None,
        funnel_events=None,
        path_id_col=None,
    ) -> "Eventstream":
        """
        Add a new categorical segment column to the eventstream.

        Exactly one of `values`, `func`, `sql`, or `funnel_events` must be provided.

        Parameters
        ----------
        name : str
            Name of the new segment column.
        values : list, optional
            CASE-WHEN rules. A list of conditions plus a final else entry:
              - Each condition entry is `[column, op, value, label]` — translates to
                `WHEN <column> <op> <value> THEN <label>` in SQL.
              - The last entry is `[else_label]` — the ELSE branch label.
            Example: `[["country", "=", "US", "domestic"], ["international"]]`.
        func : callable, optional
            A function that accepts the raw pandas DataFrame and returns a collection of
            segment labels with the same length and order as the eventstream rows.
        sql : str, optional
            DuckDB SQL SELECT statement that reads from the `eventstream` table alias and
            returns exactly one column — the segment label for each row. Row count and
            order must match the eventstream.
            Example: `"SELECT CASE WHEN platform = 'mobile' THEN 'mobile' ELSE 'web' END FROM eventstream"`.
        funnel_events : list of str, optional
            Ordered list of at least 2 event names defining a funnel. Each path is
            assigned the name of the last funnel step reached in sequence, or
            `out_of_funnel` if the first step was never reached.
            Segment values (in ascending funnel order): `out_of_funnel`, then each
            event name from `funnel_events[0]` to `funnel_events[-1]`.
        path_id_col : str, optional
            Path ID column override for `funnel_events` mode; defaults to
            `schema.path_col`.

        Examples
        --------
            # values mode — CASE WHEN rules
            stream.add_segment(
                "region",
                values=[
                    ["country", "=", "US", "domestic"],
                    ["country", "in", "('GB', 'DE', 'FR')", "europe"],
                    ["other"],
                ],
            )

            # funnel_events mode — assign the last funnel step reached per path
            stream.add_segment(
                "funnel",
                funnel_events=["add_to_cart", "checkout_start", "purchase"],
            )
            # Resulting segment values: out_of_funnel | add_to_cart | checkout_start | purchase

            # sql mode — one computed column, same row order as the eventstream
            stream.add_segment(
                "device",
                sql="SELECT CASE WHEN platform = 'mobile' THEN 'mobile' ELSE 'web' END FROM eventstream",
            )
        """
        from retentioneering.data_processors.add_segment import AddSegment

        new_df, new_schema = AddSegment(
            name,
            values=values,
            func=func,
            sql=sql,
            funnel_events=funnel_events,
            path_id_col=path_id_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_collapse_events")
    def collapse_events(
        self,
        repetitive=None,
        event_groups=None,
        event_from_col=None,
        session_id_col=None,
        session_type_col=None,
        agg=None,
        path_id_col=None,
        event_col=None,
    ) -> "Eventstream":
        """
        Merge consecutive or grouped events into a single representative event.

        Exactly one of `repetitive`, `event_groups`, `event_from_col`, or
        `session_id_col` must be provided.

        Parameters
        ----------
        repetitive : bool or list of str, optional
            Collapse consecutive identical events into one.
            Pass `True` to collapse all events; pass a list of event names to collapse
            only those specific events.
        event_groups : list of dict, optional
            Merge a set of events that belong together into a single representative event.
            Each group dict must have either an `events` key (list of event names to
            merge) or a `separator` / `start_event` + `end_event` pair. Additional keys:
              - `name` (str) — label for the merged event; defaults to the group's first event.
        event_from_col : str, optional
            Name of a column whose value replaces the event name. Consecutive rows with
            the same column value are collapsed into one event.
        session_id_col : str, optional
            Collapse events within each session defined by this column. Requires
            `session_type_col` as well.
        session_type_col : str, optional
            Column that distinguishes session event types (used with `session_id_col`).
        agg : dict, optional
            Aggregation rules for non-event columns when rows are merged, as a
            `{column: agg_func}` dict. Example: `{"duration": "sum"}`.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            # Collapse any run of the same event
            stream.collapse_events(repetitive=True)

            # Collapse only repeated page_view events
            stream.collapse_events(repetitive=["page_view"])

            # Merge checkout steps into a single "checkout" event
            stream.collapse_events(event_groups=[{"events": ["checkout_start", "checkout_step", "checkout_confirm"], "name": "checkout"}])
        """
        from retentioneering.data_processors.collapse_events import CollapseEvents

        new_df, new_schema = CollapseEvents(
            repetitive=repetitive,
            event_groups=event_groups,
            event_from_col=event_from_col,
            session_id_col=session_id_col,
            session_type_col=session_type_col,
            agg=agg,
            path_id_col=path_id_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_daily_states")
    def daily_states(
        self,
        active_events=None,
        max_dormant_days: int = 30,
        agg=None,
        path_id_col=None,
        event_col=None,
    ) -> "Eventstream":
        """
        Convert the eventstream into daily lifecycle-state events.

        Each path is expanded to one row per calendar day from its first event
        to `max_dormant_days` days after its last event. Every row is labelled
        with one of six engagement states.

        Active days:
            `new`          — first-ever active day for this path
            `current`      — active within the past 7 days
            `reactivated`  — active 8-30 days ago, not in the last 7
            `resurrected`  — last active more than 30 days ago

        Inactive days:
            `at_risk_wau`  — was active within the past 7 days
            `at_risk_mau`  — was active 8-30 days ago
            `dormant`      — was last active more than 30 days ago

        Parameters
        ----------
        active_events : list of str, optional
            Events that count as "activity". If omitted, any event counts.
        max_dormant_days : int, default 30
            Days after last event to continue generating state rows.
        agg : dict, optional
            Per-column aggregation overrides (e.g. `{"revenue": "sum"}`).
        path_id_col : str, optional
            Override the path ID column.
        event_col : str, optional
            Override the event column.

        Examples
        --------
            stream.daily_states()
            stream.daily_states(active_events=["purchase", "add_to_cart"], max_dormant_days=60)

        As a preprocessor in MCP update_base_stream / local_preprocessors:
            {"type": "daily_states"}
            {"type": "daily_states", "active_events": ["purchase"], "max_dormant_days": 60}
        """
        from retentioneering.data_processors.daily_states import DailyStates

        new_df, new_schema = DailyStates(
            active_events=active_events,
            max_dormant_days=max_dormant_days,
            agg=agg,
            path_id_col=path_id_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_drop_segment")
    def drop_segment(self, name: str) -> "Eventstream":
        """
        Remove a segment column from the eventstream.

        Parameters
        ----------
        name : str
            Name of the segment column to remove. Must exist in `schema.segment_cols`.

        Examples
        --------
            stream.drop_segment("cluster")
        """
        from retentioneering.data_processors.drop_segment import DropSegment

        new_df, new_schema = DropSegment(name).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_edit_events")
    def edit_events(self, rename=None, delete=None) -> "Eventstream":
        """
        Rename and/or delete events in a single operation.

        At least one of `rename` or `delete` must be provided. Events listed in both
        `rename` and `delete` are deleted.

        Parameters
        ----------
        rename : dict, optional
            Mapping of `{old_name: new_name}`. Events whose current name is a key are
            renamed to the corresponding value.
        delete : list of str, optional
            Event names to remove from the eventstream entirely.

        Examples
        --------
            stream.edit_events(rename={"old_checkout": "checkout"}, delete=["system_ping"])
            stream.edit_events(delete=["debug_event", "internal_event"])
        """
        from retentioneering.data_processors.edit_events import EditEvents

        new_df, new_schema = EditEvents(rename=rename, delete=delete).apply(
            self._df, self.schema
        )
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_rename_events")
    def rename_events(self, mapping: dict) -> "Eventstream":
        """
        Rename events using a mapping dict.

        Events not present in `mapping` are left unchanged. To also delete events in
        the same step, use `edit_events` instead.

        Parameters
        ----------
        mapping : dict
            Mapping of `{old_name: new_name}`.

        Examples
        --------
            stream.rename_events({"old_checkout": "checkout", "cart_add": "add_to_cart"})
        """
        from retentioneering.data_processors.rename_events import RenameEvents

        new_df, new_schema = RenameEvents(mapping).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_sample_paths")
    def sample_paths(
        self, sample_size, random_state=None, path_id_col=None
    ) -> "Eventstream":
        """
        Randomly sample paths (and all their events).

        Parameters
        ----------
        sample_size : int or float
            Number of paths to keep (int), or a fraction of total paths in the range
            `(0.0, 1.0]` (float). Passing `1.0` returns the eventstream unchanged.
        random_state : int, optional
            Seed for the random number generator; pass an integer for reproducible results.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

        Examples
        --------
            stream.sample_paths(1000)
            stream.sample_paths(0.1, random_state=42)  # 10 % of paths
        """
        from retentioneering.data_processors.sample_paths import SamplePaths

        new_df, new_schema = SamplePaths(
            sample_size=sample_size, random_state=random_state, path_id_col=path_id_col
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_split_sessions")
    def split_sessions(
        self,
        session_col="session_id",
        session_index_col="session_index",
        separator=None,
        start_event=None,
        end_event=None,
        timeout=None,
        path_id_col=None,
        event_col=None,
    ) -> "Eventstream":
        """
        Split each path into sub-sessions and add session ID and index columns.

        At least one boundary criterion must be provided: `separator`,
        `start_event` + `end_event`, or `timeout`. `separator` and
        `start_event`/`end_event` are mutually exclusive; `timeout` may be combined
        with either.

        Parameters
        ----------
        session_col : str, default `"session_id"`
            Name of the new column that holds the unique session identifier.
        session_index_col : str, default `"session_index"`
            Name of the new column that holds the 0-based session index within each path.
        separator : str or list of str, optional
            Event name(s) that mark a session boundary. The separator event starts a new
            session; the separator row itself belongs to the new session.
        start_event : str or list of str, optional
            Event name(s) that mark the start of a session. Must be provided together
            with `end_event`.
        end_event : str or list of str, optional
            Event name(s) that mark the end of a session. Must be provided together
            with `start_event`.
        timeout : int or float, optional
            Inactivity gap in seconds. A new session starts when the gap between
            consecutive events exceeds this threshold.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            stream.split_sessions(timeout=1800)
            stream.split_sessions(separator="app_open")
            stream.split_sessions(start_event="session_start", end_event="session_end")
            stream.split_sessions(separator="app_open", timeout=3600)
        """
        from retentioneering.data_processors.split_sessions import SplitSessions

        new_df, new_schema = SplitSessions(
            session_col=session_col,
            session_index_col=session_index_col,
            separator=separator,
            start_event=start_event,
            end_event=end_event,
            timeout=timeout,
            path_id_col=path_id_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("dp_truncate_paths")
    def truncate_paths(
        self, left: str, right: str, path_id_col=None, event_col=None
    ) -> "Eventstream":
        """
        Trim each path to the window between two anchor events (inclusive).

        For each path, the first occurrence of `left` and the first occurrence of `right`
        that comes after `left` are found. Events outside this window are dropped. Paths
        that do not contain both anchors in the correct order are removed entirely.

        Parameters
        ----------
        left : str
            Name of the event that marks the start of the window.
        right : str
            Name of the event that marks the end of the window.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            stream.truncate_paths(left="registration", right="purchase")
        """
        from retentioneering.data_processors.truncate_paths import TruncatePaths

        new_df, new_schema = TruncatePaths(
            left=left, right=right, path_id_col=path_id_col, event_col=event_col
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    def split_two(self, split, path_id_col: str | None = None):
        from retentioneering.exceptions import EmptyEventstreamError, DiffConfigError

        if len(split) == 3:
            segment_col, v1, v2 = split[0], split[1], split[2]
            if segment_col not in self.schema.segment_cols:
                raise DiffConfigError(f"'{segment_col}' is not a segment column")
            s1 = self.filter_events({"column": segment_col, "values": [v1]})
            if v2 == "<OUTER>":
                all_vals = set(self.get_all_segment_levels().get(segment_col, []))
                v2_vals = list(all_vals - {v1})
            else:
                v2_vals = [v2]
            s2 = self.filter_events({"column": segment_col, "values": v2_vals})
        elif len(split) == 2:
            ids1, ids2 = split[0], split[1]
            path_id_col = path_id_col or self.schema.path_col
            s1 = self.filter_events({"column": path_id_col, "values": list(ids1)})
            s2 = self.filter_events({"column": path_id_col, "values": list(ids2)})
        else:
            raise DiffConfigError("diff must be (seg, v1, v2) or (ids1, ids2)")
        if s1.empty():
            raise EmptyEventstreamError("first diff group is empty")
        if s2.empty():
            raise EmptyEventstreamError("second diff group is empty")
        return s1, s2

    @_tracked("dp_add_start_end_events")
    def add_start_end_events(self, path_id_col: str | None = None) -> "Eventstream":
        """
        Prepend a `path_start` and append a `path_end` synthetic event to each path.

        Idempotent: a path that already starts or ends with these events is left
        unchanged on that side.

        You normally don't need to call this directly — `transition_graph`,
        `step_matrix`, and `step_sankey` insert `path_start`/`path_end` themselves,
        each using its own `path_id_col`. Calling this upfront bakes in one
        specific path definition and can produce misleading boundaries if a
        widget is later given a different `path_id_col`.

        Parameters
        ----------
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

        Examples
        --------
            stream.add_start_end_events()
        """
        from retentioneering.data_processors.add_start_end_events import (
            AddStartEndEvents,
        )

        dp = AddStartEndEvents(path_id_col)
        new_df, new_schema = dp.apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), prepare=False)

    @_tracked("headless_transition_graph")
    def transition_graph_data(
        self,
        edge_weight: T_TransitionMatrixValues = "proba_out",
        path_id_col: str | None = None,
        diff: T_Diff = None,
    ) -> pd.DataFrame:
        """
        Compute the transition matrix between events (headless).

        Parameters
        ----------
        edge_weight : {"proba_out", "proba_in", "count", "unique_paths", "transition_rate", "per_path", "time_median", "time_q95"}, default "proba_out"
            Value to compute for each source -> target pair:
              - `"proba_out"` — share of transitions out of the source event that land on this target.
              - `"proba_in"` — share of transitions into the target event that come from this source.
              - `"count"` — number of times the transition occurred.
              - `"unique_paths"` — number of distinct paths containing the transition.
              - `"transition_rate"` — share of this transition among all transitions in the eventstream.
              - `"per_path"` — average number of occurrences per path.
              - `"time_median"` / `"time_q95"` — median / 95th-percentile seconds between the two events.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        diff : tuple, optional
            `(segment_col, value1, value2)` to compare two segment values, or
            `(path_ids1, path_ids2)` to compare two explicit path-id groups.
            `value2` may be `"<OUTER>"`, meaning "every other value of `segment_col`".

        Returns
        -------
        pd.DataFrame
            Events x events matrix of the selected `edge_weight`. In diff mode,
            returns `(diff, group1, group2)` — three matrices instead of one.

        Examples
        --------
            stream.transition_graph_data(edge_weight="count")
            diff, g1, g2 = stream.transition_graph_data(diff=("platform", "mobile", "desktop"))
        """
        from retentioneering.tools.transition_matrix import TransitionMatrix

        return TransitionMatrix(self).fit(edge_weight, diff, path_id_col)

    @_tracked("headless_step_sankey")
    def step_sankey_data(
        self,
        max_steps: int = 10,
        diff: T_Diff = None,
        path_id_col: str | None = None,
        path_pattern: str | None = None,
    ):
        """
        Compute per-step event-share matrices for Step Matrix / Step Sankey (headless).

        Both widgets render the same underlying data — Step Matrix as a heatmap,
        Step Sankey as a flow diagram.

        Parameters
        ----------
        max_steps : int, default 10
            Number of path steps to compute (on each side of an anchor, when
            `path_pattern` is given).
        diff : tuple, optional
            `(segment_col, value1, value2)` or `(path_ids1, path_ids2)`; `value2`
            may be `"<OUTER>"`. See `transition_graph_data` for the shared diff
            semantics.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        path_pattern : str, optional
            Restrict/split paths using a `"->"`-separated sequence of anchor
            events, where `.*` matches any run of events, e.g.
            `".*->add_to_cart->.*->purchase"`. Without a pattern, computes over
            the whole path from `path_start` to `path_end`. Each anchor event in
            the pattern produces its own matrix block.

        Returns
        -------
        tuple of pd.DataFrame
            One matrix per anchor block. In diff mode, returns
            `(combined_blocks, group1_blocks, group2_blocks)`, each itself a
            tuple of per-block DataFrames.

        Examples
        --------
            stream.step_sankey_data(max_steps=10)
            stream.step_sankey_data(path_pattern=".*->purchase")
            combined, g1, g2 = stream.step_sankey_data(diff=("plan", "pro", "free"))
        """
        from retentioneering.tools.step_matrix import StepMatrix

        return StepMatrix(self).fit(
            max_steps=max_steps,
            diff=diff,
            path_id_col=path_id_col,
            path_pattern=path_pattern,
        )

    @_tracked("widget_step_sankey")
    def step_sankey(
        self,
        max_steps=None,
        diff=None,
        path_id_col=None,
        path_pattern=None,
        step_window=None,
        height=None,
        sidebar_open=None,
    ):
        """
        Interactive step-by-step Sankey diagram for Jupyter notebooks.

        Shows which events paths pass through at each step, and how paths
        diverge over time. Each column sums to 1 (share of paths at that step);
        in diff mode, each column shows `value2 - value1` and sums to 0. Shares
        its underlying data with `step_matrix` (see `step_sankey_data`) — Sankey
        renders it as a flow diagram, Step Matrix as a heatmap.

        All parameters are also editable from the widget's sidebar without
        re-running the cell.

        Parameters
        ----------
        max_steps : int, default 10
            Number of path steps to compute.
        step_window : int, default 3
            Number of step columns shown around each anchor.
        diff : tuple, optional
            `(segment_col, value1, value2)` or `(path_ids1, path_ids2)`; `value2`
            may be `"<OUTER>"`.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        path_pattern : str, optional
            Same syntax as `step_matrix`'s `path_pattern`.
        height : int, default 500
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.

        Examples
        --------
            stream.step_sankey(max_steps=15, path_pattern=".*->purchase->.*")
            stream.step_sankey(diff=("country", "US", "<OUTER>"))
        """
        from retentioneering.widgets.step_sankey import StepSankeyWidget, _UNSET

        return StepSankeyWidget(
            eventstream=self,
            max_steps=max_steps if max_steps is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_id_col=path_id_col if path_id_col is not None else _UNSET,
            path_pattern=path_pattern if path_pattern is not None else _UNSET,
            step_window=step_window if step_window is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
        )

    @_tracked("widget_step_matrix")
    def step_matrix(
        self,
        cloud_file_name: str | None = None,
        max_steps=None,
        diff=None,
        path_id_col=None,
        path_pattern=None,
        height=None,
        sidebar_open=None,
    ):
        """
        Interactive step-by-step transition heatmap for Jupyter notebooks.

        Each cell shows the share of paths passing through a given event at a
        given step relative to an anchor: columns are step offsets from the
        anchor (negative = before, positive = after), rows are events. In
        standard mode each column sums to 1; in diff mode each cell is
        `value2 - value1` and columns sum to 0. Shares its underlying data with
        `step_sankey` (see `step_sankey_data`).

        All parameters are also editable from the widget's sidebar without
        re-running the cell.

        Parameters
        ----------
        cloud_file_name : str, optional
            Save/restore this widget's configuration (including manual layout
            tweaks) to the cloud under this name.
        max_steps : int, default 10
            Number of path steps to compute on each side of the anchor.
        diff : tuple, optional
            `(segment_col, value1, value2)` or `(path_ids1, path_ids2)`; `value2`
            may be `"<OUTER>"`.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        path_pattern : str, optional
            Restrict/split paths using a `"->"`-separated sequence of anchor
            events, where `.*` matches any run of events, e.g.
            `".*->add_to_cart->.*->purchase"`. Without a pattern, shows the
            whole path from `path_start` to `path_end`. Multiple anchors render
            one matrix block per anchor, side by side. A pattern that doesn't
            start at `path_start` or end at `path_end` shows a serrated edge,
            signalling paths continue beyond the visible range.
        height : int, default 600
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.

        Examples
        --------
            stream.step_matrix(path_pattern=".*->purchase")
            stream.step_matrix(path_pattern=".*->add_to_cart->.*->purchase")
            stream.step_matrix(diff=("is_new_user", False, True))
        """
        from retentioneering.widgets.step_matrix import StepMatrixWidget, _UNSET

        return StepMatrixWidget(
            eventstream=self,
            cloud_file_name=cloud_file_name,
            max_steps=max_steps if max_steps is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_id_col=path_id_col if path_id_col is not None else _UNSET,
            path_pattern=path_pattern if path_pattern is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
        )

    @_tracked("widget_transition_graph")
    def transition_graph(
        self,
        edge_weight=None,
        diff=None,
        path_id_col=None,
        height=None,
        sidebar_open=None,
        cloud_file_name: str | None = None,
    ):
        """
        Interactive transition graph for Jupyter notebooks.

        Nodes are events, edges are transitions between them. Supports diff mode
        to compare two segments side by side. All parameters are also editable
        from the widget's sidebar without re-running the cell.

        Parameters
        ----------
        edge_weight : {"proba_out", "proba_in", "count", "unique_paths", "transition_rate", "per_path", "time_median", "time_q95"}, default "proba_out"
            Value shown on edges. See `transition_graph_data` for what each
            value means.
        diff : tuple, optional
            `(segment_col, value1, value2)` or `(path_ids1, path_ids2)`; `value2`
            may be `"<OUTER>"`, meaning "every other value of `segment_col`".
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        height : int, default 500
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.
        cloud_file_name : str, optional
            Save/restore this widget's configuration (including manual node
            layout) to the cloud under this name.

        Examples
        --------
            stream.transition_graph()
            stream.transition_graph(edge_weight="count", diff=("plan", "pro", "free"))
        """
        from retentioneering.widgets.transition_graph import (
            TransitionGraphWidget,
            _UNSET,
        )

        return TransitionGraphWidget(
            eventstream=self,
            cloud_file_name=cloud_file_name,
            edge_weight=edge_weight if edge_weight is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_id_col=path_id_col if path_id_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
        )

    @_tracked("widget_funnel")
    def funnel(
        self,
        steps: list[str] | None = None,
        diff=None,
        path_id_col: str | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
    ):
        """
        Interactive conversion funnel for Jupyter notebooks.

        A path is counted at step N if it contains that step's event after
        already passing through all previous steps. Supports diff mode to
        compare two segments side by side. `steps` is also editable from the
        widget's sidebar without re-running the cell.

        Parameters
        ----------
        steps : list of str, optional
            Ordered event names defining the funnel steps.
        diff : tuple, optional
            `(segment_col, value1, value2)` or `(path_ids1, path_ids2)`; `value2`
            may be `"<OUTER>"`.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        height : int, default 420
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.

        Examples
        --------
            stream.funnel(steps=["page_view", "add_to_cart", "purchase"])
            stream.funnel(steps=["add_to_cart", "purchase"], diff=("plan", "pro", "free"))
        """
        from retentioneering.widgets.funnel import FunnelWidget, _UNSET

        return FunnelWidget(
            eventstream=self,
            steps=steps if steps is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_id_col=path_id_col if path_id_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
        )

    @_tracked("headless_funnel")
    def funnel_data(
        self,
        steps: list[str] | None = None,
        diff=None,
        path_id_col: str | None = None,
    ) -> dict:
        """Compute funnel conversion metrics and return a dict (headless).

        Returns
        -------
        dict with key "steps", each item containing step name, unique_paths,
        conversion_rate (and diff fields when diff is provided).
        """
        from retentioneering.tools.funnel import Funnel

        if not steps:
            return {"steps": []}
        return Funnel(self).fit(steps=steps, diff=diff, path_id_col=path_id_col)

    @_tracked("widget_segment_overview")
    def segment_overview(
        self,
        segment_col: str | None = None,
        metrics_config: list | None = None,
        path_id_col: str | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
    ):
        """
        Interactive segment comparison heatmap for Jupyter notebooks.

        Rows are metrics, columns are segment values. Click a cell to see that
        metric's distribution for the segment; shift-click a second cell in the
        same row to compare two distributions side by side. `segment_col` and
        `metrics_config` are also editable from the widget's sidebar without
        re-running the cell.

        Parameters
        ----------
        segment_col : str, optional
            Segment column to split by; must be one of `schema.segment_cols`.
            Required (directly or via the sidebar) before the widget computes
            anything.
        metrics_config : list of dict, optional
            Metric configurations, each with a `"metric"` key, optional
            `"metric_args"`, and an `"agg"` key (`"mean"`, `"median"`, `"q5"`,
            `"q25"`, `"q75"`, `"q95"`, or `"complement_diff"`) controlling how
            per-path values roll up across a segment. See `stream.get_metrics()`
            for the metric reference.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        height : int, default 480
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.

        Examples
        --------
            stream.segment_overview(
                segment_col="plan",
                metrics_config=[
                    {"metric": "length", "agg": "mean"},
                    {"metric": "event_count", "metric_args": {"events": "purchase"}, "agg": "mean"},
                ],
            )
        """
        from retentioneering.widgets.segment_overview import (
            SegmentOverviewWidget,
            _UNSET,
        )

        return SegmentOverviewWidget(
            eventstream=self,
            segment_col=segment_col if segment_col is not None else _UNSET,
            metrics_config=metrics_config if metrics_config is not None else _UNSET,
            path_id_col=path_id_col if path_id_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
        )

    @_tracked("headless_segment_overview")
    def segment_overview_data(
        self,
        segment_col: str,
        metrics_config: list | None = None,
        path_id_col: str | None = None,
        event_col: str | None = None,
    ) -> "pd.DataFrame":
        """Compute aggregated metrics across segment values (headless).

        Returns a DataFrame with metrics as rows and segment values as columns.
        Always includes segment_size and segment_share as first two rows.
        """
        from retentioneering.tools.segment_overview import SegmentOverview

        return SegmentOverview(self).fit(
            segment_col=segment_col,
            metrics_config=metrics_config or [],
            path_id_col=path_id_col,
            event_col=event_col,
        )

    @_tracked("widget_cluster_analysis")
    def cluster_analysis(
        self,
        features: list | None = None,
        method: str | None = None,
        scaler: str | None = None,
        n_clusters=None,
        metrics_config: list | None = None,
        path_id_col: str | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
    ):
        """
        Interactive clustering widget for Jupyter notebooks.

        Clusters paths by behavioral metrics and shows a Segment Overview-style
        heatmap for the resulting clusters. All parameters are also editable
        from the widget's sidebar (including feature/metric configuration and
        an NMF decomposition option not exposed here as a direct argument).

        Parameters
        ----------
        features : list of dict, optional
            Metric configurations used as clustering features (see
            `stream.get_metrics()`); defaults to per-event counts for every
            event in the eventstream.
        method : {"kmeans", "hdbscan"}, default "kmeans"
            Clustering algorithm.
        scaler : {"minmax", "std"}, optional
            Feature scaler applied before clustering; default `"minmax"`.
        n_clusters : int, list of int, or str, optional
            Number of clusters. A single int fixes the cluster count; a list of
            ints or a range string (e.g. `"3-8"`) runs a silhouette-scored grid
            search over that range and picks the best. Defaults to `"3-8"`.
        metrics_config : list of dict, optional
            Metrics shown in the overview heatmap after clustering (independent
            of `features`); defaults to per-event counts for every event.
        path_id_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        height : int, default 520
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.

        Examples
        --------
            stream.cluster_analysis(
                features=[{"metric": "length"}, {"metric": "duration"}],
                n_clusters="3-6",
            )
        """
        from retentioneering.widgets.cluster_analysis import (
            ClusterAnalysisWidget,
            _UNSET,
        )

        return ClusterAnalysisWidget(
            eventstream=self,
            features=features if features is not None else _UNSET,
            method=method if method is not None else _UNSET,
            scaler=scaler if scaler is not None else _UNSET,
            n_clusters=n_clusters if n_clusters is not None else _UNSET,
            metrics_config=metrics_config if metrics_config is not None else _UNSET,
            path_id_col=path_id_col if path_id_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
        )

    @_tracked("headless_cluster_analysis")
    def cluster_analysis_data(
        self,
        features: list,
        method: str = "kmeans",
        scaler: str | None = "minmax",
        n_clusters=None,
        min_cluster_size=None,
        cluster_selection_epsilon=None,
        nmf_k=None,
        metrics_config: list | None = None,
        path_id_col: str | None = None,
        event_col: str | None = None,
    ) -> dict:
        """Run cluster analysis headlessly and return dict with overview_df / silhouette / nmf.

        Pass lists for n_clusters / nmf_k / min_cluster_size to trigger grid search
        with silhouette scoring. n_clusters is required for the kmeans method
        (the default), including nmf_k-only searches.
        """
        from retentioneering.tools.cluster_analysis import ClusterAnalysis

        return ClusterAnalysis(self).fit(
            features_config=features,
            method=method,
            scaler=scaler,
            n_clusters=n_clusters,
            min_cluster_size=min_cluster_size,
            cluster_selection_epsilon=cluster_selection_epsilon,
            nmf_k=nmf_k,
            metrics_config=metrics_config,
            path_id_col=path_id_col,
            event_col=event_col,
        )

    def metric_distribution(
        self,
        segment_col: str,
        segment_value,
        metric: dict,
        complement: bool = False,
        path_id_col: str | None = None,
    ) -> dict:
        """Compute histogram/KDE distribution for a metric across one or two segment values.

        Parameters
        ----------
        segment_value:
            Single string → compare with complement (complement=True required).
            List of two strings → compare the two distributions.
        """
        from retentioneering.tools.segment_overview import SegmentOverview

        return SegmentOverview(self).metric_distribution(
            segment_col=segment_col,
            segment_value=segment_value,
            metric=metric,
            complement=complement,
            path_id_col=path_id_col,
        )
