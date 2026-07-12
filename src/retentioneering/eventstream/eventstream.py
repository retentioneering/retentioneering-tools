import inspect
import json
from dataclasses import asdict
from functools import cached_property

import duckdb
import pandas as pd

from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import SchemaConfigError
from retentioneering.tools.types import T_TransitionMatrixValues, T_Diff


def _to_datetime_auto(series: pd.Series) -> pd.Series:
    if pd.api.types.is_integer_dtype(series):
        n = len(str(abs(int(series.iloc[0]))))
        unit = "s" if n <= 10 else "ms" if n <= 13 else "us" if n <= 16 else "ns"
        return pd.to_datetime(series, unit=unit)
    return pd.to_datetime(series)


def _validate_path_cols_nesting(df: pd.DataFrame, path_cols: list) -> None:
    """
    path_cols must be ordered coarsest-first: every value of path_cols[i+1]
    must belong to exactly one value of path_cols[i]. This is what makes it
    safe for any tool to analyze at path_cols[i] and get the same relative
    event order that `index` was computed against (schema.path_col ==
    path_cols[0], the coarsest grain) — see ADR-0004.
    """
    for coarser, finer in zip(path_cols, path_cols[1:]):
        counts = df.groupby(finer, observed=True)[coarser].nunique()
        offenders = counts[counts > 1]
        if not offenders.empty:
            example = offenders.index[0]
            raise SchemaConfigError(
                f"path_cols must be ordered from coarsest to finest grain: "
                f"'{finer}' must nest inside '{coarser}', but {finer}={example!r} "
                f"spans {int(offenders.iloc[0])} different values of '{coarser}'. "
                f"Reorder path_cols (coarsest first) or fix the data."
            )


def _infer_caller_var_name(
    obj: object, default: str = "stream", max_depth: int = 8
) -> str:
    """Best-effort: the name of the variable the caller's frame holds `obj` under.

    Used to generate copy-pasteable code (e.g. `add_clusters` snippets) that refers
    to the eventstream by the name the user actually gave it, instead of a
    hardcoded guess. Walks up the call stack (past the `@_tracked` wrapper and
    similar decorators) looking for a local/global bound to `obj` by identity, and
    falls back to `default` if none is found (e.g. `Eventstream(df).cluster_analysis()`,
    where the call site never bound the object to a variable at all).
    """
    frame = inspect.currentframe()
    try:
        f = frame.f_back if frame else None
        for _ in range(max_depth):
            if f is None:
                break
            for scope in (f.f_locals, f.f_globals):
                for name, value in scope.items():
                    if name in ("self", "cls"):
                        continue
                    if value is obj:
                        return name
            f = f.f_back
        return default
    finally:
        del frame


try:
    from retentioneering._tracking import tracked as _tracked
except Exception:

    def _tracked(event_name, condition=None):  # type: ignore[misc]
        def decorator(func):
            return func

        return decorator


class Eventstream:
    def __init__(
        self,
        df: "pd.DataFrame | str",
        schema: dict | None = None,
        preprocess: bool = True,
    ):
        self._df = df
        self._schema = schema
        self.preprocess = preprocess
        self._post_init()

    @cached_property
    def schema(self) -> EventstreamSchema:
        return EventstreamSchema.from_dict(self._schema)

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    @df.setter
    def df(self, v: pd.DataFrame):
        self._df = v

    @_tracked(
        "eventstream_created",
        condition=lambda self: self.preprocess,
        props_fn=lambda self: {
            "rows": self._df.shape[0],
            "cols": self._df.shape[1],
            "n_path_cols": len(self.schema.path_cols),
            "n_segment_cols": len(self.schema.segment_cols),
            "n_event_cols": len(self.schema.event_cols),
        },
    )
    def _post_init(self):
        if self.preprocess:
            self._preprocess()
        else:
            for col in self.schema.event_cols + self.schema.segment_cols:
                self._df[col] = self._df[col].astype("category")

        schema = self.schema
        declared_cols = set(
            schema.path_cols
            + schema.event_cols
            + [schema.timestamp_col]
            + schema.segment_cols
            + [schema.event_type, schema.index, schema.subindex]
        )

        if self.preprocess and schema.custom_cols is not None:
            # Explicit custom_cols (even []) is a strict declaration: anything
            # else not covered by the schema is dropped, not silently kept.
            missing = [c for c in schema.custom_cols if c not in self._df.columns]
            if missing:
                raise SchemaConfigError(
                    f"custom_cols column(s) not found in the DataFrame: {missing}"
                )
            allowed = declared_cols | set(schema.custom_cols)
            self._df = self._df[[c for c in self._df.columns if c in allowed]]
        else:
            known_cols = declared_cols | set(schema.custom_cols or [])
            extra_cols = [c for c in self._df.columns if c not in known_cols]
            schema.custom_cols = (schema.custom_cols or []) + extra_cols

    def _preprocess(self):
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

        df[schema.timestamp_col] = _to_datetime_auto(df[schema.timestamp_col])

        for col in schema.path_cols:
            if df[col].isna().any():
                raise SchemaConfigError(
                    f"path_cols column '{col}' contains missing values (None/NaN). "
                    f"Every event must belong to a path; drop or fill the missing "
                    f"values in '{col}' before creating the Eventstream."
                )

        if len(schema.path_cols) > 1:
            _validate_path_cols_nesting(df, schema.path_cols)

        for col in schema.event_cols + schema.segment_cols:
            df[col] = df[col].astype("category")

        if schema.event_type not in df.columns:
            df[schema.event_type] = event_types.RAW_EVENT.type
        if schema.subindex not in df.columns:
            df[schema.subindex] = df[schema.event_type].map(event_types.get_order())

        df = df.sort_values(
            [schema.path_col, schema.timestamp_col, schema.subindex]
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

    def is_empty(self, exclude_start_end: bool = True) -> bool:
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

    def get_segment_levels(self) -> dict[str, list[str]]:
        return {
            col: self._df[col].cat.categories.tolist()
            for col in self.schema.segment_cols
        }

    @_tracked("headless_describe")
    def describe(
        self,
        percentiles: tuple = (0.25, 0.5, 0.75, 0.9, 0.99),
        top_events: int = 20,
    ) -> dict:
        """
        Compute basic descriptive statistics for the eventstream.

        A quick sanity-check summary of the dataset: schema, shape, date
        range, event frequency, and per-path-column length/duration
        statistics. Headless only - for interactive per-segment drill-down
        use `segment_overview()` instead.

        Parameters
        ----------
        percentiles : tuple of float, default (0.25, 0.5, 0.75, 0.9, 0.99)
            Percentiles (0-1) reported in `path_stats`.
        top_events : int, default 20
            Number of most frequent events to include in `event_frequency`.

        Returns
        -------
        dict
            - `schema`: event_col, path_col, path_cols, segment_cols, timestamp_col
            - `shape`: n_events, n_paths, n_unique_events
            - `date_range`: min, max, span
            - `event_frequency`: DataFrame of event/count/share, sorted
              descending, limited to `top_events` rows
            - `path_stats`: dict keyed by each entry of `schema.path_cols`,
              each value a `DataFrame` (from `DataFrame.describe`) with
              count/mean/std/min/percentiles/max rows and `length`/`duration`
              columns
            - `segments`: DataFrame of segment_col/value/count/share, one row
              per segment value across all segment columns

        Examples
        --------
            stream.describe()
        """
        from retentioneering.tools.describe import Describe

        return Describe(self).fit(percentiles=percentiles, top_events=top_events)

    @_tracked("dp_filter_events")
    def filter_events(
        self,
        keep: dict | None = None,
        drop: dict | None = None,
        func=None,
        sql: str | None = None,
    ) -> "Eventstream":
        """
        Keep only rows that match a column filter, a Python predicate, or a SQL query.

        Exactly one of `keep`, `drop`, `func`, or `sql` must be provided. If all are
        `None` the eventstream is returned unchanged.

        Parameters
        ----------
        keep : dict, optional
            `{column: values}` mapping. Keeps rows where each listed column contains
            one of the listed values. Multiple columns combine with AND: a row is
            kept only if it matches every entry.
            Example: `{"event": ["purchase", "add_to_cart"]}`.
        drop : dict, optional
            Same `{column: values}` format, but removes the matching rows instead.
            Multiple columns combine with OR: a row is removed if it matches any
            entry (the exact complement of `keep`).
        func : callable, optional
            A function that accepts the raw pandas DataFrame and returns a boolean Series.
            Rows where the Series is `True` are kept.
        sql : str, optional
            DuckDB SQL SELECT statement that reads from the `eventstream` table alias and
            returns all original columns. Example: `"SELECT * FROM eventstream WHERE event NOT LIKE 'system_%'"`.

        Examples
        --------
            stream.filter_events(keep={"event": ["purchase", "add_to_cart"]})
            stream.filter_events(drop={"event": ["system_event"], "platform": ["bot"]})
            stream.filter_events(sql="SELECT * FROM eventstream WHERE event NOT LIKE 'system_%'")
        """
        from retentioneering.data_processors.filter_events import FilterEvents

        if keep is None and drop is None and func is None and sql is None:
            return Eventstream(self._df.copy(), asdict(self.schema), preprocess=False)
        new_df, new_schema = FilterEvents(
            keep=keep, drop=drop, func=func, sql=sql
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_add_clusters")
    def add_clusters(
        self,
        name: str,
        features: list,
        method: str = "kmeans",
        scaler=None,
        n_clusters=None,
        min_cluster_size=None,
        cluster_selection_epsilon=None,
        nmf_components=None,
        path_col=None,
        event_col=None,
    ) -> "Eventstream":
        """
        Cluster paths using ML and add a new segment column with `cluster_0`, `cluster_1`,
        etc. cluster labels.

        Per-path metrics are computed from `features`, optionally scaled, then passed to
        the chosen clustering algorithm. The resulting cluster label is broadcast to every
        row of the corresponding path.

        Parameters
        ----------
        name : str
            Name of the new segment column to add.
        features : list of dict
            Metric configurations used as clustering features. Each dict has a
            `"metric"` key (str) and an optional `"metric_args"` key (dict).
            Available metrics: `"length"`, `"duration"`, `"event_count"`,
            `"has_event"`, `"event_count_bulk"`, `"has_event_bulk"`,
            `"has_all_events"`, `"has_any_event"`, `"time_between"`,
            `"first_event_time"`, `"active_days"`, `"matches_pattern"`,
            `"in_segment"`. See the Path Metrics documentation page for the full
            metric reference.
        method : str, default `"kmeans"`
            Clustering algorithm. One of `"kmeans"` or `"hdbscan"`.
        scaler : str or None, default `None`
            Feature scaler applied before clustering. One of `"minmax"`, `"standard"`, or `None`.
        n_clusters : int, optional
            Number of clusters (required for `"kmeans"`).
        min_cluster_size : int, optional
            Minimum cluster size (used by `"hdbscan"`).
        cluster_selection_epsilon : float, optional
            HDBSCAN cluster-selection epsilon.
        nmf_components : int, optional
            When set, reduces features to this many NMF components before clustering.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            stream.add_clusters(
                name="cluster",
                features=[
                    {"metric": "length"},
                    {"metric": "event_count", "metric_args": {"event": "purchase"}},
                ],
                method="kmeans",
                n_clusters=4,
                scaler="minmax",
            )
        """
        from retentioneering.data_processors.add_clusters import AddClusters

        new_df, new_schema = AddClusters(
            eventstream=self,
            name=name,
            features=features,
            method=method,
            scaler=scaler,
            n_clusters=n_clusters,
            min_cluster_size=min_cluster_size,
            cluster_selection_epsilon=cluster_selection_epsilon,
            nmf_components=nmf_components,
            path_col=path_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_urls_to_events")
    def urls_to_events(
        self,
        column: str,
        nodes: list,
        strip_host: bool = True,
        strip_query: bool = True,
        strip_locale: bool = True,
        keep_full_paths: bool = False,
        host_col=None,
        query_col=None,
        locale_col=None,
        slug_col=None,
    ) -> "Eventstream":
        """
        Turn a raw URL column into structured event names using a URL path tree.

        Each URL is matched against the `nodes` tree. A node with
        `aggregate_children` set becomes an aggregation point: the node's own URL
        keeps its path as the event name, while deeper pages are collapsed into a
        `<path>/<slug>` label. The original URL column is replaced in-place.

        Parameters
        ----------
        column : str
            Name of the column that contains raw URL strings.
        nodes : list of dict
            URL path tree. Each node dict must have a `"path"` key (str) and may
            include:
              - `"aggregate_children"` (bool) — collapse pages deeper than this node
                into a `<path>/<slug>` label.
              - `"exclude"` (bool) — drop rows whose URL falls under this node.
              - `"name"` (str) — custom label for this node (also used as the slug
                when a parent node aggregates it).
        strip_host : bool, default `True`
            Remove the scheme and hostname, keeping only the pathname.
        strip_query : bool, default `True`
            Remove the query string and URL fragment.
        strip_locale : bool, default `True`
            Remove a leading 2-letter BCP-47 locale segment (e.g. `"en"`, `"fr-ca"`).
        keep_full_paths : bool, default `False`
            When `True`, `aggregate_children` nodes are ignored and every URL keeps
            its normalized path.
        host_col : str, optional
            If provided, save the extracted hostname into this new column.
        query_col : str, optional
            If provided, save the extracted query string into this new column.
        locale_col : str, optional
            If provided, save the detected locale prefix into this new column.
        slug_col : str, optional
            If provided, save the sub-page slug into this new column.

        Examples
        --------
            stream.urls_to_events(
                column="page",
                nodes=[
                    {"path": "/catalog", "aggregate_children": True},
                    {"path": "/checkout", "aggregate_children": True, "name": "checkout"},
                ],
            )
        """
        from retentioneering.data_processors.urls_to_events import UrlsToEvents

        new_df, new_schema = UrlsToEvents(
            column=column,
            nodes=nodes,
            strip_host=strip_host,
            strip_query=strip_query,
            strip_locale=strip_locale,
            keep_full_paths=keep_full_paths,
            host_col=host_col,
            query_col=query_col,
            locale_col=locale_col,
            slug_col=slug_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_filter_paths")
    def filter_paths(
        self,
        condition: dict | list,
        path_col: str | None = None,
        event_col: str | None = None,
    ) -> "Eventstream":
        """
        Keep only paths that satisfy a metric condition.

        The condition is a tree of comparison nodes connected by `and` / `or` / `not`
        branch nodes. Per-path metrics are computed once and the condition is evaluated
        in SQL.

        Raises `EmptyEventstreamError` when no paths match.

        Parameters
        ----------
        condition : dict or list
            Condition tree. Leaf nodes have the keys:
              - `op` — comparison operator: `>`, `>=`, `<`, `<=`, `=` (or `==`), `!=`.
              - `metric` — metric name (see the Path Metrics documentation page for
                the full list).
              - `value` — threshold value.
              - `metric_args` (optional) — dict of extra arguments for the metric.
            Branch nodes have `op` set to `and`, `or`, or `not` and an `args` list of
            child nodes.
            A plain list of nodes is shorthand for AND:
            `[cond1, cond2]` ≡ `{"op": "and", "args": [cond1, cond2]}`.
            `has_event`/`event_count` take a single `event` (string) — for a
            multi-event AND/OR condition use `has_all_events`/`has_any_event` with
            an `events` list instead. `has_event_bulk`/`event_count_bulk` (which
            expand into one column per event, for `segment_overview`/`add_clusters`)
            cannot be used here since a condition needs exactly one value per path.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            # Keep paths that contain at least one purchase
            stream.filter_paths({"op": ">", "metric": "event_count", "value": 0, "metric_args": {"event": "purchase"}})

            # Keep paths that contain a promo_view or a discount_applied event
            stream.filter_paths({"op": "=", "metric": "has_any_event", "value": True,
                                  "metric_args": {"events": ["promo_view", "discount_applied"]}})

            # Keep paths longer than 3 events that match a funnel pattern
            # (a top-level list means AND)
            stream.filter_paths([
                {"op": ">", "metric": "length", "value": 3},
                {"op": "=", "metric": "matches_pattern", "value": True,
                 "metric_args": {"pattern": "registration->.*->purchase"}},
            ])
        """
        from retentioneering.data_processors.filter_paths import FilterPaths
        from retentioneering.exceptions import EmptyEventstreamError

        if isinstance(condition, list):
            condition = {"op": "and", "args": condition}

        dp = FilterPaths(condition, path_col, event_col)
        path_col = path_col or self.schema.path_col

        # Extract metric configs
        metric_configs = dp._get_metric_configs(condition)

        # Build metrics
        metrics = self.get_metrics(  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
            metric_configs, path_col=path_col
        ).reset_index()
        where_condition = dp._get_where_condition(condition)
        query = f"SELECT {path_col} FROM metrics WHERE {where_condition}"
        path_ids = duckdb.sql(query).df()[path_col].tolist()

        if len(path_ids) == 0:
            raise EmptyEventstreamError("no paths match the filter_paths condition")

        result_stream = self.filter_events(keep={path_col: path_ids})
        if result_stream.is_empty():
            raise EmptyEventstreamError("no events remain after filter_paths")
        return result_stream

    def get_metrics(self, metrics: list, path_col: str | None = None) -> pd.DataFrame:
        """
        Compute per-path metric values.

        Args:
            metrics: List of metric configuration dicts with 'metric' and optional
                'metric_args' fields. See the Path Metrics documentation page for
                the full metric reference.
            path_col: Path ID column (if None, taken from schema)

        Returns:
            DataFrame with path IDs as index and metrics as columns
        """
        from retentioneering.metrics.metric_builder import MetricBuilder

        builder = MetricBuilder(self)
        return builder.build_metrics(metrics, path_col)

    @_tracked("dp_add_events")
    def add_events(
        self, name: str, source_events=None, sql=None, churn=None
    ) -> "Eventstream":
        """
        Insert synthetic events derived from existing events or a SQL query.
        In `source_events` mode, the new event is inserted at the timestamp of the
        **first** occurrence of a source event in each path.

        Exactly one of `source_events`, `sql`, or `churn` must be provided.
        The new event rows are appended to the eventstream; original rows are kept.

        Parameters
        ----------
        name : str
            Name of the synthetic event to create.
        source_events : list of str, optional
            List of existing event names. For each path, one synthetic event is
            inserted at the timestamp of the first matching source event.
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
            name, source_events=source_events, sql=sql, churn=churn
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_add_segment")
    def add_segment(
        self,
        name: str,
        rules=None,
        func=None,
        sql=None,
        funnel_events=None,
        path_col=None,
    ) -> "Eventstream":
        """
        Add a new categorical segment column to the eventstream.

        Exactly one of `rules`, `func`, `sql`, or `funnel_events` must be provided —
        unless `name` is already listed in `schema.custom_cols`, in which case
        passing none of them promotes that existing column to a segment in
        place, without recomputing its values.

        Parameters
        ----------
        name : str
            Name of the new segment column.
        rules : list, optional
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
            Ordered list of at least 2 event names defining a strict, ordered
            ("closed") funnel. A path is assigned `funnel_events[k]` only if it
            contains every event `funnel_events[0]` through `funnel_events[k]`
            *and* their last occurrences appear in that same order — reaching a
            later step without having completed the earlier ones in order does
            not count towards it. A path is assigned the highest such `k`; if it
            never completes even `funnel_events[0]`, it is labeled
            `out_of_funnel`.
            Segment values (in ascending funnel order): `out_of_funnel`, then each
            event name from `funnel_events[0]` to `funnel_events[-1]`.
        path_col : str, optional
            Path ID column override for `funnel_events` mode; defaults to
            `schema.path_col`.

        Examples
        --------
            # rules mode — CASE WHEN rules
            stream.add_segment(
                "region",
                rules=[
                    ["country", "=", "US", "domestic"],
                    ["country", "in", "('GB', 'DE', 'FR')", "europe"],
                    ["other"],
                ],
            )

            # funnel_events mode — assign the deepest funnel step reached in order
            stream.add_segment(
                "funnel",
                funnel_events=["add_to_cart", "checkout_start", "purchase"],
            )
            # Resulting segment values: out_of_funnel | add_to_cart | checkout_start | purchase
            # A path with only "checkout_start" (no "add_to_cart") is out_of_funnel — the
            # funnel is strictly ordered, so skipping or reordering an earlier step keeps
            # a path from being credited for a later one it did reach.

            # sql mode — one computed column, same row order as the eventstream
            stream.add_segment(
                "device",
                sql="SELECT CASE WHEN platform = 'mobile' THEN 'mobile' ELSE 'web' END FROM eventstream",
            )

            # promoting an existing custom column — "returned" already rode along in
            # the source DataFrame and landed in schema.custom_cols; no mode argument
            # needed, the column's values are kept as-is
            stream.add_segment("returned")
        """
        from retentioneering.data_processors.add_segment import AddSegment

        new_df, new_schema = AddSegment(
            name,
            rules=rules,
            func=func,
            sql=sql,
            funnel_events=funnel_events,
            path_col=path_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_collapse_events")
    def collapse_events(
        self,
        consecutive=None,
        event_groups=None,
        group_col=None,
        session_col=None,
        session_type_col=None,
        agg=None,
        path_col=None,
        event_col=None,
    ) -> "Eventstream":
        """
        Merge consecutive or grouped events into a single representative event.

        Exactly one of `consecutive`, `event_groups`, `group_col`, or
        `session_col` must be provided.

        Parameters
        ----------
        consecutive : bool or list of str, optional
            Collapse consecutive repeats of the same event into one.
            Pass `True` to collapse all events; pass a list of event names to collapse
            only those specific events.
        event_groups : list of dict, optional
            Merge a chain of events into a single representative event.
              - `events` (str or list of str) — collapse any run of these events, wherever it occurs in the path, into one group.
              - `separator` (str or list of str) — collapse every event up to and including the next separator event into one group.
              - `start_event` + `end_event` (str or list of str) — collapse every event between a `start_event` and the next `end_event`, inclusive of both, into one group.
              - `name` (str) — label for the merged event, required unless `cases` are given.
              - `cases` (list of dict, optional) — conditional labels evaluated against the group's own events, falling back to `name` for groups no case matched.
            See [event_groups](/docs/data-processors/collapse-events#event_groups)
            below for worked examples.
        group_col : str, optional
            Group consecutive rows by this column's value: each run of rows sharing
            the same value is collapsed into one event named after that value.
            Example: a `session_type` column with values `browse, browse, search`
            collapses the path into `browse -> search` events.
        session_col : str, optional
            Collapse events within each session defined by this column. Requires
            `session_type_col` as well.
        session_type_col : str, optional
            Column that distinguishes session event types (used with `session_col`).
        agg : dict, optional
            Aggregation rules for non-event columns when rows are merged, as a
            `{column: agg_func}` dict. `agg_func` is one of `"first"` (default),
            `"last"`, `"min"`, `"max"`, `"mean"`, `"mode"`, `"any"`. See
            [agg](/docs/data-processors/collapse-events#agg) below. Example:
            `{"price": "max"}`.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            # Collapse any run of the same event
            stream.collapse_events(consecutive=True)

            # Collapse only repeated page_view events
            stream.collapse_events(consecutive=["page_view"])

            # Merge checkout steps into a single "checkout" event
            stream.collapse_events(event_groups=[{"events": ["checkout_start", "checkout_step", "checkout_confirm"], "name": "checkout"}])
        """
        from retentioneering.data_processors.collapse_events import CollapseEvents

        new_df, new_schema = CollapseEvents(
            consecutive=consecutive,
            event_groups=event_groups,
            group_col=group_col,
            session_col=session_col,
            session_type_col=session_type_col,
            agg=agg,
            path_col=path_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_to_daily_states")
    def to_daily_states(
        self,
        active_events=None,
        max_dormant_days: int = 30,
        agg=None,
        path_col=None,
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
        path_col : str, optional
            Override the path ID column.
        event_col : str, optional
            Override the event column.

        Examples
        --------
            stream.to_daily_states()
            stream.to_daily_states(active_events=["purchase", "add_to_cart"], max_dormant_days=60)

        As a preprocessor in MCP update_base_stream / local_preprocessors:
            {"type": "to_daily_states"}
            {"type": "to_daily_states", "active_events": ["purchase"], "max_dormant_days": 60}
        """
        from retentioneering.data_processors.to_daily_states import ToDailyStates

        new_df, new_schema = ToDailyStates(
            active_events=active_events,
            max_dormant_days=max_dormant_days,
            agg=agg,
            path_col=path_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

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
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_edit_events")
    def edit_events(self, rename=None, delete=None) -> "Eventstream":
        """
        Rename and/or delete events in a single operation.

        A convenience combination of `rename_events` and `drop_events` — useful when
        one pass over the unique event list should both clean up names and remove
        noise. At least one of `rename` or `delete` must be provided.

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
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

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
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_rename_segment_levels")
    def rename_segment_levels(self, segment_col: str, mapping: dict) -> "Eventstream":
        """
        Rename levels of a segment column using a mapping dict.

        Levels not present in `mapping` are left unchanged. Useful for cleaning up
        raw segment data, or for giving a clustering result (e.g. from `add_clusters`)
        human-readable names. Renaming a level to match another existing level merges
        the two.

        Parameters
        ----------
        segment_col : str
            Name of the segment column. Must be listed in `schema.segment_cols`.
        mapping : dict
            Mapping of `{old_level: new_level}`. Keys must be levels already present
            in `segment_col` (see `get_segment_levels`).

        Examples
        --------
            stream.add_clusters(
                name="cluster", features=[{"metric": "length"}], n_clusters=3
            ).rename_segment_levels(
                "cluster", {"cluster_0": "buyers", "cluster_1": "browsers"}
            )
        """
        from retentioneering.data_processors.rename_segment_levels import (
            RenameSegmentLevels,
        )

        new_df, new_schema = RenameSegmentLevels(segment_col, mapping).apply(
            self._df, self.schema
        )
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_drop_events")
    def drop_events(self, names: list) -> "Eventstream":
        """
        Remove events from the eventstream by name.

        Raises an error if any listed event does not exist.

        Parameters
        ----------
        names : list of str
            Event names to remove entirely.

        Examples
        --------
            stream.drop_events(["debug_event", "system_ping"])
        """
        from retentioneering.data_processors.edit_events import EditEvents

        new_df, new_schema = EditEvents(delete=names).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_sample_paths")
    def sample_paths(
        self, n=None, frac=None, random_state=None, path_col=None
    ) -> "Eventstream":
        """
        Randomly sample paths (and all their events).

        Exactly one of `n` or `frac` must be provided (mirrors
        `pandas.DataFrame.sample`).

        Parameters
        ----------
        n : int, optional
            Number of paths to keep.
        frac : float, optional
            Fraction of total paths to keep, in the range `(0.0, 1.0]`.
            Passing `1.0` returns the eventstream unchanged.
        random_state : int, optional
            Seed for the random number generator; pass an integer for reproducible results.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

        Examples
        --------
            stream.sample_paths(n=1000)
            stream.sample_paths(frac=0.1, random_state=42)  # 10 % of paths
        """
        from retentioneering.data_processors.sample_paths import SamplePaths

        new_df, new_schema = SamplePaths(
            n=n, frac=frac, random_state=random_state, path_col=path_col
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_split_sessions")
    def split_sessions(
        self,
        session_col="session_id",
        session_index_col="session_index",
        separator=None,
        start_event=None,
        end_event=None,
        timeout=None,
        path_col=None,
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
            session; the separator row itself is dropped from the output.
        start_event : str or list of str, optional
            Event name(s) that mark the start of a session. Must be provided together
            with `end_event`.
        end_event : str or list of str, optional
            Event name(s) that mark the end of a session. Must be provided together
            with `start_event`.
        timeout : str or pandas.Timedelta, optional
            Inactivity gap after which a new session starts, as a pandas-style
            duration string with an explicit unit — e.g. `"30m"`, `"1h"`,
            `"1800s"` — or a `pandas.Timedelta`. Bare numbers are rejected to
            avoid unit ambiguity.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            stream.split_sessions(timeout="30m")
            stream.split_sessions(separator="app_open")
            stream.split_sessions(start_event="session_start", end_event="session_end")
            stream.split_sessions(separator="app_open", timeout="1h")
        """
        from retentioneering.data_processors.split_sessions import SplitSessions

        new_df, new_schema = SplitSessions(
            session_col=session_col,
            session_index_col=session_index_col,
            separator=separator,
            start_event=start_event,
            end_event=end_event,
            timeout=timeout,
            path_col=path_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_truncate_paths")
    def truncate_paths(
        self, start_event: str, end_event: str, path_col=None, event_col=None
    ) -> "Eventstream":
        """
        Trim each path to the window between two anchor events (inclusive).

        For each path, the first occurrence of `start_event` and the first occurrence
        of `end_event` that comes after it are found. Events outside this window are
        dropped. Paths that do not contain both anchors in the correct order are
        removed entirely.

        Use `start_event="path_start"` / `end_event="path_end"` to refer to the actual
        first and last events of the path.

        Parameters
        ----------
        start_event : str
            Name of the event that marks the start of the window.
        end_event : str
            Name of the event that marks the end of the window.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            stream.truncate_paths(start_event="registration", end_event="purchase")
            stream.truncate_paths(start_event="registration", end_event="path_end")
        """
        from retentioneering.data_processors.truncate_paths import TruncatePaths

        new_df, new_schema = TruncatePaths(
            start_event=start_event,
            end_event=end_event,
            path_col=path_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    def _split_two(self, split, path_col: str | None = None):
        from retentioneering.exceptions import EmptyEventstreamError, DiffConfigError

        if len(split) == 3:
            segment_col, v1, v2 = split[0], split[1], split[2]
            if segment_col not in self.schema.segment_cols:
                raise DiffConfigError(f"'{segment_col}' is not a segment column")
            s1 = self.filter_events(keep={segment_col: [v1]})
            if v2 == "<REST>":
                all_vals = set(self.get_segment_levels().get(segment_col, []))
                v2_vals = list(all_vals - {v1})
                if not v2_vals:
                    raise DiffConfigError(
                        f"'{segment_col}' has no other values besides '{v1}'; "
                        "'<REST>' requires at least one complementary value."
                    )
            else:
                v2_vals = [v2]
            s2 = self.filter_events(keep={segment_col: v2_vals})
        elif len(split) == 2:
            ids1, ids2 = split[0], split[1]
            path_col = path_col or self.schema.path_col
            s1 = self.filter_events(keep={path_col: list(ids1)})
            s2 = self.filter_events(keep={path_col: list(ids2)})
        else:
            raise DiffConfigError("diff must be (seg, v1, v2) or (ids1, ids2)")
        if s1.is_empty():
            raise EmptyEventstreamError("first diff group is empty")
        if s2.is_empty():
            raise EmptyEventstreamError("second diff group is empty")
        return s1, s2

    @_tracked("dp_add_start_end_events")
    def add_start_end_events(self, path_col: str | None = None) -> "Eventstream":
        """
        Prepend a `path_start` and append a `path_end` synthetic event to each path.

        Idempotent: a path that already starts or ends with these events is left
        unchanged on that side.

        You normally don't need to call this directly — `transition_graph`,
        `step_matrix`, and `step_sankey` insert `path_start`/`path_end` themselves,
        each using its own `path_col`. Calling this upfront bakes in one
        specific path definition and can produce misleading boundaries if a
        widget is later given a different `path_col`.

        Parameters
        ----------
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

        Examples
        --------
            stream.add_start_end_events()
        """
        from retentioneering.data_processors.add_start_end_events import (
            AddStartEndEvents,
        )

        dp = AddStartEndEvents(path_col)
        new_df, new_schema = dp.apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("headless_transition_graph")
    def transition_graph_data(
        self,
        edge_weight: T_TransitionMatrixValues = "proba_out",
        path_col: str | None = None,
        diff: T_Diff = None,
    ) -> pd.DataFrame:
        """
        Compute the transition **matrix** between events (headless): an
        events x events DataFrame where cell `[source, target]` holds the selected
        `edge_weight` for the `source -> target` transition. This is the data
        behind the `transition_graph` widget.

        Parameters
        ----------
        edge_weight : {"proba_out", "proba_in", "count", "unique_paths", "share_of_total", "avg_per_path", "time_median", "time_q95"}, default "proba_out"
            Value to compute for each source -> target pair:
              - `"proba_out"` — probability of the transition among all transitions out of the source event.
              - `"proba_in"` — probability of the transition among all transitions into the target event.
              - `"count"` — number of times the transition occurred.
              - `"unique_paths"` — number of distinct paths containing the transition.
              - `"share_of_total"` — share of this transition among all transitions in the eventstream.
              - `"avg_per_path"` — average number of occurrences per path.
              - `"time_median"` / `"time_q95"` — median / 95th-percentile time between the two events (in seconds).
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        diff : tuple or list, optional
            Draws a comparative chart for a pair of segments; see
            [Diff mode](/docs/widgets#diff-mode). `(segment_col, value1, value2)` to
            compare two segment values, or `(path_ids1, path_ids2)` to compare two
            explicit path-id groups. `value2` may be `<REST>`, meaning "every other
            value of `segment_col`".

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

        return TransitionMatrix(self).fit(edge_weight, diff, path_col)

    @_tracked("headless_step_sankey")
    def step_sankey_data(
        self,
        max_steps: int = 10,
        diff: T_Diff = None,
        path_col: str | None = None,
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
        diff : tuple or list, optional
            Draws a comparative chart for a pair of segments; see
            [Diff mode](/docs/widgets#diff-mode). `(segment_col, value1, value2)` or
            `(path_ids1, path_ids2)`; `value2` may be `<REST>`. See
            `transition_graph_data` for the shared diff semantics.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        path_pattern : str, optional
            Restrict/split paths using a `"->"`-separated sequence of anchor
            events, where `.*` matches any run of events, e.g.
            `"add_to_cart->.*->purchase"`. Without a pattern, computes over
            the whole path from `path_start` to `path_end`. Each anchor event in
            the pattern produces its own matrix block. To see the
            neighborhood around a single event: `path_pattern="add_to_cart"`.

        Returns
        -------
        pd.DataFrame or tuple of pd.DataFrame
            Without `path_pattern`: a single DataFrame (or, in diff mode,
            `(combined, group1, group2)` — three DataFrames). With
            `path_pattern`: one DataFrame per anchor block, as a tuple (or,
            in diff mode, `(combined_blocks, group1_blocks, group2_blocks)`,
            each itself a tuple of per-block DataFrames) — a pattern with
            several anchor events produces several blocks.

        Examples
        --------
            df = stream.step_sankey_data(max_steps=10)
            combined, g1, g2 = stream.step_sankey_data(diff=("plan", "pro", "free"))
            blocks = stream.step_sankey_data(path_pattern="add_to_cart->.*->purchase")
        """
        from retentioneering.tools.step_matrix import StepMatrix

        result = StepMatrix(self).fit(
            max_steps=max_steps,
            diff=diff,
            path_col=path_col,
            path_pattern=path_pattern,
        )
        if path_pattern is not None:
            return result
        if diff is None:
            (sm,) = result
            return sm
        combined, group1, group2 = result
        return combined[0], group1[0], group2[0]

    @_tracked("headless_step_matrix")
    def step_matrix_data(
        self,
        max_steps: int = 10,
        diff: T_Diff = None,
        path_col: str | None = None,
        path_pattern: str | None = None,
    ):
        """
        Alias for `step_sankey_data` — Step Matrix and Step Sankey render the
        same underlying per-step data, so both widgets share one headless method.

        Parameters
        ----------
        max_steps : int, default 10
            Number of path steps to compute (on each side of an anchor, when
            `path_pattern` is given).
        diff : tuple or list, optional
            Draws a comparative chart for a pair of segments; see
            [Diff mode](/docs/widgets#diff-mode). `(segment_col, value1, value2)` or
            `(path_ids1, path_ids2)`; `value2` may be `<REST>`. See
            `transition_graph_data` for the shared diff semantics.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        path_pattern : str, optional
            Restrict/split paths using a `"->"`-separated sequence of anchor
            events, where `.*` matches any run of events, e.g.
            `"add_to_cart->.*->purchase"`. Without a pattern, computes over
            the whole path from `path_start` to `path_end`. Each anchor event in
            the pattern produces its own matrix block. To see the
            neighborhood around a single event: `path_pattern="add_to_cart"`.

        Returns
        -------
        pd.DataFrame or tuple of pd.DataFrame
            Without `path_pattern`: a single DataFrame (or, in diff mode,
            `(combined, group1, group2)` — three DataFrames). With
            `path_pattern`: one DataFrame per anchor block, as a tuple (or,
            in diff mode, `(combined_blocks, group1_blocks, group2_blocks)`,
            each itself a tuple of per-block DataFrames) — a pattern with
            several anchor events produces several blocks.

        See Also
        --------
        step_sankey_data : Same computation; this method is a plain alias.
        """
        return self.step_sankey_data(
            max_steps=max_steps,
            diff=diff,
            path_col=path_col,
            path_pattern=path_pattern,
        )

    @_tracked("widget_step_sankey")
    def step_sankey(
        self,
        max_steps=None,
        diff=None,
        path_col=None,
        path_pattern=None,
        step_window=None,
        height=None,
        sidebar_open=None,
        state_file=None,
    ):
        """
        Displays a step-by-step Sankey diagram showing which events users experience
        at each ordinal position in their path. The horizontal axis represents step
        number, making it easy to see how paths diverge over time.

        Each column sums to 1 in standard mode, while in diff mode columns sum to 0.

        Step Sankey and [Step Matrix](/docs/widgets/step-matrix) visualise the same
        underlying data in different forms: Step Sankey as a flow diagram, Step Matrix
        as a heatmap table. Use whichever makes the pattern you are looking for easier to spot.

        Parameters
        ----------
        max_steps : int, default 10
            Number of path steps to compute.
        step_window : int, default 3
            Number of step columns shown around each anchor.
        diff : tuple or list, optional
            Draws a comparative chart for a pair of segments; see
            [Diff mode](/docs/widgets#diff-mode). `(segment_col, value1, value2)` or
            `(path_ids1, path_ids2)`; `value2` may be `<REST>`.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        path_pattern : str, optional
            Same syntax as `step_matrix`'s `path_pattern`.
        height : int, default 500
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.
        state_file : str, optional
            JSON file the widget state is bound to; see
            [Saving widget state](/docs/widgets#saving-widget-state).

        Examples
        --------
            stream.step_sankey(max_steps=15, path_pattern="add_to_cart->.*->purchase")
            stream.step_sankey(diff=("country", "US", "<REST>"))
        """
        from retentioneering.widgets.step_sankey import StepSankeyWidget, _UNSET

        return StepSankeyWidget(
            eventstream=self,
            max_steps=max_steps if max_steps is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            path_pattern=path_pattern if path_pattern is not None else _UNSET,
            step_window=step_window if step_window is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
            state_file=state_file,
        )

    @_tracked("widget_step_matrix")
    def step_matrix(
        self,
        max_steps=None,
        diff=None,
        path_col=None,
        path_pattern=None,
        step_window=None,
        height=None,
        sidebar_open=None,
        state_file=None,
    ):
        """
        Displays a heatmap table of step-by-step transition probabilities. Each cell
        shows the share of paths that pass through a given event at a given step
        relative to an anchor. The horizontal axis represents step offset from the anchor
        (negative steps are before it, positive are after), and the vertical axis lists the events.

        Each column sums to 1 in standard mode, while in diff mode columns sum to 0.

        Step Matrix and [Step Sankey](/docs/widgets/step-sankey) visualise the same underlying
        data in different forms: Step Matrix as a heatmap table, Step Sankey as a flow diagram.
        Use whichever makes the pattern you are looking for easier to spot.

        Parameters
        ----------
        max_steps : int, default 10
            Number of path steps to compute on each side of the anchor.
        diff : tuple or list, optional
            Draws a comparative chart for a pair of segments; see
            [Diff mode](/docs/widgets#diff-mode). `(segment_col, value1, value2)` or
            `(path_ids1, path_ids2)`; `value2` may be `<REST>`.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        path_pattern : str, optional
            Restrict/split paths using a `"->"`-separated sequence of anchor
            events, where `.*` matches any run of events, e.g.
            `"add_to_cart->.*->purchase"`. Without a pattern, shows the
            whole path from `path_start` to `path_end`. Multiple anchors render
            one matrix block per anchor, side by side. A pattern that doesn't
            start at `path_start` or end at `path_end` shows a serrated edge,
            signalling paths continue beyond the visible range. To see the
            neighborhood around a single event: `path_pattern="add_to_cart"`.
        step_window : int, default 3
            Number of step columns shown around each anchor.
        height : int, default 600
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.
        state_file : str, optional
            JSON file the widget state is bound to; see
            [Saving widget state](/docs/widgets#saving-widget-state).

        Examples
        --------
            stream.step_matrix(path_pattern="purchase")
            stream.step_matrix(path_pattern="add_to_cart->.*->purchase")
            stream.step_matrix(diff=("is_new_user", False, True))
        """
        from retentioneering.widgets.step_matrix import StepMatrixWidget, _UNSET

        return StepMatrixWidget(
            eventstream=self,
            max_steps=max_steps if max_steps is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            path_pattern=path_pattern if path_pattern is not None else _UNSET,
            step_window=step_window if step_window is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
            state_file=state_file,
        )

    @_tracked("widget_transition_graph")
    def transition_graph(
        self,
        edge_weight=None,
        diff=None,
        path_col=None,
        height=None,
        sidebar_open=None,
        state_file=None,
    ):
        """
        Displays an interactive directed graph where nodes are unique events and edges represent transitions
        between them. Edge weights can show transition probabilities, counts, or time-based metrics.
        Supports diff mode to compare two user segments side by side.

        Parameters
        ----------
        edge_weight : {"proba_out", "proba_in", "count", "unique_paths", "share_of_total", "avg_per_path", "time_median", "time_q95"}, default "proba_out"
            Value shown on edges. See the [Edge Weights](/docs/widgets/transition-graph#edge-weights) section for more details.
        diff : tuple or list, optional
            Draws a comparative chart for a pair of segments; see
            [Diff mode](/docs/widgets#diff-mode). `(segment_col, value1, value2)` or
            `(path_ids1, path_ids2)`; `value2` may be `<REST>`, meaning "every other
            value of `segment_col`".
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        height : int, default 500
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.
        state_file : str, optional
            JSON file the widget state is bound to; see
            [Saving widget state](/docs/widgets#saving-widget-state).

        Examples
        --------
            stream.transition_graph()
            stream.transition_graph(edge_weight="count", diff=("plan", "pro", "free"))
            stream.transition_graph(state_file="checkout_graph.json")
        """
        from retentioneering.widgets.transition_graph import (
            TransitionGraphWidget,
            _UNSET,
        )

        return TransitionGraphWidget(
            eventstream=self,
            edge_weight=edge_weight if edge_weight is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
            state_file=state_file,
        )

    @_tracked("widget_funnel")
    def funnel(
        self,
        steps: list[str] | None = None,
        diff=None,
        path_col: str | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
        state_file: str | None = None,
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
        diff : tuple or list, optional
            Draws a comparative chart for a pair of segments; see
            [Diff mode](/docs/widgets#diff-mode). `(segment_col, value1, value2)` or
            `(path_ids1, path_ids2)`; `value2` may be `<REST>`.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        height : int, default 420
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.
        state_file : str, optional
            JSON file the widget state is bound to; see
            [Saving widget state](/docs/widgets#saving-widget-state).

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
            path_col=path_col if path_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
            state_file=state_file,
        )

    @_tracked("headless_funnel")
    def funnel_data(
        self,
        steps: list[str] | None = None,
        diff=None,
        path_col: str | None = None,
    ) -> dict:
        """
        Compute funnel conversion metrics and return a dict (headless).

        Parameters
        ----------
        steps : list of str, optional
            Ordered event names defining the funnel steps.
        diff : tuple or list, optional
            Draws a comparative chart for a pair of segments; see
            [Diff mode](/docs/widgets#diff-mode). `(segment_col, value1, value2)` or
            `(path_ids1, path_ids2)`; `value2` may be `<REST>`.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

        Returns
        -------
        dict with key "steps", a list of per-step dicts with:

        - `step` — event name.
        - `unique_paths` — number of paths reaching this step.
        - `conversion_rate` — `unique_paths` as a share of **all paths in the
          eventstream**, including paths that never entered the funnel.
        - `step_conversion_rate` — `unique_paths` as a share of the
          **previous step's** `unique_paths`, i.e. the step-to-step
          conversion. Equals `conversion_rate` for the first step, since
          there is no previous step to divide by.

        When `diff` is given, each of the four keys above is split into
        `funnel1_*` / `funnel2_*` (one per segment) and `delta_*`
        (`funnel1_* - funnel2_*`) instead.
        """
        from retentioneering.tools.funnel import Funnel

        if not steps:
            return {"steps": []}
        return Funnel(self).fit(steps=steps, diff=diff, path_col=path_col)

    @_tracked("widget_segment_overview")
    def segment_overview(
        self,
        segment_col: str | None = None,
        metrics: list | None = None,
        path_col: str | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
        state_file: str | None = None,
    ):
        """
        Interactive segment comparison heatmap for Jupyter notebooks.

        Rows are metrics, columns are segment values. Click a cell to see that
        metric's distribution for the segment; shift-click a second cell in the
        same row to compare two distributions side by side. `segment_col` and
        `metrics` are also editable from the widget's sidebar without
        re-running the cell.

        Parameters
        ----------
        segment_col : str, optional
            Segment column to split by; must be one of `schema.segment_cols`.
            Required (directly or via the sidebar) before the widget computes
            anything.
        metrics : list of dict, optional
            Metric configurations, each with a `"metric"` key, optional
            `"metric_args"`, and an `"agg"` key (`"mean"`, `"median"`, `"q5"`,
            `"q25"`, `"q75"`, `"q95"`, or `"complement_distance"`) controlling how
            per-path values roll up across a segment. See the Path Metrics
            documentation page for the metric reference.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        height : int, default 480
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.
        state_file : str, optional
            JSON file the widget state is bound to; see
            [Saving widget state](/docs/widgets#saving-widget-state).

        Examples
        --------
            stream.segment_overview(
                segment_col="plan",
                metrics=[
                    {"metric": "length", "agg": "mean"},
                    {"metric": "event_count", "metric_args": {"event": "purchase"}, "agg": "mean"},
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
            metrics=metrics if metrics is not None else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
            state_file=state_file,
        )

    @_tracked("headless_segment_overview")
    def segment_overview_data(
        self,
        segment_col: str,
        metrics: list | None = None,
        path_col: str | None = None,
        event_col: str | None = None,
    ) -> "pd.DataFrame":
        """
        Compute aggregated metrics across segment values (headless).

        Parameters
        ----------
        segment_col : str
            Segment column to split by; must be one of `schema.segment_cols`.
        metrics : list of dict, optional
            Metric configurations, each with a `"metric"` key, optional
            `"metric_args"`, and an `"agg"` key (`"mean"`, `"median"`, `"q5"`,
            `"q25"`, `"q75"`, `"q95"`, or `"complement_distance"`) controlling how
            per-path values roll up across a segment. See the Path Metrics
            documentation page for the metric reference.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event name column override; defaults to `schema.event_col`.

        Returns
        -------
        pd.DataFrame
            Metrics as rows and segment values as columns. Always includes
            segment_size and segment_share as the first two rows.
        """
        from retentioneering.tools.segment_overview import SegmentOverview

        return SegmentOverview(self).fit(
            segment_col=segment_col,
            metrics=metrics or [],
            path_col=path_col,
            event_col=event_col,
        )

    @_tracked("widget_cluster_analysis")
    def cluster_analysis(
        self,
        features: list | None = None,
        method: str | None = None,
        scaler: str | None = None,
        n_clusters=None,
        overview_metrics: list | None = None,
        path_col: str | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
        state_file: str | None = None,
    ):
        """
        An interactive tool for finding an optimal splitting of paths by behavioral metrics.
        Allows you to inspect clusters in a [Segment Overview](/docs/widgets/segment-overview)-style heatmap
        and offers the best possible splitting from the silhouette score perspective.
        Once the splitting looks right, you can label the clusters and save them as a new segment column
        of the eventstream right from the UI by clicking "Save Clusters".

        Parameters
        ----------
        features : list of dict, optional
            Metric configurations used as clustering features (see the [Path
            Metrics](/docs/path-metrics); defaults to per-event counts for every
            event in the eventstream.
        method : {"kmeans", "hdbscan"}, default "kmeans"
            Clustering algorithm.
        scaler : {"minmax", "standard"}, optional
            Feature scaler applied before clustering; default `"minmax"`.
        n_clusters : int, list of int, or str, optional
            Number of clusters. A single int fixes the cluster count; a list of
            ints or a range string (e.g. `"3-8"`) runs a silhouette-scored grid
            search over that range and picks the best. Defaults to `"3-8"`.
        overview_metrics : list of dict, optional
            Metrics shown in the overview heatmap after clustering (independent
            of `features`); defaults to per-event counts for every event.
            Both `features` and `overview_metrics` accept metric configs from the
            same [Path Metrics](/docs/path-metrics) registry.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        height : int, default 520
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.
        state_file : str, optional
            JSON file the widget state is bound to; see
            [Saving widget state](/docs/widgets#saving-widget-state).

        Examples
        --------
            stream.cluster_analysis(
                features=[{"metric": "length"}, {"metric": "duration"}, {"metric": "event_count_bulk"}],
                n_clusters="3-6",
            )
        """
        from retentioneering.widgets.cluster_analysis import (
            ClusterAnalysisWidget,
            _UNSET,
        )

        return ClusterAnalysisWidget(
            eventstream=self,
            stream_var_name=_infer_caller_var_name(self),
            features=features if features is not None else _UNSET,
            method=method if method is not None else _UNSET,
            scaler=scaler if scaler is not None else _UNSET,
            n_clusters=n_clusters if n_clusters is not None else _UNSET,
            overview_metrics=overview_metrics
            if overview_metrics is not None
            else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
            state_file=state_file,
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
        nmf_components=None,
        overview_metrics: list | None = None,
        path_col: str | None = None,
        event_col: str | None = None,
    ) -> dict:
        """
        Run cluster analysis headlessly and return a dict of results.

        Pass lists for n_clusters / nmf_components / min_cluster_size to trigger
        grid search with silhouette scoring. n_clusters is required for the kmeans
        method (the default), including nmf_components-only searches.

        `best_params` holds the concrete parameter values actually used to produce
        `overview_df` (the winning combination when searching, or just the fixed
        values passed in otherwise) — pass it straight to `add_clusters` to
        materialize the same clustering as a segment column.

        Parameters
        ----------
        features : list of dict, optional
            Metric configurations used as clustering features (see the Path
            Metrics documentation page); defaults to per-event counts for every
            event in the eventstream.
        method : {"kmeans", "hdbscan"}, default "kmeans"
            Clustering algorithm.
        scaler : {"minmax", "standard"}, optional
            Feature scaler applied before clustering; default `"minmax"`.
        n_clusters : int, list of int, or str, optional
            Number of clusters. A single int fixes the cluster count; a list of
            ints or a range string (e.g. `"3-8"`) runs a silhouette-scored grid
            search over that range and picks the best. Defaults to `"3-8"`.
        min_cluster_size : int or list of int, optional
            Minimum cluster size for the `"hdbscan"` method; defaults to `5`.
            A list triggers a silhouette-scored grid search over the given
            values.
        cluster_selection_epsilon : float or list of float, optional
            Cluster selection epsilon for the `"hdbscan"` method; defaults to
            `0.0`. A list triggers a silhouette-scored grid search over the
            given values.
        nmf_components : int or list of int, optional
            Number of components for an optional NMF (non-negative matrix
            factorization) step applied to the scaled features before
            clustering; if omitted, NMF is skipped. A list triggers a
            silhouette-scored grid search over the given values.
        overview_metrics : list of dict, optional
            Metrics shown in the overview heatmap after clustering (independent
            of `features`); defaults to per-event counts for every event.
            Both `features` and `overview_metrics` accept metric configs from the
            same Path Metrics registry.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event name column override; defaults to `schema.event_col`.

        Returns
        -------
        dict
            - `overview_df`: `DataFrame` from the segment overview heatmap,
              one row per path segmented by cluster label.
            - `cluster_labels`: `Series` of the cluster label assigned to each
              path, indexed by `path_col`.
            - `best_params`: concrete parameter values used to produce
              `overview_df` — pass straight to `add_clusters`.
            - `nmf`: `None` if `nmf_components` was not passed (or, in a grid
              search, if no candidate used NMF); otherwise a dict with
              `H_matrix`, `features`, and `W_cluster_means`.
            - `silhouette`: only present when a list was passed for
              `n_clusters` / `nmf_components` / `min_cluster_size` /
              `cluster_selection_epsilon` (grid search mode). A dict of two
              parallel lists — `{"params": [{"n_clusters": 3}, ...],
              "silhouette": [0.87, ...]}` — one entry per candidate tried;
              zip them to inspect individual scores. `overview_df`,
              `cluster_labels`, and `best_params` are omitted in this mode if
              every candidate was degenerate (fewer than 2 valid clusters).
        """
        from retentioneering.tools.cluster_analysis import ClusterAnalysis

        return ClusterAnalysis(self).fit(
            features_config=features,
            method=method,
            scaler=scaler,
            n_clusters=n_clusters,
            min_cluster_size=min_cluster_size,
            cluster_selection_epsilon=cluster_selection_epsilon,
            nmf_components=nmf_components,
            overview_metrics=overview_metrics,
            path_col=path_col,
            event_col=event_col,
        )

    def get_metric_distribution(
        self,
        segment_col: str,
        segment_value,
        metric: dict,
        complement: bool = False,
        path_col: str | None = None,
    ) -> dict:
        """Compute histogram/KDE distribution for a metric across one or two segment values.

        Parameters
        ----------
        segment_value:
            Single string → compare with complement (complement=True required).
            List of two strings → compare the two distributions.
        """
        from retentioneering.tools.segment_overview import SegmentOverview

        return SegmentOverview(self).get_metric_distribution(
            segment_col=segment_col,
            segment_value=segment_value,
            metric=metric,
            complement=complement,
            path_col=path_col,
        )
