import inspect
import json
from dataclasses import asdict
from functools import cached_property

import pandas as pd

from retentioneering import engine
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import SchemaConfigError
from retentioneering.ops import op as _op
from retentioneering.tools.types import T_TransitionMatrixValues, T_Diff
from retentioneering.utils.sentinels import UNSET as _SEGMENT_LEVEL_UNSET
from retentioneering.utils.sequences import find_delimiter_collisions

#: `diff`/`get_segment_levels` sentinel standing in for a missing (None/NaN)
#: segment level, since it can't be a real dict/query value the way `<REST>` can.
SEGMENT_MISSING = "<MISSING>"


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
        self._lineage: list[dict] = []
        self._post_init()

    @cached_property
    def schema(self) -> EventstreamSchema:
        return EventstreamSchema.from_dict(self._schema)

    @property
    def df(self) -> pd.DataFrame:
        return self._df

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

        for col in schema.event_cols:
            offenders = find_delimiter_collisions(df[col].cat.categories.tolist())
            if offenders:
                raise SchemaConfigError(
                    f"Event name(s) {offenders} in column '{col}' contain '->', "
                    f"which retentioneering uses as the path delimiter in "
                    f"matches_pattern/step_matrix pattern matching. Rename these "
                    f"events before creating the Eventstream."
                )

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

    def __repr__(self) -> str:
        chain = " → ".join(
            ["source"] + [str(o.get("type", "?")) for o in self._lineage]
        )
        return f"Eventstream: {chain} · {self._row_count_label()} rows"

    def _row_count_label(self) -> str:
        """Human-readable row count (excluding synthetic path_start/path_end
        rows), e.g. `120k`, `4.2M`, `57`. Cheap boolean-mask count, no copy —
        mirrors `is_empty()`'s approach so `__repr__` stays safe to call on
        large eventstreams."""
        exclude = [EventTypes().PATH_START.type, EventTypes().PATH_END.type]
        n = int((~self._df[self.schema.event_type].isin(exclude)).sum())
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.0f}k"
        return str(n)

    def recipe(self) -> list[dict]:
        """Return this eventstream's lineage as a JSON-serializable list of op
        dicts — the same `{"type": ..., **params}` shape `ops.py` defines and
        MCP preprocessors use. Empty for a freshly constructed (source)
        eventstream; one entry per processor call for a derived one.

        Round-trips with `Eventstream.from_recipe`:
        `Eventstream.from_recipe(df, s.recipe()).fingerprint == s.fingerprint`.

        Examples
        --------
            stream.filter_paths(...).add_segment(...).recipe()
            # [{"type": "filter_paths", "condition": {...}}, {"type": "add_segment", ...}]
        """
        return [dict(o) for o in self._lineage]

    @classmethod
    def from_recipe(
        cls, df: "pd.DataFrame | str", recipe: list[dict], schema: dict | None = None
    ) -> "Eventstream":
        """Reconstruct an `Eventstream` from a base dataframe (or CSV path) and
        a recipe — an op-dict list as returned by `recipe()` — by constructing
        a fresh source `Eventstream` and replaying each op via
        `ops.apply_ops`.

        Parameters
        ----------
        df : pd.DataFrame or str
            Base data, same as the `Eventstream` constructor's `df` argument.
        recipe : list of dict
            Ordered op list, e.g. from `some_stream.recipe()`.
        schema : dict, optional
            Schema for the base stream; defaults to the same default schema
            the plain `Eventstream(df)` constructor would use.

        Examples
        --------
            rebuilt = Eventstream.from_recipe(df, stream.recipe())
            assert rebuilt.fingerprint == stream.fingerprint
        """
        from retentioneering.ops import apply_ops

        base = cls(df, schema)
        return apply_ops(base, recipe)

    def to_dataframe(self, exclude_start_end: bool = True) -> pd.DataFrame:
        """Return the eventstream's rows as a plain pandas DataFrame (a copy).

        Parameters
        ----------
        exclude_start_end : bool, default True
            Drop the synthetic `path_start` / `path_end` boundary rows. Pass
            `False` to keep them, e.g. when checking what a widget actually
            counts.
        """
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
        event_col = event_col or self.schema.event_col
        event_col_q = engine.quote_ident(event_col)
        query = f"SELECT {event_col_q}, COUNT(*) AS cnt FROM df GROUP BY {event_col_q}"
        return engine.run(query, df=self._df).set_index(event_col)["cnt"].to_dict()

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
        """Available values per segment column, for UI catalogues and `diff`.

        A `Categorical`'s `.cat.categories` never includes NaN, so a segment
        column that has paths with no assigned value (e.g. via `add_segment`'s
        `func=`/`sql=` modes) would otherwise have that group silently
        unselectable. When that happens, the `SEGMENT_MISSING` sentinel is
        appended to the level list to represent it.
        """
        levels = {}
        for col in self.schema.segment_cols:
            series = self._df[col]
            cats = series.cat.categories.tolist()
            if series.isna().any():
                cats = cats + [SEGMENT_MISSING]
            levels[col] = cats
        return levels

    @_tracked("headless_describe")
    def describe(
        self,
        percentiles: tuple = (0.25, 0.5, 0.75, 0.9, 0.99),
        top_events: int | None = 20,
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
        top_events : int or None, default 20
            Number of most frequent events to include in `event_frequency`.
            Pass `None` to include every unique event, unranked and
            unlimited - use this when the result feeds something that needs
            the full event vocabulary (e.g. building a rename mapping),
            since the default silently drops everything past the top 20.

        Returns
        -------
        dict
            - `schema`: event_col, path_col, path_cols, segment_cols, timestamp_col
            - `shape`: n_events, n_paths, n_unique_events
            - `date_range`: min, max, span
            - `event_frequency`: DataFrame of event/count/share, sorted
              descending, limited to `top_events` rows. `.attrs["truncated"]`
              (bool) and `.attrs["n_total_events"]` (full unique event count)
              record whether/how much this was cut down.
            - `path_stats`: dict keyed by each entry of `schema.path_cols`,
              each value a `DataFrame` (from `DataFrame.describe`) with
              count/mean/std/min/percentiles/max rows and `length`/`duration`
              columns
            - `segments`: DataFrame of segment_col/value/count/share, one row
              per segment level across all segment columns

        Examples
        --------
            stream.describe()
            stream.describe(top_events=None)  # full event_frequency, no cap
        """
        from retentioneering.tools.describe import Describe

        return Describe(self).fit(percentiles=percentiles, top_events=top_events)

    @_tracked("dp_filter_events")
    @_op
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
    @_op
    def add_clusters(
        self,
        name: str,
        features: list,
        method: str = "kmeans",
        method_args: dict | None = None,
        scaler: str | None = "minmax",
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
            `"in_segment"`, `"in_segment_bulk"`. See the [Path Metrics](/docs/path-metrics)
            documentation page for the full metric reference.
        method : str, default "kmeans"
            Clustering algorithm. One of `"kmeans"` or `"hdbscan"`.
        method_args : dict, optional
            Parameters of the chosen `method` — the `metric` / `metric_args`
            shape applied to algorithms:

            - `"kmeans"` — `n_clusters` (int, required).
            - `"hdbscan"` — `min_cluster_size` (int, default 5),
              `cluster_selection_epsilon` (float, default 0.0).

            A key that does not belong to `method` raises rather than being
            ignored. `scaler` and `nmf_components` are not method arguments —
            they are pipeline steps applied before clustering, so they stay
            outside this dict.
        scaler : str or None, default "minmax"
            Feature scaler applied before clustering. One of `"minmax"`, `"std"`, or `None`.
            (`"standard"` is accepted as a legacy alias of `"std"`.)
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
                method_args={"n_clusters": 4},
                scaler="minmax",
            )

            # the same clustering the headless analysis settled on
            result = stream.cluster_analysis_data(features=[{"metric": "length"}])
            stream.add_clusters(name="cluster", features=[{"metric": "length"}], **result["best_params"])
        """
        from retentioneering.data_processors.add_clusters import AddClusters

        new_df, new_schema = AddClusters(
            eventstream=self,
            name=name,
            features=features,
            method=method,
            method_args=method_args,
            scaler=scaler,
            nmf_components=nmf_components,
            path_col=path_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_urls_to_events")
    @_op
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
        keeps its path as the event name, while every deeper page collapses to the
        single label `<path>/<slug>`. The original URL column is replaced in-place.

        Parameters
        ----------
        column : str
            Name of the column that contains raw URL strings.
        nodes : list of dict
            URL path tree. Each node dict must have a `"path"` key (str) and may
            include:
              - `"aggregate_children"` (bool) — collapse every page below this node
                into one `<path>/<slug>` event. The slug is a fixed placeholder, not
                the URL's own segment: all of `/catalog/phones` and
                `/catalog/laptops/mac` become `catalog/sub-page`.
              - `"exclude"` (bool) — drop rows whose URL falls under this node.
              - `"name"` (str) — the slug used for pages collapsed into this node;
                it does not rename the node's own URL, which always keeps its path.
                Declaring a child node with a `"name"` gives that branch its own slug
                (`catalog/phones`); declaring a child with `aggregate_children`
                instead makes it a nested aggregation point of its own.
        strip_host : bool, default True
            Remove the scheme and hostname, keeping only the pathname.
        strip_query : bool, default True
            Remove the query string and URL fragment.
        strip_locale : bool, default True
            Remove a leading 2-letter BCP-47 locale segment (e.g. `"en"`, `"fr-ca"`).
        keep_full_paths : bool, default False
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
    @_op
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
            Condition tree of leaf (comparison) and branch (`and`/`or`/`not`) nodes;
            a plain list is shorthand for AND. Metrics used in a leaf must produce
            exactly one value per path, which rules out `has_event_bulk`,
            `event_count_bulk`, and `in_segment_bulk`.
              - `op` — for a leaf, a comparison operator: `>`, `>=`, `<`, `<=`,
                `=` (or `==`), `!=`. For a branch, one of `and`, `or`, `not`.
              - `args` (branch nodes only) — list of child nodes.
                `[cond1, cond2]` ≡ `{"op": "and", "args": [cond1, cond2]}`.
              - `metric` — metric name (see the [Path Metrics](/docs/path-metrics) documentation page for
                the full list).
              - `value` — threshold value.
              - `metric_args` (optional) — dict of extra arguments for the metric.
                `has_event`/`event_count` take a single `event` string; for a
                multi-event AND/OR condition use `has_all_events`/`has_any_event`
                with an `event_groups` list.
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
        metrics = self.get_metrics(metric_configs, path_col=path_col).reset_index()

        where_condition = dp._get_where_condition(condition)
        path_col_q = engine.quote_ident(path_col)
        query = f"SELECT {path_col_q} FROM metrics WHERE {where_condition}"
        path_ids = engine.run(query, metrics=metrics)[path_col].tolist()

        if len(path_ids) == 0:
            raise EmptyEventstreamError("no paths match the filter_paths condition")

        result_stream = self.filter_events(keep={path_col: path_ids})
        if result_stream.is_empty():
            raise EmptyEventstreamError("no events remain after filter_paths")
        return result_stream

    @_tracked("get_metrics")
    def get_metrics(self, metrics: list, path_col: str | None = None) -> pd.DataFrame:
        """
        Compute per-path metric values as a feature table.

        Parameters
        ----------
        metrics : list of dict
            Metric configs, each with a `metric` name and optional `metric_args`.
            No `agg` field here — these are per-path values, not aggregates. See
            the [Path Metrics](/docs/path-metrics) documentation page for the full metric reference.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

        Returns
        -------
        pd.DataFrame
            One row per path (path ID as the index), one column per metric.
            `event_count_bulk`/`has_event_bulk` expand into one column per event,
            `in_segment_bulk` into one column per segment level.

        Examples
        --------
            features = stream.get_metrics([
                {"metric": "length"},
                {"metric": "duration"},
                {"metric": "event_count", "metric_args": {"event": "purchase"}},
            ])
        """
        from retentioneering.metrics.metric_builder import MetricBuilder

        builder = MetricBuilder(self)
        return builder.build_metrics(metrics, path_col)

    @_tracked("dp_add_events")
    @_op
    def add_events(
        self,
        name: str,
        source_event=None,
        sql=None,
        churn=None,
        anchor=None,
        path_col=None,
    ) -> "Eventstream":
        """
        Insert synthetic events derived from existing events or a SQL query.

        Exactly one of `source_event`, `sql`, `churn`, or `anchor` must be
        provided. The new event rows are appended to the eventstream; original
        rows are kept.

        A synthetic event shares its source row's timestamp and sorts *before*
        it, so it marks the moment the source event opens. The exception is a
        `churn` event, which closes a stretch of inactivity and so sorts after
        the last active event (and always before `path_end`).

        Parameters
        ----------
        name : str
            Name of the synthetic event to create.
        source_event : str or list of str, optional
            An existing event name, or several. Every occurrence of any of them
            gets a synthetic event at the same timestamp — a path with three
            matching events gets three synthetic events.
        sql : str, optional
            DuckDB SQL SELECT statement that reads from the `eventstream` table alias
            and returns rows in the eventstream schema. Each returned row is added as a
            new synthetic event.
        churn : dict, optional
            Creates a churn event after a period of inactivity.
              - `inactivity_days` (int or float, required) — gap in days after which
                a churn event is inserted.
              - `active_events` (list of str, optional) — only these events count as
                activity; defaults to all events.
        anchor : str or dict, optional
            Mark a *position* in each path rather than an event name: an event
            name, or an anchor spec — the same form `truncate_paths` takes for
            `start_anchor`, and the same keys (`pattern`, `at`, `occurrence`,
            `offset`, `offset_side`); see
            [Path Patterns](/docs/path-patterns). One anchor per call, not a
            list. By default the anchor resolves once per path
            (`occurrence="first"`); pass `occurrence="all"` to mark every
            position the anchor can occupy. Paths where it resolves nowhere get
            no event.

            This is what makes a position addressable by every other tool: a
            pattern can say "the cart that was followed by checkout with no cart
            in between", but only an event name can be centred on by
            `step_matrix`, counted by a funnel, or filtered on.
        path_col : str, optional
            Path ID column the `anchor` is resolved within; defaults to
            `schema.path_col`. Ignored by the other modes.

        Examples
        --------
            stream.add_events("session_start", source_event=["login", "app_open"])
            stream.add_events("churned", churn={"inactivity_days": 30})
            stream.add_events("churned", churn={"inactivity_days": 30, "active_events": ["purchase"]})

            # the cart that actually led to checkout, not every cart
            stream.add_events(
                "checkout_cart",
                anchor={"pattern": "cart->[^cart]*->shipping_details", "at": "start"},
            )
            stream.step_matrix(path_pattern="checkout_cart")

            # every purchase a path made, marked three events ahead of time
            stream.add_events(
                "pre_purchase",
                anchor={"pattern": "purchase", "occurrence": "all", "offset": -3},
            )
        """
        from retentioneering.data_processors.add_events import AddEvents

        new_df, new_schema = AddEvents(
            name,
            source_event=source_event,
            sql=sql,
            churn=churn,
            anchor=anchor,
            path_col=path_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_add_segment")
    @_op
    def add_segment(
        self,
        name: str,
        rules=None,
        func=None,
        sql=None,
        funnel_events=None,
        time_range=None,
        metric_bins=None,
        path_col=None,
    ) -> "Eventstream":
        """
        Add a new categorical segment column to the eventstream.

        Exactly one of `rules`, `func`, `sql`, `funnel_events`, `time_range`, or
        `metric_bins` must be provided — unless `name` is already listed in
        `schema.custom_cols`, in which case passing none of them promotes that
        existing column to a segment in place, without recomputing its values.

        Parameters
        ----------
        name : str
            Name of the new segment column.
        rules : list, optional
            CASE-WHEN rules. A list of conditions plus a final else entry:
              - Each condition entry is `[column, op, value, label]` — translates to
                `WHEN <column> <op> <value> THEN <label>` in SQL. A string `value`
                is quoted for you, so write `"US"`, not `"'US'"` — the exception is
                `op="in"`, whose value is inserted raw and must therefore be a
                complete SQL tuple: `"('GB', 'DE', 'FR')"`.
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
            ("closed") funnel. A path is assigned `funnel_events[k]` if there
            exists an increasing sequence of event occurrences matching
            `funnel_events[0]`, `funnel_events[1]`, ..., `funnel_events[k]` in
            that order (later steps may be reached via any qualifying
            occurrence, not necessarily the first or last one — earlier
            events occurring again after a later step was reached don't
            un-complete it). A path is assigned the highest such `k`; if it
            never completes even `funnel_events[0]`, it is labeled
            `out_of_funnel`.
            Segment values (in ascending funnel order): `out_of_funnel`, then each
            event name from `funnel_events[0]` to `funnel_events[-1]`.
        time_range : tuple or list, optional
            `(start, end)` — two timestamps (string or `pd.Timestamp`) bounding
            an inclusive interval over `schema.timestamp_col`. Each event is
            labeled `inside` if its timestamp falls within `[start, end]`,
            otherwise `outside`.
        metric_bins : dict, optional
            Split paths into bins by a per-path metric. Keys:

            - `metric` (required) — a metric config, `{"metric": ..., "metric_args": ...}`,
              as used by `filter_paths` and `add_clusters`. It must produce exactly
              one value per path. See the [Path Metrics](/docs/path-metrics) page.
            - `edges` — cut points in the metric's own units. **Interior** points:
              N of them always give N+1 bins, `[5, 15]` meaning `< 5`, `5-15`, and
              `>= 15`. (This differs from `pd.cut`, where the list is the outer
              edges; here nothing can fall outside the split, because a segment
              has no room for `NaN`.)
            - `quantiles` — the same cut points expressed as quantiles: a list
              strictly between 0 and 1, or an int asking for that many equal-sized
              bins (`4` for quartiles). Computed over the eventstream as it is at
              this call, so a `filter_paths` before or after this one changes them.
            - `segment_levels` — one name per bin, so `len(segment_levels)` is
              always the number of bins. Omit for auto names: `"[5, 15)"` for
              `edges`, `q1`..`qN` for `quantiles`.

            Exactly one of `edges` / `quantiles` is required. Paths the metric has
            no value for (`time_between` is the one that can be undefined — for a
            path missing either of its two events) get the level `"undefined"`,
            which is not one of the bins and so is not counted against
            `segment_levels`; rename it with `rename_segment_levels`.
        path_col : str, optional
            Path ID column override for `funnel_events` and `metric_bins` modes;
            defaults to `schema.path_col`.

        Examples
        --------
            # ordered CASE-WHEN rules over an existing column
            stream.add_segment("region", rules=[
                ["country", "=", "US", "domestic"],
                ["country", "in", "('GB', 'DE', 'FR')", "europe"],
                ["other"],
            ])

            # the deepest funnel step each path completed in order
            stream.add_segment("funnel", funnel_events=["add_to_cart", "shipping_details", "purchase"])

            # "inside" / "outside" a time window
            stream.add_segment("incident", time_range=("2024-03-10", "2024-03-17"))

            # bin paths by a metric — named bins with explicit cut points ...
            stream.add_segment("path_length", metric_bins={
                "metric": {"metric": "length"},
                "edges": [5, 15],
                "segment_levels": ["short", "mid", "long"],
            })

            # ... or quartiles, with q1..q4 named for you
            stream.add_segment("speed", metric_bins={
                "metric": {"metric": "duration"},
                "quantiles": 4,
            })

            # a DuckDB SELECT returning one label per row
            stream.add_segment("device", sql="SELECT CASE WHEN platform = 'mobile' THEN 'mobile' ELSE 'web' END FROM eventstream")

            # promote a column that is already in the eventstream, keeping its values
            stream.add_segment("returned")
        """
        from retentioneering.data_processors.add_segment import AddSegment

        new_df, new_schema = AddSegment(
            name,
            rules=rules,
            func=func,
            sql=sql,
            funnel_events=funnel_events,
            time_range=time_range,
            metric_bins=metric_bins,
            path_col=path_col,
            eventstream=self,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_collapse_events")
    @_op
    def collapse_events(
        self,
        loops=None,
        event_groups=None,
        bounds=None,
        group_col=None,
        name=None,
        agg=None,
        path_col=None,
        event_col=None,
    ) -> "Eventstream":
        """
        Merge a run of events into a single representative event.

        Two independent decisions. **How to chunk the path** is exactly one mode:
        `loops` or `group_col` group adjacent rows sharing a value, while
        `event_groups` and `bounds` cut the path into windows, the latter
        spelled as in `split_sessions`. **How to name** the merged event is `name`, which is orthogonal to
        the mode.

        Breaking on an inactivity gap is not a mode here: run
        `split_sessions(timeout="30m")` first and collapse its session column
        with `group_col`, which says in two readable steps what one combined
        argument would have hidden.

        Parameters
        ----------
        loops : bool or list of str, optional
            Collapse each self-loop — a run of the same event repeating — into
            one row. `True` collapses every event's loops; a list of event names
            collapses only those. This is the `A → A → A` on a transition graph.
        event_groups : str or list of str, optional
            Events that belong to one group: any run drawn from this set
            collapses into a single event, wherever it occurs in the path. Unlike
            `loops`, the run may mix the listed events —
            `event_groups=["a", "b"]` turns `a, a, b, b` into one event, where
            `loops=["a", "b"]` turns it into two.
        bounds : dict, optional
            Collapse every event between a `start_event` and the next
            `end_event`, both included. Requires both keys.
        group_col : str, optional
            Collapse each run of *adjacent* rows sharing this column's value.
            Not SQL's `GROUP BY`: a value that comes back later in the path
            starts a second event rather than joining the first across the gap.
            Must differ from the event column — for repeats of the same event
            use `loops`.
        name : str or dict or list, optional
            What the merged event is called. Three forms:

            - a **string** — that literal name.
            - `{"col": "<column>"}` — the value of another column, so a run of
              sessions can be named by the session's type.
            - a **list of cases** — `{"condition": ..., "name": ...}` dicts
              evaluated against the group's own events, optionally closed by a
              plain string used as the fallback for groups no case matched.
              A condition is the `filter_paths` condition tree, over the metrics
              `has_event`, `event_count`, `has_all_events`, `has_any_event`,
              `duration`, `length`, `time_between`, `active_days`.

            Required for the window modes, which have no natural name of their
            own. `loops` defaults to the repeated event's name and `group_col`
            to the value of the column being grouped on.
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
            stream.collapse_events(loops=True)

            # Collapse only repeated product_view events
            stream.collapse_events(loops=["product_view"])

            # Merge checkout steps into a single "checkout" event
            stream.collapse_events(
                event_groups=["checkout_start", "checkout_step", "checkout_confirm"],
                name="checkout",
            )

            # One event per session, named after the session's type column
            stream.collapse_events(group_col="session_id", name={"col": "session_type"})

            # One event per session, named by what the session did
            stream.collapse_events(
                group_col="session_id",
                name=[
                    {"condition": {"op": "=", "metric": "has_event", "value": True,
                                   "metric_args": {"event": "purchase"}},
                     "name": "buying_session"},
                    "browsing_session",
                ],
            )
        """
        from retentioneering.data_processors.collapse_events import CollapseEvents

        new_df, new_schema = CollapseEvents(
            loops=loops,
            event_groups=event_groups,
            bounds=bounds,
            group_col=group_col,
            name=name,
            agg=agg,
            path_col=path_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_to_daily_states")
    @_op
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
            Days after a path's last event to continue generating state rows.
            Capped at the last day present in the dataset, so paths active near
            the end of the observation window get fewer trailing rows.
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
    @_op
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
    @_op
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
    @_op
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
    @_op
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
                name="cluster",
                features=[{"metric": "length"}],
                method_args={"n_clusters": 3},
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
    @_op
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
    @_op
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
    @_op
    def split_sessions(
        self,
        session_col="session_id",
        session_index_col="session_index",
        separator=None,
        bounds=None,
        timeout=None,
        path_col=None,
        event_col=None,
    ) -> "Eventstream":
        """
        Split each path into sub-sessions and add session ID and index columns.

        At least one boundary criterion must be provided: `separator`, `bounds`,
        or `timeout`. `separator` and `bounds` are mutually exclusive; `timeout`
        may be combined with either.

        Parameters
        ----------
        session_col : str, default "session_id"
            Name of the new column that holds the unique session identifier.
        session_index_col : str, default "session_index"
            Name of the new column that holds the 1-based session index within each path.
        separator : str or list of str, optional
            Event name(s) that mark a session boundary. The separator event starts a new
            session; the separator row itself is dropped from the output.
        bounds : dict, optional
            Sessions delimited by their own opening and closing events. Both keys
            are required:
              - `start_event` (str or list of str) — event name(s) that open a session.
              - `end_event` (str or list of str) — event name(s) that close it.
            Events outside a `start_event`..`end_event` window get no session.
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
            stream.split_sessions(bounds={"start_event": "session_start", "end_event": "session_end"})
            stream.split_sessions(separator="app_open", timeout="1h")
        """
        from retentioneering.data_processors.split_sessions import SplitSessions

        new_df, new_schema = SplitSessions(
            session_col=session_col,
            session_index_col=session_index_col,
            separator=separator,
            bounds=bounds,
            timeout=timeout,
            path_col=path_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_truncate_paths")
    @_op
    def truncate_paths(
        self, start_anchor, end_anchor, path_col=None, event_col=None
    ) -> "Eventstream":
        """
        Trim each path to the window between two anchors (inclusive).

        Each anchor is an event name, an anchor spec, or a list of either. Events
        outside the resulting window are dropped, and a path with no resolvable
        anchor on either side is dropped entirely.

        The end anchor is searched for *after* the resolved start, so a path whose
        end event only occurs before its start event is dropped rather than
        producing an inverted window.

        An anchor spec is a dict:

        - `pattern` (required) — an event name, or a `"->"`-separated pattern
          such as `"add_to_cart->.*->purchase"`; see
          [Path Patterns](/docs/path-patterns) for the full syntax. The reserved
          names `"path_start"` / `"path_end"` refer to a path's own first / last
          event.
        - `at` — which of the pattern's event names is the anchor point:
          `"start"`, `"end"` (default), or an integer index over the pattern's
          event names (`.*` is not a position, so it is not counted). For
          `"a->b->.*->c"` the names are `["a", "b", "c"]` and `at=1` is `b`.
        - `occurrence` — which match to use when the pattern has more than one.
          `"first"` (default) puts every event name of the pattern as early as it
          can be in any valid match, `"last"` as late as it can be. Note that
          `"last"` is not "the last occurrence of the anchor event": an
          occurrence that is part of no valid match is not a candidate, so on
          `catalog, cart, purchase, cart` the last match of
          `"catalog->.*->cart->.*->purchase"` anchors on the *second* event, not
          the fourth. `"first"` is the same match `step_matrix` centres on, given
          the same `path_pattern`. `"all"` is rejected here — a window bound has
          to be a single position; it is for `add_events(anchor=...)`.
        - `offset` — move the anchor off the matched event: an int shifts it that
          many events, a duration string or `pd.Timedelta` (`"30m"`) shifts it in
          time and then snaps to the nearest event *inside* the window. An offset
          that runs past the path's own boundary clamps to it.
        - `offset_side` — which way a time `offset` rounds to a real event,
          `"start"` (forward) or `"end"` (backward). Defaults to the side of the
          window being resolved, which rounds inward; set it to widen instead.

        A **list** of anchors keeps the narrowest window they imply — the latest
        start, the earliest end. That expresses both "whichever comes first"
        (`["purchase", {"pattern": "add_to_cart", "offset": 10}]` cuts at the
        purchase or 10 events after the cart, whichever is sooner) and a fallback
        (`["purchase", "path_end"]` cuts at the purchase, or at the end of the
        path for those who never purchased).

        Parameters
        ----------
        start_anchor : str or dict or list
            Where the window opens.
        end_anchor : str or dict or list
            Where the window closes.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event column override; defaults to `schema.event_col`.

        Examples
        --------
            stream.truncate_paths(start_anchor="registration", end_anchor="purchase")
            stream.truncate_paths(start_anchor="registration", end_anchor="path_end")

            # keep every path, cutting the converted ones at their purchase
            stream.truncate_paths(
                start_anchor="path_start", end_anchor=["purchase", "path_end"]
            )

            # 10 events after the cart that completed catalog -> ... -> add_to_cart,
            # or the purchase if it comes sooner
            stream.truncate_paths(
                start_anchor={"pattern": "catalog->.*->add_to_cart"},
                end_anchor=[
                    "purchase",
                    {"pattern": "catalog->.*->add_to_cart", "offset": 10},
                ],
            )

            # the half hour that follows a user's last support chat
            stream.truncate_paths(
                start_anchor={"pattern": "support_chat", "occurrence": "last"},
                end_anchor={
                    "pattern": "support_chat",
                    "occurrence": "last",
                    "offset": "30m",
                },
            )
        """
        from retentioneering.data_processors.truncate_paths import TruncatePaths

        new_df, new_schema = TruncatePaths(
            start_anchor=start_anchor,
            end_anchor=end_anchor,
            path_col=path_col,
            event_col=event_col,
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    def _filter_by_segment_levels(
        self, segment_col: str, levels: list
    ) -> "Eventstream":
        """`keep`-style filter, but levels may include SEGMENT_MISSING — which
        `keep=` can't express, since SQL `IN` never matches NULL."""
        if SEGMENT_MISSING not in levels:
            return self.filter_events(keep={segment_col: levels})
        real_values = {v for v in levels if v != SEGMENT_MISSING}
        return self.filter_events(
            func=lambda df: df[segment_col].isin(real_values) | df[segment_col].isna()
        )

    def _split_two(self, split, path_col: str | None = None):
        from retentioneering.exceptions import (
            EmptyEventstreamError,
            DiffConfigError,
            SegmentLevelNotFoundError,
            PathIdNotFoundError,
        )

        if len(split) == 3:
            segment_col, v1, v2 = split[0], split[1], split[2]
            if segment_col not in self.schema.segment_cols:
                raise DiffConfigError(f"'{segment_col}' is not a segment column")
            all_vals = set(self.get_segment_levels().get(segment_col, []))

            def _coerce(value):
                """Match a value that arrived as a string (the JS widget, MCP
                and any JSON round-trip stringify everything) back to the
                actual typed segment level: 'false' -> False, '5' -> 5.
                Returns the value unchanged when there is no unambiguous
                match — the caller then raises its normal error."""
                if value in all_vals or not isinstance(value, str):
                    return value
                matches = []
                for candidate in all_vals:
                    if isinstance(candidate, str):
                        continue
                    try:
                        candidate_json = json.dumps(candidate)
                    except (TypeError, ValueError):
                        candidate_json = None
                    if value in (candidate_json, str(candidate)):
                        matches.append(candidate)
                return matches[0] if len(matches) == 1 else value

            v1 = _coerce(v1)
            if v1 not in all_vals:
                raise SegmentLevelNotFoundError(
                    segment_level=v1,
                    segment_col=segment_col,
                    available_levels=sorted(all_vals),
                )
            s1 = self._filter_by_segment_levels(segment_col, [v1])
            if v2 == "<REST>":
                v2_vals = list(all_vals - {v1})
                if not v2_vals:
                    raise DiffConfigError(
                        f"'{segment_col}' has no other levels besides '{v1}'; "
                        "'<REST>' requires at least one complementary level."
                    )
            else:
                v2 = _coerce(v2)
                if v2 not in all_vals:
                    raise SegmentLevelNotFoundError(
                        segment_level=v2,
                        segment_col=segment_col,
                        available_levels=sorted(all_vals),
                    )
                v2_vals = [v2]
            s2 = self._filter_by_segment_levels(segment_col, v2_vals)
        elif len(split) == 2:
            ids1, ids2 = split[0], split[1]
            path_col = path_col or self.schema.path_col
            available_ids = set(self._df[path_col].unique().tolist())
            missing1 = [i for i in ids1 if i not in available_ids]
            missing2 = [i for i in ids2 if i not in available_ids]
            if missing1 or missing2:
                raise PathIdNotFoundError(
                    sorted(set(missing1 + missing2), key=str), path_col
                )
            s1 = self.filter_events(keep={path_col: list(ids1)})
            s2 = self.filter_events(keep={path_col: list(ids2)})
        else:
            raise DiffConfigError("diff must be (seg, v1, v2) or (ids1, ids2)")
        if s1.is_empty():
            raise EmptyEventstreamError("first diff group is empty")
        if s2.is_empty():
            raise EmptyEventstreamError("second diff group is empty")
        return s1, s2

    def _restrict_to_pattern(
        self, path_pattern: str, path_col: str | None = None, stacklevel: int = 5
    ) -> "Eventstream":
        """
        Keep only the paths matching `path_pattern` (see `paths.anchors`).

        Shared by every tool that takes a `path_pattern`, so that the pattern
        selects the same set of paths everywhere and reports the same errors:
        `InvalidParameterError` for a token that is not an event in this
        eventstream, `PatternNoMatchError` when nothing matches at all.
        """
        from retentioneering.exceptions import PatternNoMatchError
        from retentioneering.paths import anchors

        path_col = path_col or self.schema.path_col
        path_pattern = anchors.normalize_pattern(
            path_pattern, stacklevel=stacklevel, param="path_pattern"
        )
        stream = self.add_start_end_events(path_col=path_col)
        anchors.validate_pattern_tokens(
            path_pattern,
            stream.df[stream.schema.event_col].unique().tolist(),
            param="path_pattern",
        )
        match = anchors.resolve_anchors(
            stream.df, stream.schema, path_pattern, path_col=path_col
        )
        matching_ids = match.paths().tolist()
        if not matching_ids:
            raise PatternNoMatchError(path_pattern)
        return self.filter_events(keep={path_col: matching_ids})

    @_tracked("dp_add_start_end_events")
    @_op
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
        path_pattern: str | None = None,
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
            compare two segment levels, or `(path_ids1, path_ids2)` to compare two
            explicit path-id groups. `value2` may be `<REST>`, meaning "every other
            value of `segment_col`". Either value may be `<MISSING>`, meaning paths
            with no `segment_col` value assigned (e.g. left unset by `add_segment`'s
            `func=`/`sql=` modes) — see `get_segment_levels`.
        path_pattern : str, optional
            Restrict the graph to paths matching a `"->"`-separated event sequence:
            `"add_to_cart->.*->purchase"` is those two in that order with
            anything in between, `"add_to_cart->[^support_chat]*->purchase"`
            the same without support in between. Full syntax:
            [Path Patterns](/docs/path-patterns). Unlike Step Matrix's
            parameter of the same name this only selects *which paths* are
            drawn — a graph has no step axis to centre. To cut the paths
            themselves down to the window the pattern describes, use
            `truncate_paths`.

        Returns
        -------
        pd.DataFrame
            Events x events matrix of the selected `edge_weight`. In diff mode,
            returns `(diff, group1, group2)` — three matrices instead of one.

        Examples
        --------
            stream.transition_graph_data(edge_weight="count")
            diff, g1, g2 = stream.transition_graph_data(diff=("platform", "mobile", "desktop"))
            stream.transition_graph_data(path_pattern="add_to_cart->.*->purchase")
        """
        from retentioneering.tools.transition_matrix import TransitionMatrix

        return TransitionMatrix(self).fit(
            edge_weight, diff, path_col, path_pattern=path_pattern
        )

    @_tracked("headless_step_sankey")
    def step_sankey_data(
        self,
        max_steps: int = 10,
        diff: T_Diff = None,
        path_col: str | None = None,
        path_pattern: str | None = None,
        anchor: str | dict | None = None,
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
            Restrict and split paths on a `"->"`-separated event sequence:
            `"add_to_cart->.*->purchase"` is those two in that order with
            anything in between, `"add_to_cart->[^support_chat]*->purchase"`
            the same without support in between. Full syntax:
            [Path Patterns](/docs/path-patterns). Without a pattern, computes over
            the whole path from `path_start` to `path_end`. Each anchor event in
            the pattern produces its own matrix block. To see the
            neighborhood around a single event: `path_pattern="add_to_cart"`.
        anchor : str or dict, optional
            Centre everything on one position instead, given as an anchor spec —
            the same form `truncate_paths` takes for `start_anchor`, and the same
            keys (`pattern`, `at`, `occurrence`, `offset`, `offset_side`). Yields
            a single block. Mutually exclusive with `path_pattern`, which lays
            the pattern's parts out side by side; use `anchor` when the position
            needs `occurrence` or an `offset`, neither of which a pattern can
            express. Paths where the anchor does not resolve are absent, so it
            selects as well as centres. `occurrence="all"` is rejected: several
            positions per path would make a path count more than once, and the
            cells are shares of paths.

        Returns
        -------
        pd.DataFrame or tuple of pd.DataFrame
            Without `path_pattern` or `anchor`: a single DataFrame (or, in diff
            mode, `(combined, group1, group2)` — three DataFrames). With either:
            one DataFrame per block, as a tuple (or, in diff mode,
            `(combined_blocks, group1_blocks, group2_blocks)`, each itself a
            tuple of per-block DataFrames) — a pattern with several anchor events
            produces several blocks, an `anchor` always one.

        Examples
        --------
            df = stream.step_sankey_data(max_steps=10)
            combined, g1, g2 = stream.step_sankey_data(diff=("user_lifecycle", "loyal", "new"))
            blocks = stream.step_sankey_data(path_pattern="add_to_cart->.*->purchase")
            blocks = stream.step_sankey_data(
                anchor={"pattern": "add_to_cart", "occurrence": "last"}
            )
        """
        from retentioneering.tools.step_matrix import StepMatrix

        result = StepMatrix(self).fit(
            max_steps=max_steps,
            diff=diff,
            path_col=path_col,
            path_pattern=path_pattern,
            anchor=anchor,
        )
        if path_pattern is not None or anchor is not None:
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
        anchor: str | dict | None = None,
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
            Restrict and split paths on a `"->"`-separated event sequence:
            `"add_to_cart->.*->purchase"` is those two in that order with
            anything in between, `"add_to_cart->[^support_chat]*->purchase"`
            the same without support in between. Full syntax:
            [Path Patterns](/docs/path-patterns). Without a pattern, computes over
            the whole path from `path_start` to `path_end`. Each anchor event in
            the pattern produces its own matrix block. To see the
            neighborhood around a single event: `path_pattern="add_to_cart"`.
        anchor : str or dict, optional
            Centre everything on one position instead, given as an anchor spec —
            the same form `truncate_paths` takes for `start_anchor`, and the same
            keys (`pattern`, `at`, `occurrence`, `offset`, `offset_side`). Yields
            a single block. Mutually exclusive with `path_pattern`, which lays
            the pattern's parts out side by side; use `anchor` when the position
            needs `occurrence` or an `offset`, neither of which a pattern can
            express. Paths where the anchor does not resolve are absent, so it
            selects as well as centres. `occurrence="all"` is rejected: several
            positions per path would make a path count more than once, and the
            cells are shares of paths.

        Returns
        -------
        pd.DataFrame or tuple of pd.DataFrame
            Without `path_pattern` or `anchor`: a single DataFrame (or, in diff
            mode, `(combined, group1, group2)` — three DataFrames). With either:
            one DataFrame per block, as a tuple (or, in diff mode,
            `(combined_blocks, group1_blocks, group2_blocks)`, each itself a
            tuple of per-block DataFrames) — a pattern with several anchor events
            produces several blocks, an `anchor` always one.

        See Also
        --------
        step_sankey_data : Same computation; this method is a plain alias.
        """
        return self.step_sankey_data(
            max_steps=max_steps,
            diff=diff,
            path_col=path_col,
            path_pattern=path_pattern,
            anchor=anchor,
        )

    @_tracked("widget_step_sankey")
    def step_sankey(
        self,
        max_steps=None,
        diff=None,
        path_col=None,
        path_pattern=None,
        anchor=None,
        step_window=None,
        height=None,
        sidebar_open=None,
        state_file=None,
    ):
        """
        Flow diagram of what users are doing at each step of their path: block
        height is the share of paths on that event at that step (columns sum to 1,
        or to 0 in diff mode), and ribbons show how volume moves between two
        adjacent steps. Ribbons do not chain — paths arriving by different routes
        merge at every column.

        Same numbers as [Step Matrix](/docs/widgets/step-matrix), drawn as flows.

        Parameters
        ----------
        max_steps : int, default 10
            Number of path steps to compute.
        anchor : str or dict, optional
            Centre on one position instead of laying out a pattern's parts; an
            anchor spec, same form as `truncate_paths`' `start_anchor`. Yields a
            single block and is mutually exclusive with `path_pattern`. See
            `step_sankey_data`.
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
            stream.step_sankey(diff=("acquisition_channel", "paid_search", "<REST>"))
        """
        from retentioneering.widgets.step_sankey import StepSankeyWidget, _UNSET

        return StepSankeyWidget(
            eventstream=self,
            max_steps=max_steps if max_steps is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            path_pattern=path_pattern if path_pattern is not None else _UNSET,
            anchor=anchor if anchor is not None else _UNSET,
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
        anchor=None,
        step_window=None,
        height=None,
        sidebar_open=None,
        state_file=None,
    ):
        """
        Heatmap of what users are doing at each step of their path: cell
        `[event, step]` is the share of paths on that event at that step, so every
        column sums to 1 (to 0 in diff mode). Rows are events, columns are steps
        from the anchor — `path_start` by default, or a `path_pattern` event, in
        which case the columns to its left are negative.

        Same numbers as [Step Sankey](/docs/widgets/step-sankey), drawn as a table.

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
            Restrict and split paths on a `"->"`-separated event sequence:
            `"add_to_cart->.*->purchase"` is those two in that order with
            anything in between, `"add_to_cart->[^support_chat]*->purchase"`
            the same without support in between. Full syntax:
            [Path Patterns](/docs/path-patterns). Without a pattern, shows the
            whole path from `path_start` to `path_end`. Multiple anchors render
            one matrix block per anchor, side by side. A pattern that doesn't
            start at `path_start` or end at `path_end` shows a serrated edge,
            signalling paths continue beyond the visible range. To see the
            neighborhood around a single event: `path_pattern="add_to_cart"`.
        anchor : str or dict, optional
            Centre on one position instead of laying out a pattern's parts; an
            anchor spec, same form as `truncate_paths`' `start_anchor`. Yields a
            single block and is mutually exclusive with `path_pattern`. See
            `step_sankey_data`.
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
            stream.step_matrix(diff=("user_lifecycle", "new", "loyal"))
        """
        from retentioneering.widgets.step_matrix import StepMatrixWidget, _UNSET

        return StepMatrixWidget(
            eventstream=self,
            max_steps=max_steps if max_steps is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            path_pattern=path_pattern if path_pattern is not None else _UNSET,
            anchor=anchor if anchor is not None else _UNSET,
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
        path_pattern=None,
        height=None,
        sidebar_open=None,
        views=None,
        view=None,
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
        path_pattern : str, optional
            Restrict the graph to paths matching a `"->"`-separated event sequence:
            `"add_to_cart->.*->purchase"` is those two in that order with
            anything in between, `"add_to_cart->[^support_chat]*->purchase"`
            the same without support in between. Full syntax:
            [Path Patterns](/docs/path-patterns). Unlike Step Matrix's
            parameter of the same name this only selects *which paths* are
            drawn — a graph has no step axis to centre. Everything the widget shows, event counts
            included, is computed from the restricted set. To cut the paths
            themselves down to the window the pattern describes, use
            `truncate_paths`.
        height : int, default 500
            Widget height in pixels.
        sidebar_open : bool, default True
            Whether the sidebar starts open.
        views : list of dict, optional
            Named visual presets rendered as pills above the graph. Each view
            is a dict with the keys `name`, `focus`, `edgeFilter`,
            `eventCountFilter`, `hiddenEvents`, `viewport` (all optional
            except `name`) and describes only how the graph is *displayed* —
            never the computed data. See the
            [Views](/docs/widgets/transition-graph#views) section for a
            detailed description of every key.
        view : dict or str, optional
            View applied once after the graph is built: a view dict (`name`
            not required), or the name of an entry in `views`. See the
            [Views](/docs/widgets/transition-graph#views) section.
        state_file : str, optional
            JSON file the widget state is bound to; see
            [Saving widget state](/docs/widgets#saving-widget-state).

        Examples
        --------
            stream.transition_graph()
            stream.transition_graph(edge_weight="count", diff=("user_lifecycle", "loyal", "new"))
            stream.transition_graph(state_file="checkout_graph.json")
            stream.transition_graph(path_pattern="add_to_cart->.*->purchase")
            stream.transition_graph(
                views=[{"name": "Checkout", "focus": {"type": "node", "event": "cart"}}],
                view="Checkout",
            )
        """
        from retentioneering.widgets.transition_graph import (
            TransitionGraphWidget,
            _UNSET,
        )

        # Restricting up front rather than passing the pattern down keeps every
        # number the widget derives — event counts, diff splits, the matrix —
        # computed from the same set of paths.
        source = (
            self
            if path_pattern is None
            else self._restrict_to_pattern(path_pattern, path_col, stacklevel=3)
        )

        return TransitionGraphWidget(
            eventstream=source,
            edge_weight=edge_weight if edge_weight is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
            views=views if views is not None else _UNSET,
            view=view if view is not None else _UNSET,
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
            stream.funnel(steps=["catalog", "add_to_cart", "purchase"])
            stream.funnel(steps=["add_to_cart", "purchase"], diff=("user_lifecycle", "loyal", "new"))
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

    @_tracked("get_conversion_rate")
    def get_conversion_rate(
        self,
        start_anchor,
        end_anchor,
        within=None,
        path_col: str | None = None,
    ) -> pd.DataFrame:
        """
        Given that `start_anchor` happened, how often does `end_anchor` follow?

        One row per (`start_anchor`, `end_anchor`) pair, counted over **paths**:
        a path where the start anchor occurred contributes one to the
        denominator no matter how many times it occurred, and converts if the
        end anchor lands strictly after it (within `within`, if given).

        Each anchor is an event name or an anchor spec — the same forms
        `truncate_paths` takes, including `pattern` / `at` / `occurrence` /
        `offset`, with `"path_start"` / `"path_end"` usable as ordinary names.
        A **list** on either side is a fan-out into separate questions (one row
        per combination), *not* `truncate_paths`' narrowest-window chain.

        Parameters
        ----------
        start_anchor : str or dict or list
            The condition — where the window opens. `occurrence` (default
            `"first"`) picks which occurrence of it counts.
        end_anchor : str or dict or list
            The target(s) looked for after it. The whole pattern has to fall
            after the start anchor, lead-in included.
        within : int or str or pd.Timedelta, optional
            Window size, measured from the start anchor and inclusive of its far
            edge: an int counts events (`10` — "within 10 events"), a duration
            counts time (`"30m"`, `pd.Timedelta`). `None` (default) looks to the
            end of the path.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

        Returns
        -------
        pd.DataFrame
            One row per pair, with columns:

            - `start_anchor`, `end_anchor` — the anchors' patterns.
            - `paths_with_start` — the denominator: paths where the start
              anchor resolved.
            - `converted` — of those, paths where the end anchor followed.
            - `conversion_rate` — `converted / paths_with_start`, or `NaN` when
              the start anchor never resolved (which is data, not an error).
            - `base_rate` — share of *all* paths containing the end anchor
              anywhere, the baseline the rate has to beat.
            - `lift` — `conversion_rate / base_rate`; `NaN` if `base_rate` is 0.
              Below 1 means the start event makes the outcome *less* likely.

        Examples
        --------
            stream.get_conversion_rate("add_to_cart", "purchase")

            # within a window, in events or in time
            stream.get_conversion_rate("product_view", "add_to_cart", within=10)
            stream.get_conversion_rate("support_chat", "churn", within="30m")

            # several targets at once, per session rather than per user
            stream.get_conversion_rate(
                "add_to_cart",
                ["purchase", "delivery_choice", "path_end"],
                path_col="session_id",
            )

            # exit rate: the path ended right after the event
            stream.get_conversion_rate("payment_error", "path_end", within=1)

            # only the sessions that *landed* on the page, not every visit to it
            stream.get_conversion_rate(
                {"pattern": "path_start->catalog", "at": -1}, "purchase"
            )
        """
        from retentioneering.tools.conversion import ConversionRate

        return ConversionRate(self).fit(
            start_anchor=start_anchor,
            end_anchor=end_anchor,
            within=within,
            path_col=path_col,
        )

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

        Rows are metrics, columns are segment levels. Click a cell to see that
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
            per-path values roll up across a segment. See the
            [Path Metrics](/docs/path-metrics) documentation page for the
            metric reference.
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
        Compute aggregated metrics across segment levels (headless).

        Parameters
        ----------
        segment_col : str
            Segment column to split by; must be one of `schema.segment_cols`.
        metrics : list of dict, optional
            Metric configurations, each with a `"metric"` key, optional
            `"metric_args"`, and an `"agg"` key (`"mean"`, `"median"`, `"q5"`,
            `"q25"`, `"q75"`, `"q95"`, or `"complement_distance"`) controlling how
            per-path values roll up across a segment. See the
            [Path Metrics](/docs/path-metrics) documentation page for the
            metric reference.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event name column override; defaults to `schema.event_col`.

        Returns
        -------
        pd.DataFrame
            Metrics as rows and segment levels as columns. Always includes
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
        method_args: dict | None = None,
        scaler: str | None = None,
        overview_metrics: list | None = None,
        path_col: str | None = None,
        select: dict | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
        state_file: str | None = None,
    ):
        """
        An interactive tool for finding an optimal splitting of paths by behavioral metrics.
        Allows you to inspect clusters in a [Segment Overview](/docs/widgets/segment-overview)-style heatmap
        and offers the best possible splitting from the silhouette score perspective.
        When a grid is searched, the Silhouette tab charts every candidate and you can
        click any bar to interpret that partition instead of the top-scoring one —
        useful when several candidates score alike and the winner doesn't read well.
        Everything downstream follows the pick, including the code shown and the
        segment saved by "Save Clusters".
        Once the splitting looks right, you can label the clusters and save them as a new segment column
        of the eventstream right from the UI by clicking "Save Clusters".

        Parameters
        ----------
        features : list of dict, optional
            Metric configurations used as clustering features (see the [Path
            Metrics](/docs/path-metrics)). If omitted, the sidebar starts
            pre-filled with a wildcard `event_count_bulk` metric (one column
            per event in the eventstream) — that pre-fill is a starting point
            to edit, not something that runs on its own: passing `features`
            explicitly (or clicking "Apply" in the sidebar) is what actually
            triggers clustering.
        method : {"kmeans", "hdbscan"}, default "kmeans"
            Clustering algorithm.
        method_args : dict, optional
            Parameters of the chosen `method`, same shape and schema as
            `cluster_analysis_data`:

            - `"kmeans"` — `n_clusters` (int, list of int, or a range string like
              `"3-8"`; a list or range runs a silhouette-scored grid search).
              Defaults to `"3-8"`. This is the one the sidebar also edits, so a
              value passed here is the starting point, not a lock.
            - `"hdbscan"` — `min_cluster_size`, `cluster_selection_epsilon`. The
              sidebar has no fields for these, so passing them here is the only
              way to set them.
        scaler : {"minmax", "std"}, optional
            Feature scaler applied before clustering; default `"minmax"`.
        overview_metrics : list of dict, optional
            Metrics shown in the overview heatmap after clustering (independent
            of `features`). If omitted, the sidebar starts pre-filled with a
            wildcard `event_count_bulk` metric here too (mean count per event);
            same as `features`, it only takes effect once you click "Apply" or
            pass the argument explicitly.
            Both `features` and `overview_metrics` accept metric configs from the
            same [Path Metrics](/docs/path-metrics) registry.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        select : dict, optional
            Which grid point the widget opens on, e.g. `{"n_clusters": 5}`; by
            default the top-scoring one. Its keys are bare parameter names, as
            they appear in `silhouette["params"]` — a grid point is a coordinate,
            not a call. Only meaningful when a `method_args` value (or
            `nmf_components`) is a range, and equivalent to clicking that bar in
            the Silhouette tab. Persisted with the rest of the widget state.
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
                method_args={"n_clusters": "3-6"},
            )

            stream.cluster_analysis(
                features=[{"metric": "length"}],
                method="hdbscan",
                method_args={"min_cluster_size": 50},
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
            method_args=method_args if method_args is not None else _UNSET,
            scaler=scaler if scaler is not None else _UNSET,
            overview_metrics=overview_metrics
            if overview_metrics is not None
            else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            height=height if height is not None else _UNSET,
            sidebar_open=sidebar_open if sidebar_open is not None else _UNSET,
            select=select if select is not None else _UNSET,
            state_file=state_file,
        )

    @_tracked("headless_cluster_analysis")
    def cluster_analysis_data(
        self,
        features: list,
        method: str = "kmeans",
        method_args: dict | None = None,
        scaler: str | None = "minmax",
        nmf_components=None,
        overview_metrics: list | None = None,
        path_col: str | None = None,
        event_col: str | None = None,
        select: dict | None = None,
    ) -> dict:
        """
        Run cluster analysis headlessly and return a dict of results.

        Pass lists inside `method_args` (or for `nmf_components`) to trigger grid
        search with silhouette scoring. For the kmeans method (the default),
        `n_clusters` defaults to `"3-8"` if omitted — including for
        nmf_components-only searches.

        `best_params` holds the concrete parameter values actually used to produce
        `overview_df` (the winning combination when searching, the point named by
        `select` if you named one, or just the fixed values passed in otherwise),
        already shaped as `add_clusters` keyword arguments (`method`,
        `method_args`, `scaler`, and `nmf_components` if one was used) — splat it
        into `add_clusters` to materialize the same clustering as a segment
        column.

        Parameters
        ----------
        features : list of dict
            Metric configurations used as clustering features (see the Path
            Metrics documentation page). Required — there is no interactive
            sidebar here to pick them for you, unlike the `cluster_analysis`
            widget.
        method : {"kmeans", "hdbscan"}, default "kmeans"
            Clustering algorithm.
        method_args : dict, optional
            Parameters of the chosen `method`. Every value may be a single value
            or a list, and any list triggers a silhouette-scored grid search over
            it:

            - `"kmeans"` — `n_clusters` (int, list of int, or a range string like
              `"3-8"`). Defaults to `"3-8"`, i.e. a grid search.
            - `"hdbscan"` — `min_cluster_size` (int, default 5),
              `cluster_selection_epsilon` (float, default 0.0).

            A key that does not belong to `method` raises rather than being
            ignored.
        scaler : {"minmax", "std"}, optional
            Feature scaler applied before clustering; default `"minmax"`.
        nmf_components : int or list of int, optional
            Number of components for an optional NMF (non-negative matrix
            factorization) step applied to the scaled features before
            clustering; if omitted, NMF is skipped. A list triggers a
            silhouette-scored grid search over the given values.
        overview_metrics : list of dict, optional
            Metrics shown in the overview heatmap after clustering (independent
            of `features`); if omitted, `overview_df` only has segment_size and
            segment_share rows. Both `features` and `overview_metrics` accept
            metric configs from the same [Path Metrics](/docs/path-metrics) registry.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
        event_col : str, optional
            Event name column override; defaults to `schema.event_col`.
        select : dict, optional
            Which grid point to interpret, given as the parameter values naming
            it — `{"n_clusters": 5}`, or `{"n_clusters": 5, "nmf_components": 3}`
            when more than one parameter was searched. The keys are the same ones
            that appear in `silhouette["params"]`, so picking a point is a matter
            of copying an entry from there.

            Only valid in search mode. By default the highest-silhouette point is
            the one turned into `overview_df` / `cluster_labels`; `select`
            interprets a different one while keeping the whole grid in
            `silhouette`, which is what you want when several candidates score
            alike and the best-scoring one doesn't read well.

            A subset of the keys is enough as long as it picks out exactly one
            point — naming no point raises `GridPointNotFoundError`, and naming
            several raises `AmbiguousGridPointError` rather than silently taking
            one of them.

        Returns
        -------
        dict
            - `overview_df`: `DataFrame` from the segment overview heatmap,
              one row per path segmented by cluster label.
            - `cluster_labels`: `Series` of the cluster label assigned to each
              path, indexed by `path_col`.
            - `best_params`: the parameter values used to produce `overview_df`,
              shaped as `add_clusters` keyword arguments — splat straight into it.
            - `nmf`: `None` if `nmf_components` was not passed (or, in a grid
              search, if no candidate used NMF); otherwise a dict with
              `H_matrix`, `features`, and `W_cluster_means`.
            - `silhouette`: only present when a list was passed for a
              `method_args` value or for `nmf_components` (grid search mode). A dict of two
              parallel lists — `{"params": [{"n_clusters": 3}, ...],
              "silhouette": [0.87, ...]}` — one entry per candidate tried;
              zip them to inspect individual scores. Also carries `best_index`
              (the highest-scoring point) and `selected_index` (the point
              `select` named, or `None`) — they differ exactly when `select`
              overrode the winner. `overview_df`,
              `cluster_labels`, and `best_params` are omitted in this mode if
              every candidate was degenerate (fewer than 2 valid clusters).
        """
        from retentioneering.tools.cluster_analysis import ClusterAnalysis

        return ClusterAnalysis(self).fit(
            features_config=features,
            method=method,
            method_args=method_args,
            scaler=scaler,
            nmf_components=nmf_components,
            overview_metrics=overview_metrics,
            path_col=path_col,
            event_col=event_col,
            select=select,
        )

    @_tracked("get_metric_distribution")
    def get_metric_distribution(
        self,
        segment_col: str,
        metric: dict,
        *,
        segment_level=_SEGMENT_LEVEL_UNSET,
        segment_levels: list | None = None,
        path_col: str | None = None,
    ) -> dict:
        """
        Histogram/KDE distribution of a per-path metric, compared across two groups.

        Exactly one of `segment_level` or `segment_levels` must be provided —
        they are the two ways of naming the pair being compared.

        Parameters
        ----------
        segment_col : str
            Segment column the levels are read from.
        metric : dict
            Metric config, `{"metric": ..., "metric_args": ...}`, producing exactly
            one value per path. See the [Path Metrics](/docs/path-metrics) page.
        segment_level : optional
            One level, compared against **its complement** — every other level of
            `segment_col` taken together. `None` is a level in its own right (paths
            whose segment value is missing), which is why omitting the argument,
            not passing `None`, is what selects the other mode.
        segment_levels : list, optional
            Exactly two levels, compared against each other.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

        Returns
        -------
        dict
            `distribution_1`, `distribution_2` (each with `bins`, `counts`,
            `counts_normalized`, `kde`, `mean`, `median`), `distance` (Wasserstein
            distance between them) and `log_scale`. With `segment_level`,
            `distribution_2` is the complement.

        Examples
        --------
            stream.get_metric_distribution("cluster", {"metric": "length"}, segment_level="cluster_0")
            stream.get_metric_distribution(
                "cluster", {"metric": "duration"}, segment_levels=["cluster_0", "cluster_1"]
            )
        """
        from retentioneering.tools.segment_overview import SegmentOverview

        return SegmentOverview(self).get_metric_distribution(
            segment_col=segment_col,
            metric=metric,
            segment_level=segment_level,
            segment_levels=segment_levels,
            path_col=path_col,
        )
