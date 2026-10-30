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


def _numpydoc_split(doc: str) -> tuple[str, dict[str, str]]:
    """Split a numpy-style docstring into its summary and ``{section: body}``.

    Used to build the docstrings of `ab_test` and `ab_test_data` from the one of
    `get_ab_test`, so the widget, its headless twin and the getter can never
    document the same parameter differently.
    """
    lines = inspect.cleandoc(doc).splitlines()
    summary: list[str] = []
    sections: dict[str, list[str]] = {}
    current = summary
    i = 0
    while i < len(lines):
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        if lines[i].strip() and nxt.strip() and set(nxt.strip()) == {"-"}:
            current = sections.setdefault(lines[i].strip(), [])
            i += 2
            continue
        current.append(lines[i])
        i += 1
    return "\n".join(summary).strip(), {
        name: "\n".join(body).strip("\n") for name, body in sections.items()
    }


def _numpydoc_join(summary: str, sections: list[tuple[str, str]]) -> str:
    parts = [summary.strip()]
    for name, body in sections:
        parts.append(f"{name}\n{'-' * len(name)}\n{body.strip(chr(10))}")
    return "\n\n".join(parts) + "\n"


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
        },
    )
    def _post_init(self):
        if self.preprocess:
            self._preprocess()
        else:
            for col in [self.schema.event_col] + self.schema.segment_cols:
                self._df[col] = self._df[col].astype("category")

        schema = self.schema
        declared_cols = set(
            schema.path_cols
            + [schema.event_col]
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

        for col in [schema.event_col] + schema.segment_cols:
            df[col] = df[col].astype("category")

        offenders = find_delimiter_collisions(
            df[schema.event_col].cat.categories.tolist()
        )
        if offenders:
            raise SchemaConfigError(
                f"Event name(s) {offenders} in column '{schema.event_col}' contain "
                f"'->', which retentioneering uses as the path delimiter in "
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

    def get_event_counts(self) -> dict[str, int]:
        event_col = self.schema.event_col
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
                    "event_col": s.event_col,
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

    @_tracked("get_path_segments")
    def get_path_segments(
        self, segment_col: str, path_col: str | None = None
    ) -> pd.Series:
        """
        Return one segment value per path.

        The per-path view of a segment column: use it to carry labels computed on
        one stream onto another (`add_segment(name, path_values=...)`), to check
        that an experiment arm is constant per path, or to join path labels to
        your own tables.

        Parameters
        ----------
        segment_col : str
            A column in `schema.segment_cols`.
        path_col : str, optional
            Path grain; defaults to `schema.path_col`.

        Returns
        -------
        pandas.Series
            Index = path id (sorted, named after `path_col`), name = `segment_col`,
            values = the segment value; `"<MULTI>"` when the value is not constant
            within the path (a missing value on some events counts as a second
            value), `"<MISSING>"` when it is null throughout. When this stream's
            recipe shows how the column was made, `attrs["pre_exposure"]` records it:
            True for a column computed from behaviour (`add_clusters`,
            `add_segment(metric_bins=...)`) after a `truncate_paths`, False for one
            computed without a truncation. `add_segment(path_values=...)` carries the
            flag to another stream, where `get_ab_test(strata_col=...)` reads it.

        Raises
        ------
        InvalidParameterError
            `segment_col` is not a segment column, or `path_col` is not a path column.

        Examples
        --------
            arms = stream.get_path_segments("exp_group")
            arms.value_counts()          # paths per arm, <MULTI> = assignment bug

            # clusters found on the first week, carried onto the whole history
            clusters = first_week.add_clusters(...).get_path_segments("cluster")
            stream.add_segment("cluster", path_values=clusters)
        """
        from retentioneering.exceptions import InvalidParameterError
        from retentioneering.tools._diff import per_path_label

        if segment_col not in self.schema.segment_cols:
            raise InvalidParameterError(
                "segment_col", segment_col, list(self.schema.segment_cols)
            )
        path_col = path_col or self.schema.path_col
        if path_col not in self.schema.path_cols:
            raise InvalidParameterError(
                "path_col", path_col, list(self.schema.path_cols)
            )
        from retentioneering.tools.ab_test import _strata_provenance

        labels = per_path_label(self._df, segment_col, path_col)
        labels.name = segment_col
        pre_exposure = _strata_provenance(self, segment_col)
        if pre_exposure is not None:
            labels.attrs["pre_exposure"] = pre_exposure
        return labels

    @_tracked("get_sample_ratio")
    def get_sample_ratio(
        self,
        segment_col: str | None = None,
        expected_shares: dict | None = None,
        alpha: float = 0.001,
        path_col: str | None = None,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        strata_col: str | None = None,
        period: str | None = None,
    ) -> dict:
        """
        Check an experiment's arm sizes against the expected split (sample ratio mismatch).

        Counts paths per arm and runs a chi-square goodness-of-fit test against the
        expected shares. A mismatch means the assignment, the logging or a filter
        lost paths from one arm, and no metric of the test can be trusted until it is
        explained. Paths whose arm changes within the path are an assignment bug and
        also raise the alarm.

        Run it on the assignment table (`assignment=`) or on the full stream before
        any filter that depends on behaviour. On a post-exposure subset (users who
        reached the cart) the shares differ whenever the treatment works: that is an
        effect, not a bug, and this check does not apply there.

        Parameters
        ----------
        segment_col : str, optional
            Segment column holding the arm (`schema.segment_cols`); required without
            `assignment`. With `assignment` it is optional: `None` or `"arm"` (the
            table's column) checks the table alone; a segment column of the stream
            adds the check that every path's arm on the stream agrees with the
            table. Any other value raises `InvalidParameterError`.
        expected_shares : dict, optional
            `{level: share}`, shares summing to 1 (within 1e-9), at least two levels.
            Default: an equal split over the column's levels (or the table's arms).
            Levels not named here are not arms; their paths are counted in
            `unassigned_paths`.
        alpha : float, default 0.001
            Threshold of `mismatch`. SRM alarms are set stricter than 0.05 because
            they gate every other result.
        path_col : str, optional
            Path grain; defaults to `schema.path_col`.
        assignment : pandas.Series or pandas.DataFrame, optional
            Per-path arm table: a Series `path_id -> arm`, or a DataFrame indexed by
            path id with a column `arm` (and optionally `assigned_at`). The counts are
            the table's rows, including paths with no events in the stream. Ids are
            matched to `path_col` as in `get_ab_test` (by string form when none
            matches as is; none matching raises, fewer than half warns), and a table
            of a coarser unit than `path_col` (user ids with session paths) raises
            `InvalidParameterError`.
        strata_col : str, optional
            Repeat the check within strata, as `get_ab_test(strata_col=)` names
            them: a column of the `assignment` DataFrame (used first) or a segment
            column constant per path. Valid only for a variable fixed at assignment
            (signup platform): on a variable the treatment can change, per-stratum
            shares measure an effect. Raises `InvalidParameterError` for a segment
            that changes within a path.
        period : str, optional
            Repeat the check per calendar unit of `assigned_at` (or of the path's
            first event without a table), spelled as `get_ab_test(period=)`: `"D"`
            (day), `"W"` (week; `"W-MON"` etc. pick the week's end), `"M"`, `"Q"`,
            `"Y"`. A multiple (`"2W"`) or another fixed length raises
            `InvalidParameterError`; so does `period` together with `strata_col`.

        Returns
        -------
        dict
            - `levels` (DataFrame): one row per arm — `level`, `paths`, `share`,
              `expected_share`, `expected_paths`. With `strata_col` (`period`), a
              leading `stratum` (`period`) column; the overall rows carry
              `"<ALL>"` there, followed by the rows of every stratum (period).
            - `chi2` (float), `dof` (int), `p_value` (float): the overall test; NaN
              when no path is counted.
            - `alpha` (float): the threshold used.
            - `mismatch` (bool): `p_value < alpha` or `multi_valued_paths > 0`.
            - `multi_valued_paths` (int): paths with more than one arm value (on the
              stream, or disagreeing with the assignment table).
            - `silent_paths` (int): assignment rows with no events in the stream (0
              without `assignment`).
            - `unassigned_paths` (int): paths not counted in the test: stream paths
              the assignment table does not list, paths with no arm (a missing
              segment value or table arm), and paths of an arm outside
              `expected_shares`.
            - `basis` (str): what was counted.
            - `reading` (str): the check in one sentence, built from the numbers
              above: `"expected 50/50, observed 5040/4960 (control/treatment),
              p 0.42: no mismatch"`, or `"mismatch: ... — the test is invalid until
              the assignment is fixed"` when the split is off or paths carry two
              arms; paths not counted and the strata or periods that mismatch
              (`"by platform: ..."`, `"by day: ..."`) are added.
            - `strata_col` (str) and `strata` (DataFrame), with `strata_col` only:
              one row per stratum — `stratum`, `paths`, `chi2`, `dof`, `p_value`,
              `p_value_adjusted` (Holm over the strata) and `mismatch`
              (`p_value_adjusted < alpha`).
            - `period` (str) and `periods` (DataFrame), with `period` only: the same
              per period, with a `period` column (`"2024-03-01"` for days,
              `"2024-03-04/2024-03-10"` for weeks, `"2024-03"` for months).
            The overall `mismatch` does not include the strata or periods.

        Raises
        ------
        InvalidParameterError
            `segment_col` missing or not a segment column (without `assignment`),
            or neither `None`, `"arm"` nor a segment column (with it); fewer than
            two arms; `expected_shares` not summing to 1 or naming an unknown level;
            `alpha` outside (0, 1); `strata_col` not a per-path segment or table
            column; `period` not one calendar unit, or given with `strata_col`;
            `path_col` finer than the assignment table's unit; `assignment`
            malformed (no `arm` column, empty, index not unique) or no id of it
            matching a path id.

        Examples
        --------
            check = stream.get_sample_ratio("exp_group")
            check["mismatch"], check["p_value"]
            check["levels"]

            # an unequal design, and the assignment table as the population
            stream.get_sample_ratio(
                "exp_group",
                expected_shares={"control": 0.8, "treatment": 0.2},
                assignment=groups,          # index user_id, columns arm, assigned_at
            )

            # did the split hold on every day of the rollout, and on every platform?
            stream.get_sample_ratio("exp_group", period="D")["periods"]
            stream.get_sample_ratio(assignment=groups, strata_col="platform")["strata"]
        """
        from retentioneering.tools.sample_ratio import SampleRatio

        return SampleRatio(self).fit(
            segment_col,
            expected_shares=expected_shares,
            alpha=alpha,
            path_col=path_col,
            assignment=assignment,
            strata_col=strata_col,
            period=period,
        )

    @_tracked("get_ab_test")
    def get_ab_test(
        self,
        diff: T_Diff,
        metrics: list,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        strata_col: str | None = None,
        period=None,
        period_basis: str = "calendar",
        period_population: str = "itt",
        horizon=None,
        test: str = "auto",
        correction: str = "holm",
        correction_scope: str = "metric",
        ci_level: float = 0.95,
        power: float = 0.8,
        n_bootstrap: "int | str" = 1000,
        bootstrap_weights: str = "multinomial",
        min_support: int = 10,
        random_state: int = 42,
        path_col: str | None = None,
        strata_pre_exposure: bool | None = None,
    ) -> pd.DataFrame:
        """
        Compare two arms of an experiment on path metrics: a scorecard.

        One row per metric (and per stratum and period when requested): each arm's
        estimate, the delta with its interval, the p-value and its adjusted value, the
        minimum detectable effect, a verdict and a `reading` sentence. The path
        (`path_col`) must be the randomised unit. An inconclusive row is not "no
        effect": its interval shows the effects the data still allow. Choose the window
        and one decision metric before reading. No variance reduction, sequential tests
        or equivalence verdicts: `get_ab_paths` returns the per-path table behind the
        scorecard; the [A/B Testing guide](/docs/ab-testing) lists what is deferred.

        Parameters
        ----------
        diff : tuple
            `("segment_col", group1, group2)` or `(ids1, ids2)`, resolved per path;
            `delta = group1 - group2`. A path in both groups, or two strings, raise
            `DiffConfigError`. A segment that keeps one arm in at least 99 % of the
            users with several sessions (at least 10 users) raises
            `InvalidParameterError` at the session grain: per-session intervals of a
            per-user experiment are too narrow.
        metrics : list of dict
            Registry configs (as for `get_metrics`, one column per path) with optional
            `name`, `denominator` (a config: a ratio of sums, e.g. revenue / orders)
            and `test`. `*_bulk`, `first_event_time` and a `denominator` on a 0/1
            metric are rejected. A path with no events counts 0 for 0/1 metrics
            (`has_event`, `has_any_event`, `has_all_events`, `matches_pattern`,
            `in_segment`) and totals (`event_count`, `length`, `active_days`,
            `column_sum`); values (`duration`, `time_between`, `column_mean`,
            `column_last`) leave it out.
        assignment : pandas.Series or pandas.DataFrame, optional
            Per-path arm table, the intention-to-treat population: a Series `path_id ->
            arm` or a DataFrame with `arm` and optionally `assigned_at`. Silent assigned
            paths count; events before `assigned_at` are dropped; paths absent from the
            table are `unassigned_paths`. `diff[0]` is `"arm"` or a segment that must
            agree with the table. Ids matching no path raise, fewer than half warn. A
            naive `assigned_at` is read in the stream's timezone (a wall time that
            occurs twice raises). A table of another unit than `path_col` raises
            `InvalidParameterError`.
        strata_col : str, optional
            A segment constant per path, or an `assignment` column (used first, so
            silent paths keep their stratum). Adds per-stratum rows with balance,
            heterogeneity and stratum-vs-rest tests.
        strata_pre_exposure : bool, optional
            Whether the strata were fixed before exposure. `None` reads an
            `add_segment(..., pre_exposure=...)` declaration, else the recipe (a
            `truncate_paths` ending paths at an event), else False with a warning;
            `True` / `False` declare it. Needs `strata_col`.
        period : str or pandas offset, optional
            Rows per period: a calendar unit (`"D"`, `"W"`, `"M"`, `"Q"`, `"Y"`;
            `"W-MON"` etc. pick the week's end, `"ME"` / `"MS"` mean `"M"`; multiples
            like `"2W"`) or a fixed length (`"7D"`, `"12h"`), in the stream's
            timezone. A bare number raises. Needs the primary path column.
        period_basis : {"calendar", "since_assignment"}, default "calendar"
            What `period` bins.

            - `"calendar"` — calendar bins of the event time; a period holds every
              path assigned before its end.
            - `"since_assignment"` — bins of time since `assigned_at` (`"0-6"`,
              `"7-13"`); a path counts once its window closes (else
              `immature_paths`).
        period_population : {"itt", "active"}, default "itt"
            Which paths a period row compares.

            - `"itt"` — every path assigned by the period's end, 0 when silent.
            - `"active"` — paths active in the period: survivors, so
              `verdict="descriptive"`, no p-value.
        horizon : str or pandas.Timedelta, optional
            Window per path from `assigned_at`, e.g. `"28D"`; paths whose window is not
            closed by the last event are `immature_paths`. Needs `assigned_at`.
        test : {"auto", "z_test", "welch_t_test", "bootstrap"}, default "auto"
            The test of every metric without its own `test`.

            - `"auto"` — `"z_test"` for 0/1 metrics, `"welch_t_test"` for totals and
              values, `"bootstrap"` for ratios.
            - `"z_test"` — pooled z-test, Wilson and Newcombe intervals (0/1 only).
            - `"welch_t_test"` — Welch's t-test (not for ratios).
            - `"bootstrap"` — joint path bootstrap, percentile intervals.
        correction : {"holm", "bonferroni", "bh", "none"}, default "holm"
            Correction of the p-values within each family; intervals stay per
            comparison.

            - `"holm"` — Holm step-down (family-wise error).
            - `"bonferroni"` — Bonferroni; adds simultaneous `delta_ci_low_adj` /
              `delta_ci_high_adj` (normal on the bootstrap SE for ratio metrics).
            - `"bh"` — Benjamini-Hochberg (false discovery rate).
            - `"none"` — no correction.
        correction_scope : {"metric", "all"}, default "metric"
            What one correction family holds.

            - `"metric"` — one metric's rows (strata, periods). Name one decision metric
              before the test; read each guardrail on its own against a declared harm
              margin.
            - `"all"` — every row of every metric, for a scorecard read without one
              decision metric.
        ci_level : float, default 0.95
            Level of every interval, in `[0.5, 1)`; `1 - ci_level` is the verdict's
            alpha.
        power : float, default 0.8
            Power used for `mde`.
        n_bootstrap : int or "auto", default 1000
            Bootstrap replicates, at least 100; `"auto"` = `max(1000, ceil(20 * m /
            alpha))`, `m` the largest family.
        bootstrap_weights : {"multinomial", "poisson"}, default "multinomial"
            Resampling scheme of the bootstrap.

            - `"multinomial"` — resample each arm's paths with replacement.
            - `"poisson"` — a Poisson(1) weight per path.
        min_support : int, default 10
            Pooled paths below which a row is `"insufficient_data"` (no p-value):
            paths with the event (0/1; the paths without it too), a non-zero total
            (totals), a non-zero numerator (ratios) or a value (values). A behaviour
            of one arm only is tested and flagged `"one_arm_only"`.
        random_state : int, default 42
            Seed of the bootstrap.
        path_col : str, optional
            Path grain; defaults to `schema.path_col`.

        Returns
        -------
        pandas.DataFrame
            One row per metric × stratum × period; * marks columns that only some
            arguments produce.

            - `metric`; `stratum`*; `period`*, `period_start`*, `period_end`*,
              `period_complete`* (False when the data or `horizon` cuts the window).
            - `population`: `"assignment"`, `"stream"` or `"active"`.
            - `group1_paths`, `group2_paths`; `group1_support`, `group2_support`
              (paths with a value); `conditional` (a support below its arm's paths:
              the row compares the paths with a value).
            - `group1_silent_paths`*, `group2_silent_paths`*, `unassigned_paths`,
              `immature_paths`*.
            - `group1_value`, `group1_ci_low`, `group1_ci_high`, `group2_value`,
              `group2_ci_low`, `group2_ci_high`.
            - `delta`, `delta_se`, `delta_ci_low`, `delta_ci_high` (per comparison);
              `delta_ci_low_adj`*, `delta_ci_high_adj`* (Bonferroni, simultaneous);
              `rel_delta`, `rel_delta_ci_low`, `rel_delta_ci_high`.
            - `p_value`, `p_value_adjusted`, `correction_family`.
            - `mde`, `rel_mde` at `mde_alpha` (`alpha / m` under a correction of `m`
              p-values; not the verdict's threshold).
            - `verdict`: `"increase"` / `"decrease"` when `p_value_adjusted < 1 -
              ci_level`, else `"inconclusive"`; `"descriptive"`;
              `"insufficient_data"`.
            - `test`, `ci_basis` (`"wilson"`, `"welch"`, `"path_bootstrap"`),
              `p_basis` (`"pooled_z"`, `"welch"`, `"bootstrap_se"`),
              `valid_replicates`.
            - `support_warning`: `"no_data"` (nothing to compare), `"invalid_replicates"`
              (no interval), `"one_arm_only"` (tested), `"low_support"` (bootstrap
              coverage below nominal) or `""`.
            - `group1_share`*, `group2_share`*, `balance_p_value`*,
              `heterogeneity_p_value`*, `heterogeneity_dof`*,
              `heterogeneity_strata_used`*, `stratum_vs_rest_p_value`*,
              `strata_pre_exposure`*, `strata_pre_exposure_source`*, `exploratory`*.
            - `reading`: the row in one sentence from its own numbers; it does not
              check the sample ratio or the unit (see the
              [tool page](/docs/tools/get-ab-test#reading-a-row)).

            An absent estimate is NaN, never 0.

        Raises
        ------
        DiffConfigError
            Overlapping groups, the same level twice, or a `diff` that disagrees with
            the `assignment` table.
        SegmentLevelNotFoundError
            A `diff` value that is not a level of the segment or an arm of the table.
        InvalidMetricConfigError
            A bad metric entry or a `test` that does not fit the metric.
        InvalidParameterError
            A value outside its range or list; a path grain finer than the randomised
            unit; `horizon` or `"since_assignment"` without `assigned_at`.
        EmptyEventstreamError
            Either group is empty.

        Examples
        --------
            scorecard = stream.get_ab_test(
                ("exp_group", "treatment", "control"),
                metrics=[
                    {"name": "conversion", "metric": "has_event",
                     "metric_args": {"event": "order_success"}},
                    {"name": "revenue_per_user", "metric": "column_sum",
                     "metric_args": {"column": "order_value"}},
                    {"name": "aov", "metric": "column_sum",
                     "metric_args": {"column": "order_value"},
                     "denominator": {"metric": "event_count",
                                     "metric_args": {"event": "order_success"}}},
                ],
                assignment=assignment,          # index user_id, columns arm, assigned_at
            )
            scorecard[["metric", "delta", "delta_ci_low", "delta_ci_high", "mde", "verdict"]]

            # pre-exposure strata, Holm within each metric: cluster the paths cut
            # before the exposure event, carry the labels back, declare them
            before = stream.truncate_paths(
                "path_start", [{"pattern": "product_view", "offset": -1}, "path_end"])
            labels = before.add_clusters(...).get_path_segments("cluster")
            stream = stream.add_segment("entry", path_values=labels, pre_exposure=True)
            stream.get_ab_test(diff, metrics, assignment=assignment, strata_col="entry")

            # one family over every metric: a scorecard read without one decision metric
            stream.get_ab_test(diff, metrics, correction_scope="all")

            # weekly ITT rows, and a fixed 28-day window per user
            stream.get_ab_test(diff, metrics, assignment=assignment, period="W")
            stream.get_ab_test(diff, metrics, assignment=assignment, horizon="28D")
        """
        from retentioneering.tools.ab_test import ABTest

        return ABTest.from_stream(
            self,
            diff,
            metrics,
            assignment=assignment,
            strata_col=strata_col,
            period=period,
            period_basis=period_basis,
            period_population=period_population,
            horizon=horizon,
            test=test,
            correction=correction,
            correction_scope=correction_scope,
            ci_level=ci_level,
            power=power,
            n_bootstrap=n_bootstrap,
            bootstrap_weights=bootstrap_weights,
            min_support=min_support,
            random_state=random_state,
            path_col=path_col,
            strata_pre_exposure=strata_pre_exposure,
        ).fit()

    @_tracked("get_ab_bootstrap")
    def get_ab_bootstrap(
        self,
        diff: T_Diff,
        metrics: list,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        strata_col: str | None = None,
        period=None,
        period_basis: str = "calendar",
        period_population: str = "itt",
        horizon=None,
        contrast: dict | None = None,
        correction_scope: str = "metric",
        n_bootstrap: "int | str" = 1000,
        bootstrap_weights: str = "multinomial",
        random_state: int = 42,
        path_col: str | None = None,
        strata_pre_exposure: bool | None = None,
    ) -> pd.DataFrame:
        """
        Return joint path-bootstrap replicates of every metric of an experiment.

        All metrics, strata and periods are evaluated on the same resampled paths, so
        the replicates can be combined freely: ratios of metrics, differences between
        periods, custom summaries such as the probability that one arm is better. The
        replicates are the ones `get_ab_test` uses for its bootstrap rows (same seed,
        same engine). `contrast` adds rows for named period contrasts, the way to
        measure a novelty effect with a joint interval.

        Parameters
        ----------
        diff, metrics, assignment, strata_col, period, period_basis : see get_ab_test
            As in `get_ab_test`.
        period_population, horizon, bootstrap_weights, random_state, path_col : see get_ab_test
            As in `get_ab_test`.
        strata_pre_exposure : bool, optional
            As in `get_ab_test` (here it only decides the `UserWarning`).
        contrast : dict, optional
            `{"name": (periods_a, periods_b)}` (period labels): for each metric (and
            stratum), rows with `group*_value = value(a) - value(b)` within each arm
            and `delta = group1_value - group2_value`, where each side pools the listed
            periods (summed numerators over summed denominators). Both sides run on
            the same paths: those in the population of every contrasted period (with
            since-assignment bins, the paths whose windows of all contrasted bins
            close by the stream's last event), so the contrast is a change within the
            same paths, not a change of cohort mix between early and late bins; a
            `horizon` is a choice of window, not a requirement. Needs `period`.
            Example: `{"novelty": (["0-13"], ["28-41"])}` with `period="14D",
            period_basis="since_assignment"`.
        correction_scope : {"metric", "all"}, default "metric"
            As in `get_ab_test`; here it only sets the family size `m` of
            `n_bootstrap="auto"`, so the replicates match the scorecard's.
        n_bootstrap : int or "auto", default 1000
            Number of replicates, at least 100; `"auto"` as in `get_ab_test` (the
            replicates `get_ab_test` would draw at `ci_level=0.95`).

        Returns
        -------
        pandas.DataFrame
            Long table, one row per replicate × metric × stratum × period (and per
            replicate × metric × stratum × contrast):

            - `replicate` (int): replicate number, 0 to `n_bootstrap - 1`.
            - `metric` (str): metric name.
            - `stratum`* (str): with `strata_col`.
            - `period`* (str): with `period`; NaN on contrast rows.
            - `contrast`* (str): with `contrast`; NaN on plain rows.
            - `group1_value`, `group2_value` (float): the metric in each arm in this
              replicate (ratio of weighted sums; the mean for 0/1, totals and values).
            - `delta` (float): `group1_value - group2_value`.

            NaN where a replicate has no support in a cell (no path with a value or a
            zero weighted denominator). Replicate rows carry no `reading`; the
            sentence that reads each metric is on its `get_ab_test` row.

        Raises
        ------
        DiffConfigError, InvalidMetricConfigError, InvalidParameterError, EmptyEventstreamError
            As in `get_ab_test`; `contrast` without `period` or naming an unknown period
            raises `InvalidParameterError`.

        Examples
        --------
            reps = stream.get_ab_bootstrap(
                ("exp_group", "treatment", "control"),
                metrics=[{"name": "conversion", "metric": "has_event",
                          "metric_args": {"event": "order_success"}}],
                assignment=assignment,
                period="14D",
                period_basis="since_assignment",
                contrast={"novelty": (["0-13"], ["28-41"])},
            )
            novelty = reps[reps["contrast"] == "novelty"]["delta"]
            novelty.quantile([0.025, 0.975])
        """
        from retentioneering.tools.ab_test import ABTest

        return ABTest.from_stream(
            self,
            diff,
            metrics,
            assignment=assignment,
            strata_col=strata_col,
            period=period,
            period_basis=period_basis,
            period_population=period_population,
            horizon=horizon,
            correction_scope=correction_scope,
            n_bootstrap=n_bootstrap,
            bootstrap_weights=bootstrap_weights,
            random_state=random_state,
            path_col=path_col,
            strata_pre_exposure=strata_pre_exposure,
        ).bootstrap(contrast)

    @_tracked("get_ab_paths")
    def get_ab_paths(
        self,
        diff: T_Diff,
        metrics: list,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
    ) -> pd.DataFrame:
        """
        Return the per-path table behind an A/B scorecard: one row per compared path.

        Each row holds the path's arm and the value of every metric in the window
        `get_ab_test` reads, computed the same way: silent paths count 0 for 0/1
        metrics and totals, values are NaN where undefined, ratio metrics come as
        numerator and denominator. Aggregating it per arm gives the scorecard's
        `group1_value` / `group2_value` (the mean of a 0/1 metric, a total or a
        value over the paths with one; the summed numerator over the summed
        denominator of a ratio). Use it for what the scorecard does not do:
        variance reduction (CUPED, regression on pre-period covariates), quantile
        or rank tests, custom plots and power calculations from history.

        Parameters
        ----------
        diff, metrics : see get_ab_test
            As in `get_ab_test`: the arms and the metric configs (with `name` and
            `denominator`).
        assignment : pandas.Series or pandas.DataFrame, optional
            As in `get_ab_test`: the intention-to-treat population (silent paths
            included, events before `assigned_at` dropped).
        horizon : str or pandas.Timedelta, optional
            As in `get_ab_test`: only events in `[assigned_at, assigned_at +
            horizon)` count; paths whose window is not closed by the stream's last
            event stay in the table with `mature=False` and NaN metric values.
        path_col : str, optional
            Path grain; defaults to `schema.path_col` (the unit guard of
            `get_ab_test` applies).

        Returns
        -------
        pandas.DataFrame
            Index: path id (named after `path_col`), sorted; both arms. Columns:

            - `arm` (str): the arm label, `diff[1]` or `diff[2]` (`"group1"` /
              `"group2"` for id lists).
            - `assigned_at` (Timestamp): with an `assignment` DataFrame that has
              it, in the stream's timezone.
            - `silent` (boolean): no event in the analysed window (`<NA>` for an
              immature path).
            - `mature` (bool): the path is in the analysed population (always True
              without `horizon`).
            - one column per metric, named by its `name`: the per-path value
              (float); for a ratio metric two columns, `<name>_numerator` and
              `<name>_denominator`. NaN on immature paths and where a value metric
              is undefined.

        Raises
        ------
        DiffConfigError, InvalidMetricConfigError, InvalidParameterError, EmptyEventstreamError
            As in `get_ab_test`.

        Examples
        --------
            paths = stream.get_ab_paths(
                ("exp_group", "treatment", "control"),
                metrics=[{"name": "conversion", "metric": "has_event",
                          "metric_args": {"event": "order_success"}}],
                assignment=assignment,
                horizon="28D",
            )
            paths[paths["mature"]].groupby("arm")["conversion"].mean()

            # CUPED with a pre-period covariate x (a Series indexed by user_id)
            mature = paths[paths["mature"]].join(x)
            theta = mature[["conversion", "x"]].cov().iloc[0, 1] / mature["x"].var()
            mature["adjusted"] = mature["conversion"] - theta * (
                mature["x"] - mature["x"].mean())
            mature.groupby("arm")["adjusted"].mean()
        """
        from retentioneering.tools.ab_test import ab_paths

        return ab_paths(
            self,
            diff,
            metrics,
            assignment=assignment,
            horizon=horizon,
            path_col=path_col,
        )

    @_tracked("get_ab_transitions")
    def get_ab_transitions(
        self,
        diff: T_Diff,
        edge_weight: str = "unique_paths",
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        edges: list | None = None,
        test: str = "auto",
        correction: str = "bh",
        ci_level: float = 0.95,
        power: float = 0.8,
        n_bootstrap: int = 1000,
        bootstrap_weights: str = "multinomial",
        min_support: int = 10,
        random_state: int = 42,
    ) -> pd.DataFrame:
        """
        Test every transition-graph edge between two arms: which transitions differ.

        One row per edge `source -> target` (the synthetic `path_start` / `path_end`
        included) with each arm's value of `edge_weight`, the delta with its
        interval, a p-value adjusted over the tested edges (Benjamini-Hochberg by
        default), a verdict and a `reading`. Arms, population, unit guard and
        `horizon` are `get_ab_test`'s; an arm's value equals the matrix
        `transition_graph_data(edge_weight, diff=...)` draws for it on the stream's
        paths (for `unique_paths` the graph counts paths, the row gives their
        share). An exploratory screen, read after the scorecard: a discovery is a
        hypothesis for a declared metric of a new test. Pass the frame to
        `transition_graph(diff=..., ab_transitions=...)` to highlight it.

        Parameters
        ----------
        diff : tuple
            As in `get_ab_test`: `("segment_col", group1, group2)` or `(ids1,
            ids2)`, resolved per path; `delta = group1 - group2`.
        edge_weight : {"unique_paths", "avg_per_path", "proba_out", "proba_in", "share_of_total"}, default "unique_paths"
            The per-path quantity compared.

            - `"unique_paths"` — 0/1: the path has the transition; the share of
              paths, pooled z-test with a Newcombe interval.
            - `"avg_per_path"` — the transition's count per path; Welch's t-test.
            - `"proba_out"` — transitions `source -> target` over all transitions
              out of `source`; a ratio of sums, joint path bootstrap.
            - `"proba_in"` — over all transitions into `target`; bootstrap.
            - `"share_of_total"` — over all transitions; bootstrap.

            The last three are conditional on post-treatment behaviour (their
            denominators are behaviour the treatment may change; `conditional=True`).
            `"count"` raises (it grows with the arm's size: use `"avg_per_path"`);
            `"time_median"` and `"time_q95"` are not tested in this release.
        assignment : pandas.Series or pandas.DataFrame, optional
            As in `get_ab_test`: the intention-to-treat population. Silent assigned
            paths count with no transition, so per-path weights have the table's
            denominators, not the graph's.
        horizon : str or pandas.Timedelta, optional
            As in `get_ab_test`: events in `[assigned_at, assigned_at + horizon)`
            only, paths with an open window left out (`immature_paths`).
        path_col : str, optional
            The randomised unit; defaults to `schema.path_col`.
        edges : list of tuple, optional
            `[(source, target), ...]`: test only these edges, declared before the
            test (a confirmatory family, which the readings name); rows for them
            only. Events must exist in the stream (or be `path_start` /
            `path_end`).
        test : {"auto", "z_test", "welch_t_test", "bootstrap"}, default "auto"
            `"auto"` is the test of the weight above; `"z_test"` fits only
            `"unique_paths"`, ratios take only `"bootstrap"`.
        correction : {"bh", "holm", "bonferroni", "none"}, default "bh"
            Over the tested edges, one family.

            - `"bh"` — Benjamini-Hochberg (false discovery rate), for a screen.
            - `"holm"` — Holm step-down (family-wise error).
            - `"bonferroni"` — Bonferroni; adds simultaneous intervals
              (`delta_ci_low_adj`, `delta_ci_high_adj`) at `1 - alpha / n_tested`:
              Newcombe or Welch for the per-path weights, `delta ± z · delta_se`
              for the bootstrapped ratios (a percentile that far in the tail is
              beyond what `n_bootstrap` replicates resolve), so they exclude 0
              exactly when `p_value_adjusted < alpha`.
            - `"none"` — no correction.
        ci_level : float, default 0.95
            Level of the per-edge intervals (not simultaneous); `1 - ci_level` is
            the verdict's alpha.
        power : float, default 0.8
            Power used for `mde`.
        n_bootstrap : int, default 1000
            Replicates of the bootstrap, at least 100. Only for the bootstrapped
            weights (`proba_out`, `proba_in`, `share_of_total`, or
            `test="bootstrap"`); a value other than the default with another
            test warns that it is ignored, as do `bootstrap_weights` and
            `random_state`.
        bootstrap_weights : {"multinomial", "poisson"}, default "multinomial"
            Resampling scheme of the bootstrap (bootstrapped weights only).

            - `"multinomial"` — resample each arm's paths with replacement.
            - `"poisson"` — a Poisson(1) weight per path.
        min_support : int, default 10
            Pooled paths with the transition below which an edge is
            `"insufficient_data"`: reported, not tested, not in the family (for
            `unique_paths` the pooled paths without it too).
        random_state : int, default 42
            Seed of the bootstrap (bootstrapped weights only).

        Returns
        -------
        pandas.DataFrame
            One row per edge seen in a compared path (per declared edge with
            `edges`), sorted by `p_value_adjusted` (untested last), then `|delta|`;
            * marks optional columns.

            - `source`, `target`, `edge_weight`; `group1_edge_paths`,
              `group2_edge_paths` (paths with the transition).
            - `population` (`"assignment"` or `"stream"`), `group1_paths`,
              `group2_paths`, `group1_support`, `group2_support` (paths with a
              value: for a probability, paths with a transition out of `source`
              or into `target`), `conditional`, `group1_silent_paths`*,
              `group2_silent_paths`*, `unassigned_paths`, `immature_paths`*.
            - `group1_value`, `group2_value`, `delta`, `delta_se`, `delta_ci_low`,
              `delta_ci_high` (per edge), `delta_ci_low_adj`*, `delta_ci_high_adj`*
              (Bonferroni), `rel_delta`.
            - `p_value`, `p_value_adjusted`, `correction_family`, `mde_alpha`,
              `mde`, `rel_mde`, `verdict` (as in `get_ab_test`, except that a
              row without a p-value — a value constant in both arms, or more
              than 1 % of bootstrap replicates without a value — is not tested
              and reads `"insufficient_data"`, so `increase`, `decrease` and
              `inconclusive` add up to `n_tested`), `test`, `ci_basis`,
              `p_basis`, `valid_replicates`, `support_warning`.
            - `reading`: the row in one sentence, with the family size.

            `attrs` holds `n_tested` (the family size), `correction`, `ci_level`,
            `edge_weight`, the arms and `diff`. An absent estimate is NaN, never 0.

        Raises
        ------
        DiffConfigError, InvalidParameterError, EmptyEventstreamError
            As in `get_ab_test`; an `edge_weight`, `test` or `edges` it does not
            take raises `InvalidParameterError`.

        Examples
        --------
            edges = stream.get_ab_transitions(("exp_group", "treatment", "control"))
            edges[edges["verdict"] != "inconclusive"][
                ["source", "target", "delta", "delta_ci_low", "delta_ci_high",
                 "p_value_adjusted", "verdict"]]

            # ITT, each user's first 28 days, transition probabilities
            stream.get_ab_transitions(("arm", "treatment", "control"), "proba_out",
                                      assignment=assignment, horizon="28D")

            # highlight the significant edges in the diff graph
            stream.transition_graph(diff=("exp_group", "treatment", "control"),
                                    ab_transitions=edges)
        """
        from retentioneering.tools.ab_transitions import ab_transitions

        return ab_transitions(
            self,
            diff,
            edge_weight,
            assignment=assignment,
            horizon=horizon,
            path_col=path_col,
            edges=edges,
            test=test,
            correction=correction,
            ci_level=ci_level,
            power=power,
            n_bootstrap=n_bootstrap,
            bootstrap_weights=bootstrap_weights,
            min_support=min_support,
            random_state=random_state,
        )

    @_tracked("get_ab_steps")
    def get_ab_steps(
        self,
        diff: T_Diff,
        max_steps: int = 10,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        correction: str = "bh",
        ci_level: float = 0.95,
        power: float = 0.8,
        min_support: int = 10,
        path_pattern: str | None = None,
        anchor: str | dict | None = None,
    ) -> pd.DataFrame:
        """
        Test every step-matrix cell between two arms: which events at which step differ.

        One row per cell `(event, step)`: the share of each arm's paths whose
        `step`-th event is `event` (`path_end`: the path has fewer events, as the
        step matrix's last row), the delta with its Newcombe interval, a pooled
        z-test adjusted over the tested cells (Benjamini-Hochberg by default), a
        verdict and a `reading`. Arms, population, unit guard and `horizon` are
        `get_ab_test`'s; an arm's value equals `step_matrix_data(max_steps,
        diff=...)` for it on the stream's paths. An exploratory screen, read after
        the scorecard: a discovery is a hypothesis for a declared metric of a new
        test. Steps count from each path's start; to compare around an event, cut
        the paths with `truncate_paths` first (`path_pattern` and `anchor` raise).

        Parameters
        ----------
        diff : tuple
            As in `get_ab_test`; `delta = group1 - group2`.
        max_steps : int, default 10
            Steps compared, from 1 (the first event) to `max_steps`.
        assignment : pandas.Series or pandas.DataFrame, optional
            As in `get_ab_test`. A silent assigned path is in the denominators and
            has ended at step 1 (`path_end`).
        horizon : str or pandas.Timedelta, optional
            As in `get_ab_test`.
        path_col : str, optional
            The randomised unit; defaults to `schema.path_col`.
        correction : {"bh", "holm", "bonferroni", "none"}, default "bh"
            Over the tested cells, one family.

            - `"bh"` — Benjamini-Hochberg (false discovery rate), for a screen.
            - `"holm"` — Holm step-down (family-wise error).
            - `"bonferroni"` — Bonferroni; adds simultaneous intervals.
            - `"none"` — no correction.
        ci_level : float, default 0.95
            Level of the per-cell intervals (not simultaneous); `1 - ci_level` is
            the verdict's alpha.
        power : float, default 0.8
            Power used for `mde`.
        min_support : int, default 10
            Pooled paths with the event at the step (and pooled paths without it)
            below which a cell is `"insufficient_data"`: reported, not tested.
        path_pattern : None
            Not in this release: any value raises `InvalidParameterError`. Steps
            count from each path's start; a pattern (as in `step_matrix_data`)
            would keep and centre the paths where it matches, a selection by
            behaviour the treatment may change. To compare the steps from an
            event, cut the paths first with `truncate_paths(start_anchor=...,
            end_anchor="path_end")` on an event before exposure.
        anchor : None
            Not in this release, as `path_pattern`.

        Returns
        -------
        pandas.DataFrame
            One row per cell seen in a compared path, sorted by
            `p_value_adjusted` (untested last), then `|delta|`. Columns `event`,
            `step` and the statistic columns of `get_ab_transitions` (without its
            edge columns; `conditional` is False). `attrs` holds `n_tested`,
            `correction`, `ci_level`, `max_steps`, the arms and `diff`.

        Raises
        ------
        DiffConfigError, InvalidParameterError, EmptyEventstreamError
            As in `get_ab_test`; `max_steps` below 1, a `path_pattern` or an
            `anchor` raises `InvalidParameterError`.

        Examples
        --------
            cells = stream.get_ab_steps(("exp_group", "treatment", "control"), max_steps=8)
            cells[cells["verdict"].isin(["increase", "decrease"])]
        """
        from retentioneering.tools.ab_transitions import ab_steps

        return ab_steps(
            self,
            diff,
            max_steps,
            assignment=assignment,
            horizon=horizon,
            path_col=path_col,
            correction=correction,
            ci_level=ci_level,
            power=power,
            min_support=min_support,
            path_pattern=path_pattern,
            anchor=anchor,
        )

    @_tracked("get_ab_transitions_table")
    def get_ab_transitions_table(
        self,
        diff: T_Diff,
        edge_weight: str = "unique_paths",
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        edges: list | None = None,
        test: str = "auto",
        correction: str = "bh",
        ci_level: float = 0.95,
        power: float = 0.8,
        n_bootstrap: int = 1000,
        bootstrap_weights: str = "multinomial",
        min_support: int = 10,
        random_state: int = 42,
        values=("delta", "delta_ci_low", "delta_ci_high", "p_value_adjusted"),
        styled: bool = False,
    ):
        # The docstring is assembled below from get_ab_transitions'.
        from retentioneering.tools.ab_tables import ab_table, style_table

        frame = self.get_ab_transitions(
            diff,
            edge_weight,
            assignment=assignment,
            horizon=horizon,
            path_col=path_col,
            edges=edges,
            test=test,
            correction=correction,
            ci_level=ci_level,
            power=power,
            n_bootstrap=n_bootstrap,
            bootstrap_weights=bootstrap_weights,
            min_support=min_support,
            random_state=random_state,
        )
        table = ab_table(frame, values)
        return style_table(table, frame) if styled else table

    @_tracked("get_ab_steps_table")
    def get_ab_steps_table(
        self,
        diff: T_Diff,
        max_steps: int = 10,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        correction: str = "bh",
        ci_level: float = 0.95,
        power: float = 0.8,
        min_support: int = 10,
        path_pattern: str | None = None,
        anchor: str | dict | None = None,
        values=("delta", "delta_ci_low", "delta_ci_high", "p_value_adjusted"),
        styled: bool = False,
    ):
        # The docstring is assembled below from get_ab_steps'.
        from retentioneering.tools.ab_tables import ab_table, style_table

        frame = self.get_ab_steps(
            diff,
            max_steps,
            assignment=assignment,
            horizon=horizon,
            path_col=path_col,
            correction=correction,
            ci_level=ci_level,
            power=power,
            min_support=min_support,
            path_pattern=path_pattern,
            anchor=anchor,
        )
        table = ab_table(frame, values)
        return style_table(table, frame) if styled else table

    _tr_summary, _tr_sections = _numpydoc_split(get_ab_transitions.__doc__ or "")
    _st_summary, _st_sections = _numpydoc_split(get_ab_steps.__doc__ or "")
    _TABLE_STYLED = """styled : bool, default False
    Return a pandas `Styler` of the table instead, drawn as the {widget} widget
    draws a cell: the `delta` column on a red (group 1 higher) or blue (group 1
    lower) background, its intensity proportional to `|delta|` over the largest
    one; a significant {row} (`p_value_adjusted < alpha`) with a bold black border
    around its group of columns and bold text; a tested, not significant one in
    grey text with a thin grey border; an untested one in light grey italics with a
    dashed border; no style where the frame has no row. `Styler.to_excel(path)`
    keeps the colours and borders (Excel export needs `openpyxl`, the `Styler`
    itself `jinja2`, as for any pandas Styler)."""
    get_ab_transitions_table.__doc__ = _numpydoc_join(
        """Return the `get_ab_transitions` screen as a table shaped like the diff transition matrix.

One row per source event and, for every target event, a group of statistic
columns: each cell of the ordinary diff matrix becomes `len(values)` explicit
cells, by default the delta `group1 - group2`, its per-edge interval and the
adjusted p-value. The table is a pivot of `get_ab_transitions(...)` called with
the same arguments; nothing is recomputed, so every number is a cell of that
frame. A target with no row for a source (the transition never occurs in the
compared paths) is NaN; an untested edge keeps its delta and interval, with a
NaN p-value. Made for export: `to_csv` / `to_excel` write the two header rows
(`pd.read_csv(path, header=[0, 1], index_col=0)` reads them back), and
`styled=True` returns the widget's encoding as a pandas `Styler`.""",
        [
            (
                "Parameters",
                _tr_sections.get("Parameters", "")
                + """
values : str or list of str, default ("delta", "delta_ci_low", "delta_ci_high", "p_value_adjusted")
    Columns of the `get_ab_transitions` frame shown for every edge, in this
    order: any of its statistic columns (`group1_value`, `group2_value`,
    `delta_se`, `rel_delta`, `p_value`, `mde`, `verdict`, `support_warning`,
    `reading`, ...). The key columns `source` and `target` are refused, and so is
    a name that is not a column of the frame.
"""
                + _TABLE_STYLED.format(widget="`ab_transitions`", row="edge"),
            ),
            (
                "Returns",
                """pandas.DataFrame or pandas.io.formats.style.Styler
    Index `source`: the events with an edge in the frame, `path_start` first,
    `path_end` last and the others by name. Columns: a MultiIndex `(target,
    stat)`, the targets in the same order and `values` in the given order inside
    each target. The values are the frame's, unchanged (shares for every weight
    but `avg_per_path`, transitions per path for it). `attrs` are the frame's
    (`n_tested`, `correction`, `ci_level`, `alpha`, `population`, the arms,
    `edge_weight`, ...) plus `values`. With `styled=True`, the `Styler` of that
    table (the table is its `.data`).""",
            ),
            (
                "Raises",
                """DiffConfigError, InvalidParameterError, EmptyEventstreamError
    As in `get_ab_transitions`; a `values` entry that is not a statistic column
    of its frame raises `InvalidParameterError`.""",
            ),
            (
                "Examples",
                """    diff = ("exp_group", "treatment", "control")
    table = stream.get_ab_transitions_table(diff, assignment=assignment)
    table.loc["cart", ("cart", "delta")]
    table.to_csv("edges.csv")
    pd.read_csv("edges.csv", header=[0, 1], index_col=0)

    # the widget's colours and outlines, in Excel (needs openpyxl)
    stream.get_ab_transitions_table(diff, styled=True).to_excel("edges.xlsx")""",
            ),
            (
                "See Also",
                """get_ab_transitions : The same rows as a long DataFrame, one row per edge.
ab_transitions : The same frame drawn as an interactive matrix.""",
            ),
        ],
    )
    get_ab_steps_table.__doc__ = _numpydoc_join(
        """Return the `get_ab_steps` screen as a table shaped like the diff step matrix.

One row per event and, for every step, a group of statistic columns: each cell of
the ordinary diff step matrix becomes `len(values)` explicit cells, by default the
delta `group1 - group2`, its per-cell interval and the adjusted p-value. The table
is a pivot of `get_ab_steps(...)` called with the same arguments; nothing is
recomputed, so every number is a cell of that frame. An event with no row at a
step (no compared path has it there) is NaN; an untested cell keeps its delta and
interval, with a NaN p-value. Made for export: `to_csv` / `to_excel` write the two
header rows (`pd.read_csv(path, header=[0, 1], index_col=0)` reads them back), and
`styled=True` returns the widget's encoding as a pandas `Styler`.""",
        [
            (
                "Parameters",
                _st_sections.get("Parameters", "")
                + """
values : str or list of str, default ("delta", "delta_ci_low", "delta_ci_high", "p_value_adjusted")
    Columns of the `get_ab_steps` frame shown for every cell, in this order: any
    of its statistic columns (`group1_value`, `group2_value`, `delta_se`,
    `rel_delta`, `p_value`, `mde`, `verdict`, `support_warning`, `reading`, ...).
    The key columns `event` and `step` are refused, and so is a name that is not
    a column of the frame.
"""
                + _TABLE_STYLED.format(widget="`ab_steps`", row="cell"),
            ),
            (
                "Returns",
                """pandas.DataFrame or pandas.io.formats.style.Styler
    Index `event`: the events with a cell in the frame, `path_end` last and the
    others by name. Columns: a MultiIndex `(step, stat)`, steps from 1 and
    `values` in the given order inside each step. The values are the frame's,
    unchanged (shares of the arm's paths). `attrs` are the frame's (`n_tested`,
    `correction`, `ci_level`, `alpha`, `population`, the arms, `max_steps`, ...)
    plus `values`. With `styled=True`, the `Styler` of that table (the table is
    its `.data`).""",
            ),
            (
                "Raises",
                """DiffConfigError, InvalidParameterError, EmptyEventstreamError
    As in `get_ab_steps`; a `values` entry that is not a statistic column of its
    frame raises `InvalidParameterError`.""",
            ),
            (
                "Examples",
                """    diff = ("exp_group", "treatment", "control")
    table = stream.get_ab_steps_table(diff, max_steps=8, assignment=assignment)
    table.loc["cart", (8, "delta")]
    stream.get_ab_steps_table(diff, values=["group1_value", "group2_value", "verdict"])
    stream.get_ab_steps_table(diff, styled=True).to_excel("steps.xlsx")""",
            ),
            (
                "See Also",
                """get_ab_steps : The same rows as a long DataFrame, one row per cell.
ab_steps : The same frame drawn as an interactive matrix.""",
            ),
        ],
    )

    @_tracked("get_ab_transitions_power")
    def get_ab_transitions_power(
        self,
        diff: T_Diff,
        edge_weight: str = "unique_paths",
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        edges: list | None = None,
        test: str = "auto",
        correction: str = "bh",
        ci_level: float = 0.95,
        power: float = 0.8,
        n_bootstrap: int = 1000,
        bootstrap_weights: str = "multinomial",
        min_support: int = 10,
        random_state: int = 42,
        scale=(0.25, 0.5, 1, 2, 4, 8, 16),
        target_delta: float | None = None,
    ) -> pd.DataFrame:
        # The docstring is assembled below from get_ab_transitions'.
        from retentioneering.tools.ab_power import ab_transitions_power

        return ab_transitions_power(
            self,
            diff,
            edge_weight,
            scale=scale,
            target_delta=target_delta,
            assignment=assignment,
            horizon=horizon,
            path_col=path_col,
            edges=edges,
            test=test,
            correction=correction,
            ci_level=ci_level,
            power=power,
            n_bootstrap=n_bootstrap,
            bootstrap_weights=bootstrap_weights,
            min_support=min_support,
            random_state=random_state,
        )

    @_tracked("get_ab_steps_power")
    def get_ab_steps_power(
        self,
        diff: T_Diff,
        max_steps: int = 10,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        correction: str = "bh",
        ci_level: float = 0.95,
        power: float = 0.8,
        min_support: int = 10,
        path_pattern: str | None = None,
        anchor: str | dict | None = None,
        scale=(0.25, 0.5, 1, 2, 4, 8, 16),
        target_delta: float | None = None,
    ) -> pd.DataFrame:
        # The docstring is assembled below from get_ab_steps'.
        from retentioneering.tools.ab_power import ab_steps_power

        return ab_steps_power(
            self,
            diff,
            max_steps,
            scale=scale,
            target_delta=target_delta,
            assignment=assignment,
            horizon=horizon,
            path_col=path_col,
            correction=correction,
            ci_level=ci_level,
            power=power,
            min_support=min_support,
            path_pattern=path_pattern,
            anchor=anchor,
        )

    _POWER_PARAMETERS = """
scale : list of float, default (0.25, 0.5, 1, 2, 4, 8, 16)
    Multiples of the current number of paths to project to, the same in both
    arms; 1 (the data as it is) is always included.
target_delta : float, optional
    An absolute effect in the frame's units (a share: `0.01` is 1 pp; for
    `avg_per_path` transitions per path). Adds the paths per arm a {row} needs for
    its `mde` to reach it, and `attrs["curve"]` the share of testable {rows}
    that reach it."""
    _POWER_RETURNS = """pandas.DataFrame
    One row per {row} of `{getter}` and per `scale`, sorted by {keys} and `scale`;
    * marks the columns added with `target_delta`.

    - {key_list}; `scale`.
    - `group1_paths`, `group2_paths`: the projected paths per arm.
    - `pooled_support`: the projected supporting paths of the support rule (paths
      with the {what}, both arms).
    - `testable`: the {row} passes the pooled support rule at this scale (at
      `scale=1`, exactly the {rows} the getter tests); a {row} that passes it and
      still has no p-value (a value constant in both arms) stays untestable.
    - `family_size`: testable {rows} at this scale, the projected family `m`;
      `mde_alpha`: `alpha / m`, the Bonferroni level the `mde` is computed at
      (as the getter's; conservative for Benjamini-Hochberg, which rejects the
      k-th smallest p-value at `alpha * k / m`, never below `alpha / m`).
    - `mde`, `rel_mde`: the minimum detectable effect at that level and `power`:
      the getter's own computation with the standard error scaled by
      `1 / sqrt(scale)` (the pooled share and the projected arm sizes for a
      z-test row; `delta_se / sqrt(scale)` for a Welch row and for a bootstrapped
      one, whose `delta_se` is the bootstrap standard error). At `scale=1` the
      getter's `mde`.
    - `delta`, `test`: the getter's, unchanged (for reference).
    - `scale_to_test`: the smallest multiple of the paths at which the {row}
      passes the support rule (NaN when it never does).
    - `group1_paths_needed`*, `group2_paths_needed`*, `scale_needed`*: the paths
      per arm (and their multiple of the current ones) at which the {row}'s `mde`
      reaches `target_delta`, keeping the family level of this scale.

    `attrs` are the getter's plus `scale`, `target_delta`, `power`, `assumption`
    and `curve`: one dict per scale with the paths per arm, `cells`, `testable`,
    `family_size`, `mde_alpha`, `mde_median`, `mde_q25`, `mde_q75` (over the
    testable {rows}) and, with `target_delta`, `share_reaching_target`.

    The projection assumes the same path composition: more paths with the same
    mix of behaviours, so the same pooled shares and per-path spread. It sizes the
    data a screen needs; it says nothing about the effects the screen will find."""

    _tp_summary, _tp_sections = _numpydoc_split(get_ab_transitions.__doc__ or "")
    get_ab_transitions_power.__doc__ = _numpydoc_join(
        """Return how much data the `get_ab_transitions` screen needs: its edges projected to other sizes.

A screen with many events is data-hungry: each edge is its own test, the family
is every testable edge, and an edge with few supporting paths is not tested. This
getter runs `get_ab_transitions(...)` with the same arguments and projects every
edge to `scale` times the current paths: whether it would be testable, the family
it would belong to, and the minimum detectable effect (`mde`) there. At
`scale=1` every number is the getter's. Read `attrs["curve"]` for the screen as a
whole (testable edges and median MDE against the users), or draw it with
`ab_transitions(...)` in its "Data needed" view.""",
        [
            (
                "Parameters",
                _tp_sections.get("Parameters", "")
                + _POWER_PARAMETERS.format(row="edge", rows="edges"),
            ),
            (
                "Returns",
                _POWER_RETURNS.format(
                    row="edge",
                    rows="edges",
                    getter="get_ab_transitions",
                    keys="`source`, `target`",
                    key_list="`source`, `target`",
                    what="transition",
                ),
            ),
            (
                "Raises",
                """DiffConfigError, InvalidParameterError, EmptyEventstreamError
    As in `get_ab_transitions`; a `scale` that is not a list of positive numbers
    or a `target_delta` that is not a positive number raises
    `InvalidParameterError`.""",
            ),
            (
                "Examples",
                """    diff = ("exp_group", "treatment", "control")
    power = stream.get_ab_transitions_power(diff, assignment=assignment,
                                            target_delta=0.01)
    pd.DataFrame(power.attrs["curve"])[["scale", "testable", "mde_median"]]
    power[power["scale"] == 1].nlargest(5, "scale_needed")""",
            ),
            (
                "See Also",
                """get_ab_transitions : The screen itself.
get_ab_compression : A smaller vocabulary for the same paths, so that more edges are testable.""",
            ),
        ],
    )
    _sp_summary, _sp_sections = _numpydoc_split(get_ab_steps.__doc__ or "")
    get_ab_steps_power.__doc__ = _numpydoc_join(
        """Return how much data the `get_ab_steps` screen needs: its cells projected to other sizes.

The step matrix has a cell per event and step, and the later steps split the
paths over more events, so many cells are thin. This getter runs
`get_ab_steps(...)` with the same arguments and projects every cell to `scale`
times the current paths: whether it would be testable, the family it would belong
to, and its minimum detectable effect (`mde`) there. At `scale=1` every number is
the getter's. Read `attrs["curve"]` for the screen as a whole, or draw it with
`ab_steps(...)` in its "Data needed" view.""",
        [
            (
                "Parameters",
                _sp_sections.get("Parameters", "")
                + _POWER_PARAMETERS.format(row="cell", rows="cells"),
            ),
            (
                "Returns",
                _POWER_RETURNS.format(
                    row="cell",
                    rows="cells",
                    getter="get_ab_steps",
                    keys="`event`, `step`",
                    key_list="`event`, `step`",
                    what="event at the step",
                ),
            ),
            (
                "Raises",
                """DiffConfigError, InvalidParameterError, EmptyEventstreamError
    As in `get_ab_steps`; a `scale` that is not a list of positive numbers or a
    `target_delta` that is not a positive number raises `InvalidParameterError`.""",
            ),
            (
                "Examples",
                """    diff = ("exp_group", "treatment", "control")
    power = stream.get_ab_steps_power(diff, max_steps=8, assignment=assignment)
    pd.DataFrame(power.attrs["curve"])[["scale", "cells", "testable", "mde_median"]]""",
            ),
            (
                "See Also",
                """get_ab_steps : The screen itself.
get_ab_compression : A smaller vocabulary for the same paths, so that more cells are testable.""",
            ),
        ],
    )

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
              per segment level across all segment columns. `count` is the number
              of events carrying the value and `share` its fraction of all events,
              per column. Events with no value are reported under the `<MISSING>`
              level `get_segment_levels` uses.

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
            A missing value (`None`/`NaN`) in the list selects the rows where
            that column has no value, so `keep={"plan": [np.nan]}` keeps exactly
            the rows `drop={"plan": [np.nan]}` removes.
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
            `"in_segment"`, `"in_segment_bulk"`, `"column_sum"`, `"column_mean"`,
            `"column_last"`. See the [Path Metrics](/docs/path-metrics)
            documentation page for the full metric reference.
        method : str, default "kmeans"
            Clustering algorithm. One of `"kmeans"` or `"hdbscan"`.
        method_args : dict, optional
            Parameters of the chosen `method`: `n_clusters` for `"kmeans"`
            (required), `min_cluster_size` and `cluster_selection_epsilon` for
            `"hdbscan"` (both optional). This is the `metric` / `metric_args`
            shape applied to algorithms, so a key that does not belong to
            `method` raises rather than being ignored. `scaler` and
            `nmf_components` are not method arguments — they are pipeline steps
            applied before clustering, so they stay outside this dict.

            - `"kmeans"` — `n_clusters` (int, required); optionally `n_init`
              (`"auto"` or a positive int, default `"auto"`: how many k-means++
              initialisations to run, keeping the best) and `random_state`
              (int or None, default 42). More starts can reduce sensitivity to
              the seed; a fixed seed makes a run repeatable.
            - `"hdbscan"` — `min_cluster_size` (int, default 5),
              `cluster_selection_epsilon` (float, default 0.0).
        scaler : str or None, default "minmax"
            Feature scaler applied before clustering. One of `"minmax"`, `"std"`, or `None`.
            (`"standard"` is accepted as a legacy alias of `"std"`.)
        nmf_components : int, optional
            When set, reduces features to this many NMF components before clustering.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

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
            add `"aggregate_children"` to collapse everything below it,
            `"exclude"` to drop those rows, and `"name"` to set the slug used for
            the collapsed pages.
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
                with an `events` list.
            A leaf comparing a 0/1 metric (`has_event`, `has_any_event`,
            `has_all_events`, `matches_pattern`, `in_segment`) with `True` may be
            written as `{metric: metric_args}`, or as `{metric: value}` for a metric
            with one required argument: `{"has_event": "purchase"}`,
            `{"matches_pattern": "A->.*->B"}`, `{"has_any_event": ["A", "B"]}`.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

        Examples
        --------
            # Keep paths that contain at least one purchase
            stream.filter_paths({"op": ">", "metric": "event_count", "value": 0, "metric_args": {"event": "purchase"}})

            # Keep paths that contain a promo_view or a discount_applied event
            stream.filter_paths({"op": "=", "metric": "has_any_event", "value": True,
                                  "metric_args": {"events": ["promo_view", "discount_applied"]}})

            # The same, with the shorthand for a 0/1 metric compared with True
            stream.filter_paths({"has_any_event": ["promo_view", "discount_applied"]})

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

        dp = FilterPaths(condition, path_col)
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
            An optional `name` labels the column, as in `get_ab_test` (a metric
            that expands into several columns, a `*_bulk` one, takes no `name`;
            names must be unique and must not repeat another metric's column
            name, or `InvalidMetricConfigError` is raised).
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
        from retentioneering.exceptions import InvalidMetricConfigError
        from retentioneering.metrics.metric_builder import MetricBuilder, MetricConfig

        names = [
            entry.get("name") if isinstance(entry, dict) else None
            for entry in (metrics or [])
        ]
        if not any(name is not None for name in names):
            return MetricBuilder(self).build_metrics(metrics, path_col)
        configs = [{k: v for k, v in entry.items() if k != "name"} for entry in metrics]
        given = [n for n in names if n is not None]
        bad = [n for n in given if not isinstance(n, str) or not n]
        if bad:
            raise InvalidMetricConfigError(f"'name' must be a non-empty str, got {bad}")
        dup = sorted({n for n in given if given.count(n) > 1})
        if dup:
            raise InvalidMetricConfigError(f"duplicate metric name(s) {dup}")
        frame = MetricBuilder(self).build_metrics(configs, path_col)
        parsed = MetricConfig(configs).parsed_configs
        rename = {}
        for name, entry, info in zip(names, configs, parsed):
            if name is None:
                continue
            columns = info.get("metric_names") or []
            if len(columns) != 1:
                raise InvalidMetricConfigError(
                    f"metric {name!r}: '{entry.get('metric')}' produces "
                    f"{len(columns) or 'one per level'} columns; 'name' labels a "
                    "single column"
                )
            rename[columns[0]] = name
        frame = frame.rename(columns=rename)
        clash = sorted({c for c in frame.columns if list(frame.columns).count(c) > 1})
        if clash:
            raise InvalidMetricConfigError(
                f"duplicate output column(s) {clash}: a 'name' repeats the column "
                "name of another metric; choose another name"
            )
        return frame

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
        path_values=None,
        pre_exposure: bool | None = None,
    ) -> "Eventstream":
        """
        Add a new categorical segment column to the eventstream.

        Exactly one of `rules`, `func`, `sql`, `funnel_events`, `time_range`,
        `metric_bins`, or `path_values` must be provided — unless `name` is already listed in
        `schema.custom_cols`, in which case passing none of them promotes that
        existing column to a segment in place, without recomputing its values.

        Parameters
        ----------
        name : str
            Name of the new segment column.
        rules : list, optional
            CASE-WHEN rules: a list of condition entries plus a final else entry,
            e.g. `[["country", "=", "US", "domestic"], ["international"]]`.
              - Each condition entry is `[column, op, value, label]` — translates to
                `WHEN <column> <op> <value> THEN <label>` in SQL. A string `value`
                is quoted for you, so write `"US"`, not `"'US'"` — the exception is
                `op="in"`, whose value is inserted raw and must therefore be a
                complete SQL tuple: `"('GB', 'DE', 'FR')"`.
              - The last entry is `[else_label]` — the ELSE branch label.
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
            Split paths into bins by a per-path metric, keyed by `metric`
            (required), exactly one of `edges` / `quantiles` for the cut points,
            and optional `segment_levels` naming the bins. Paths the metric has no
            value for (`time_between` is the one that can be undefined — for a path
            missing either of its two events) get the level `"undefined"`, which is
            not one of the bins and so is not counted against `segment_levels`;
            rename it with `rename_segment_levels`.

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
        path_values : pandas.Series or dict, optional
            One value per path, indexed by path id — an assignment table read from
            a file, or labels computed on another stream (clusters found on a
            truncated stream, carried back onto the full one with
            `truncated.get_path_segments("cluster")`). Broadcast to every event of
            the path, so the segment is constant per path. Paths the mapping does
            not hold get no value, which `diff` and `get_path_segments` show as
            `<MISSING>`; ids in the mapping that are not in the eventstream are
            ignored, with a `UserWarning` when fewer than half of them match a path
            id, and `InvalidParameterError` when none does (a mapping keyed by
            another column or grain). A categorical Series keeps its category
            order. Keys that are strings match non-string path ids by their string
            form (a dict that went through JSON). A non-unique index raises
            `InvalidParameterError`.
        path_col : str, optional
            Path ID column override for `funnel_events`, `metric_bins` and
            `path_values` modes; defaults to `schema.path_col`.
        pre_exposure : bool, optional
            Declare whether the segment was fixed before the experiment's
            exposure (a platform at signup, clusters found on paths cut before the
            exposure event). The declaration is recorded in the recipe, so every
            later `get_ab_test(strata_col=name)` (and the widget, and MCP) reads
            it as `strata_pre_exposure_source="declared"` without a warning; it
            also lands in `get_path_segments(name).attrs["pre_exposure"]`. `None`
            (default) leaves it to the recipe: labels from `get_path_segments` on
            a stream truncated at an event carry `attrs["pre_exposure"]=True`
            through `path_values`, labels rebuilt in pandas lose it. The library
            cannot check the claim: it does not know which event exposes a path.

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

            # one value per path, e.g. the experiment arm from an assignment table
            groups = pd.read_csv("ab_groups.csv", index_col="user_id")
            stream.add_segment("exp_group", path_values=groups["exp_group"])

            # labels fixed before exposure, declared for get_ab_test(strata_col=)
            stream.add_segment("entry", path_values=entry_labels, pre_exposure=True)
        """
        from retentioneering.data_processors.add_segment import AddSegment
        from retentioneering.exceptions import InvalidParameterError

        if pre_exposure is not None and not isinstance(pre_exposure, bool):
            raise InvalidParameterError(
                "pre_exposure", pre_exposure, ["None", "True", "False"]
            )

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
            path_values=path_values,
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
            What the merged event is called: a literal string, `{"col": "<column>"}`
            to take another column's value, or a list of cases naming each group by
            what happened inside it. Required for the window modes, which have no
            natural name of their own; `loops` defaults to the repeated event's name
            and `group_col` to the value of the column being grouped on.

            - a **string** — that literal name.
            - `{"col": "<column>"}` — the value of another column, so a run of
              sessions can be named by the session's type.
            - a **list of cases** — `{"condition": ..., "name": ...}` dicts
              evaluated against the group's own events, optionally closed by a
              plain string used as the fallback for groups no case matched.
              A condition is the `filter_paths` condition tree, over the metrics
              `has_event`, `event_count`, `has_all_events`, `has_any_event`,
              `duration`, `length`, `time_between`, `active_days`, including its
              shorthand leaves (`{"has_event": "purchase"}`).
        agg : dict, optional
            Aggregation rules for non-event columns when rows are merged, as a
            `{column: agg_func}` dict. `agg_func` is one of `"first"` (default),
            `"last"`, `"min"`, `"max"`, `"mean"`, `"mode"`, `"any"`. See
            [agg](/docs/data-processors/collapse-events#agg) below. Example:
            `{"price": "max"}`.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

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
            Sessions delimited by their own opening and closing events, given as
            `start_event` and `end_event` — both required. Events outside a
            `start_event`..`end_event` window get no session.
              - `start_event` (str or list of str) — event name(s) that open a session.
              - `end_event` (str or list of str) — event name(s) that close it.
        timeout : str or pandas.Timedelta, optional
            Inactivity gap after which a new session starts, as a pandas-style
            duration string with an explicit unit — e.g. `"30m"`, `"1h"`,
            `"1800s"` — or a `pandas.Timedelta`. Bare numbers are rejected to
            avoid unit ambiguity.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.

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
        ).apply(self._df, self.schema)
        return Eventstream(new_df, asdict(new_schema), preprocess=False)

    @_tracked("dp_truncate_paths")
    @_op
    def truncate_paths(self, start_anchor, end_anchor, path_col=None) -> "Eventstream":
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
          time and then snaps to the nearest event *inside* the window. Negative
          values (`-2`, `"-30m"`) shift backwards, widening the window past the
          anchor. An offset that runs past the path's own boundary clamps to it.
        - `offset_side` — which way a time `offset` rounds to a real event,
          `"start"` (forward) or `"end"` (backward). Defaults to the side of the
          window being resolved, which rounds inward; set it to widen instead.
        - `event_col` — the column the pattern is matched against; defaults to
          `schema.event_col`. Naming a coarser column (`"screen"`, `"category"`)
          cuts the window at that grain while the events inside it stay atomic —
          the column is only read, never written to. Note that such a column
          usually holds *runs* of one value, so an anchor lands on the run's
          first row.

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

            # the visit to the cart screen, atomic events inside it kept as they are
            stream.truncate_paths(
                start_anchor={"pattern": "cart", "event_col": "screen"},
                end_anchor={"pattern": "checkout", "event_col": "screen"},
            )
        """
        from retentioneering.data_processors.truncate_paths import TruncatePaths

        new_df, new_schema = TruncatePaths(
            start_anchor=start_anchor,
            end_anchor=end_anchor,
            path_col=path_col,
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
        """Event-level split used by the existing widgets' `diff`.

        Validation and level resolution are delegated to
        `tools._diff.resolve_diff(require_constant=False)`, so every `diff` reports
        the same errors. The split itself stays event-level for the segment form
        (a path whose segment changes contributes its events to both groups);
        inferential methods call `resolve_diff` directly and refuse such paths.
        """
        from retentioneering.exceptions import DiffConfigError
        from retentioneering.tools._diff import coerce_level, resolve_diff

        if split is None or isinstance(split, (str, bytes)):
            raise DiffConfigError("diff must be (seg, v1, v2) or (ids1, ids2)")
        if len(split) == 3:
            resolve_diff(self, split, require_constant=False)
            segment_col, v1, v2 = split[0], split[1], split[2]
            all_vals = set(self.get_segment_levels().get(segment_col, []))
            v1 = coerce_level(v1, all_vals)
            if v2 == "<REST>":
                v2_vals = list(all_vals - {v1})
            else:
                v2_vals = [coerce_level(v2, all_vals)]
            s1 = self._filter_by_segment_levels(segment_col, [v1])
            s2 = self._filter_by_segment_levels(segment_col, v2_vals)
        else:
            path_col = path_col or self.schema.path_col
            groups = resolve_diff(self, split, path_col, require_constant=False)
            s1 = self.filter_events(keep={path_col: list(groups.ids1)})
            s2 = self.filter_events(keep={path_col: list(groups.ids2)})
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
            Number of step columns shown around each anchor. Only slices what
            `max_steps` computed, so a window wider than that — set here or in
            the sidebar — raises `max_steps` to `step_window + 10` and
            recomputes.
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
            Number of step columns shown around each anchor. Only slices what
            `max_steps` computed, so a window wider than that — set here or in
            the sidebar — raises `max_steps` to `step_window + 10` and
            recomputes.
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
        label_scale=None,
        views=None,
        view=None,
        ab_transitions=None,
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
        label_scale : float, default 1.0
            Multiplier on node and edge label font sizes, from 0.5 to 3.
            Raise it for slides and screenshots.
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
        ab_transitions : pandas.DataFrame, optional
            A `get_ab_transitions` frame (rows may be filtered) to highlight in
            diff mode: significant edges in the colour of their sign and never
            hidden by the edge filter, inconclusive edges grey, untested edges
            grey and dashed, edges the frame has no row for (a filtered frame,
            or outside its window) faint. Every row of the frame on an edge of
            the graph is drawn, also where the graph's two values tie, and its
            label is the frame's delta (in pp for shares), the number its
            colour reads; under the overlay the edge tooltip shows the test's
            values (for `unique_paths` the arms' shares, not the graph's path
            counts), the delta, its interval, the adjusted p-value and the
            verdict, and the legend names the correction, the family size and
            the population. Needs the frame's `diff` (the same
            groups in the same order; a frame computed on `("arm", ...)` with
            an assignment table matches the arm segment), its `edge_weight`
            (the default when `edge_weight` is not given) and its `path_col`;
            `path_pattern` is refused. The frame is the data: there is no
            `_data` twin. Changing the weight, the diff or the path column in
            the sidebar hides the overlay until they match again.
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
            edges = stream.get_ab_transitions(("exp_group", "treatment", "control"))
            stream.transition_graph(diff=("exp_group", "treatment", "control"),
                                    ab_transitions=edges)
        """
        from retentioneering.widgets.transition_graph import (
            TransitionGraphWidget,
            _UNSET,
        )

        if path_pattern is not None and ab_transitions is not None:
            from retentioneering.exceptions import InvalidParameterError

            raise InvalidParameterError(
                "path_pattern",
                path_pattern,
                ["None with ab_transitions"],
                reason=(
                    "ab_transitions was computed on every path of the arms; a graph "
                    "of the paths matching a pattern draws other numbers, and "
                    "selecting paths by post-exposure behaviour breaks the "
                    "randomisation. Cut the paths before both calls instead"
                ),
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
            label_scale=label_scale if label_scale is not None else _UNSET,
            views=views if views is not None else _UNSET,
            view=view if view is not None else _UNSET,
            ab_transitions=ab_transitions if ab_transitions is not None else _UNSET,
            state_file=state_file,
        )

    @_tracked("widget_funnel")
    def funnel(
        self,
        steps: list[str] | None = None,
        diff=None,
        path_col: str | None = None,
        ci_level: float | None = None,
        correction: str | None = None,
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
        ci_level : float, optional
            Two-sided confidence level in [0.5, 1), e.g. `0.95`: draws an interval
            whisker on every bar and adds the delta's interval and p-value to the
            table. Also switchable in the sidebar (Off / 90% / 95% / 99%). In diff
            mode the groups are then resolved per path and a path in both groups
            raises `DiffConfigError`; see `funnel_data` for every number shown.
        correction : {"none", "holm", "bonferroni", "bh"}, default "none"
            Multiple-comparison correction over the overall-step p-values; see
            `funnel_data`. A value other than `"none"` needs `ci_level`
            (`InvalidParameterError` otherwise).
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

            # A/B test: per-arm intervals, the delta's interval and Holm-adjusted p
            stream.funnel(
                steps=["catalog", "add_to_cart", "purchase"],
                diff=("exp_group", "treatment", "control"),
                ci_level=0.95,
                correction="holm",
            )
        """
        from retentioneering.widgets.funnel import FunnelWidget, _UNSET

        if correction not in (None, "none") and ci_level is None:
            from retentioneering.exceptions import InvalidParameterError

            raise InvalidParameterError(
                "correction", correction, ["'none' without ci_level", "a ci_level="]
            )
        return FunnelWidget(
            eventstream=self,
            steps=steps if steps is not None else _UNSET,
            diff=diff if diff is not None else _UNSET,
            path_col=path_col if path_col is not None else _UNSET,
            ci_level=ci_level if ci_level is not None else _UNSET,
            correction=correction if correction is not None else _UNSET,
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
        ci_level: float | None = None,
        correction: str = "none",
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
        ci_level : float, optional
            Two-sided confidence level in [0.5, 1), e.g. `0.95`. Adds a Wilson score
            interval to every rate and, with `diff`, a Newcombe hybrid score
            interval to every delta and a pooled two-proportion z-test p-value to
            every **overall** delta (see Returns). In diff mode the groups are then
            resolved per path: a path that falls in both groups (a segment that
            changes within the path, overlapping id lists) raises
            `DiffConfigError` — compare at a finer `path_col` or carry a per-path
            label with `add_segment(path_values=...)`. A segment `diff` constant
            within (at least 99 % of) the paths of a coarser path column, with at
            least 10 of them holding several finer paths, raises
            `InvalidParameterError` at the finer `path_col`: per-session intervals
            of a per-user experiment are too narrow (the guard of `get_ab_test`).
            Without `ci_level` the diff keeps the widgets' event-level split and
            no guard applies.
        correction : {"none", "holm", "bonferroni", "bh"}, default "none"
            Multiple-comparison correction applied across the funnel's steps to the
            overall-delta p-values (`delta_conversion_rate_p_value_adjusted`); used
            only with `ci_level` and `diff`, and a value other than `"none"` without
            `ci_level` raises `InvalidParameterError`. `"none"` copies the raw p-values;
            `"holm"` (Holm step-down) and `"bonferroni"` control the family-wise
            error rate; `"bh"` (Benjamini–Hochberg) controls the false discovery
            rate. Steps whose p-value is undefined (`NaN`) are left out of the
            family.

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
          there is no previous step to divide by; 0.0 when the previous step
          has no paths.

        When `diff` is given, each of the four keys above is split into
        `funnel1_*` / `funnel2_*` (one per segment) and `delta_*`
        (`funnel1_* - funnel2_*`) instead.

        Top-level keys besides `"steps"` (when `steps` is not empty):

        - `total_paths` — paths in the eventstream, the denominator of
          `conversion_rate`; with `diff`, `funnel1_total_paths` and
          `funnel2_total_paths` instead (the paths of each group).

        With `ci_level`, per step (no `diff`):

        - `conversion_rate_ci_low`, `conversion_rate_ci_high` — Wilson interval
          of `conversion_rate`.
        - `step_conversion_rate_ci_low`, `step_conversion_rate_ci_high` — Wilson
          interval of `step_conversion_rate`; `NaN` when the previous step has
          no paths (the rate itself stays 0.0 for compatibility).

        With `ci_level` and `diff`, per step:

        - `funnel1_conversion_rate_ci_low`, `funnel1_conversion_rate_ci_high`,
          `funnel1_step_conversion_rate_ci_low`,
          `funnel1_step_conversion_rate_ci_high` and the same `funnel2_*` keys —
          Wilson intervals of each group's rates.
        - `delta_conversion_rate_ci_low`, `delta_conversion_rate_ci_high` —
          Newcombe interval of `delta_conversion_rate`.
        - `delta_conversion_rate_p_value` — pooled two-proportion z-test of equal
          overall rates; `NaN` when undefined (both rates 0 or 1, e.g. a first
          step that every path reaches).
        - `delta_conversion_rate_p_value_adjusted` — the same p-value after
          `correction` across the steps; `NaN` where the raw p-value is `NaN`.
        - `delta_step_conversion_rate_ci_low`,
          `delta_step_conversion_rate_ci_high` — Newcombe interval of
          `delta_step_conversion_rate`. **No p-value**: see `step_conditional`.
        - `step_conditional` — `False` for the first step, `True` after it. A
          step-to-step rate conditions on reaching the previous step, and the
          treatment itself changes who reaches it, so a step delta compares
          different populations: it is descriptive, never a treatment effect,
          and carries an interval but no p-value.

        With `ci_level`, top level:

        - `ci_level` — the level used.
        - `ci_basis` — how each number is computed: `{"rate": "wilson"}`
          without `diff`; `{"rate": "wilson", "delta": "newcombe", "p_value":
          "pooled_z"}` with it.
        - with `diff` only: `correction`; `correction_family` — the p-values
          corrected together, spelled as in `get_ab_test`:
          `"metric=delta_conversion_rate/steps"` (the overall-step p-values with
          a finite value), `"none"` without correction.

        `NaN` marks an absent estimate (never 0); widgets send it as `null`.

        Notes
        -----
        Population: the unconditional denominator is the paths **with at least
        one event** in the eventstream (per group with `diff`), what
        `get_ab_test` calls `population="stream"`: users assigned to an arm who
        never produced an event are not counted, so a funnel's first-step rate
        can differ from the scorecard's (`get_ab_test(assignment=...)` counts
        every assigned user, silent ones as 0, `population="assignment"`). Use
        `get_ab_test` with an assignment table for the effect on the arms; the
        funnel shows where the paths go. The intervals and p-values
        treat every path as an independent unit, so they assume `path_col` is
        the unit that was assigned to a group: session paths of a user-level
        experiment give intervals that are too narrow. p-values are per
        comparison unless `correction` is set; intervals are never adjusted.

        Reading an interval that includes 0: the data are compatible with no
        difference *and* with every difference inside the interval, so the step
        is not shown to differ — it is not shown to be equal either. Read the
        bounds as the range of effects still allowed (`[-1.07, +2.28]` pp: a
        drop larger than about 1 pp or a gain larger than about 2.3 pp is
        unlikely); a narrow interval around 0 rules out effects that matter, a
        wide one says the test is too small to tell. `get_ab_test` gives the
        same step as a scorecard row with `mde` and a `reading` that says how
        many more paths a smaller effect would need.

        Examples
        --------
            stream.funnel_data(["catalog", "add_to_cart", "purchase"])
            stream.funnel_data(
                ["catalog", "add_to_cart", "purchase"],
                diff=("exp_group", "treatment", "control"),
                ci_level=0.95,
                correction="holm",
            )
        """
        from retentioneering.tools.funnel import Funnel

        if not steps:
            return {"steps": []}
        return Funnel(self).fit(
            steps=steps,
            diff=diff,
            path_col=path_col,
            ci_level=ci_level,
            correction=correction,
        )

    @_tracked("get_conversion_rate")
    def get_conversion_rate(
        self,
        start_anchor,
        end_anchor,
        within=None,
        path_col: str | None = None,
        ci_level: float | None = None,
        diff: T_Diff = None,
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
            `"first"`) picks which occurrence of it counts. With `within`,
            `occurrence="all"` opens a window at every occurrence, and the path
            converts if any of them is followed by the end anchor in time;
            without `within` it gives the same result as `"first"`.
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
        ci_level : float, optional
            Two-sided confidence level in [0.5, 1), e.g. `0.95`. Adds Wilson score
            intervals of the conversion rate (`conversion_rate_ci_low` /
            `conversion_rate_ci_high`). `None` (default) means no interval without
            `diff`, and `0.95` with `diff`: diff mode always reports the intervals
            and the p-value, at this level.
        diff : tuple, optional
            Compare two groups of paths: `("segment_col", value1, value2)` (`value2`
            may be `<REST>`, either value `<MISSING>`) or `(path_ids1, path_ids2)`.
            Groups are resolved at path level: a path that falls in both groups (a
            segment that changes within the path, overlapping id lists) raises
            `DiffConfigError` — compare at a finer `path_col` or carry a per-path
            label with `add_segment(path_values=...)`. Replaces the columns below
            with the diff columns. The intervals and the p-value treat every path as
            an independent unit, so they assume `path_col` is the unit that was
            assigned to a group: session paths of a user-level experiment give
            intervals that are too narrow. A segment `diff` constant within (at
            least 99 % of) the paths of a coarser path column, with at least 10 of
            them holding several finer paths, raises `InvalidParameterError` at
            the finer `path_col` (the guard of `get_ab_test`); pass `path_col` of
            the randomised unit. See Notes on conditional rows.

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
            - `conversion_rate_ci_low`, `conversion_rate_ci_high` — with
              `ci_level` only, right after `conversion_rate`: the Wilson interval;
              `NaN` when `paths_with_start` is 0.

            With `diff`, one row per pair with `start_anchor`, `end_anchor` and:

            - `group1_paths_with_start`, `group1_converted`,
              `group1_conversion_rate`, `group1_conversion_rate_ci_low`,
              `group1_conversion_rate_ci_high` (Wilson), `group1_base_rate`,
              `group1_lift` — the columns above computed within group 1's paths
              (the base rate over group 1's paths); the same `group2_*` columns.
            - `delta` — `group1_conversion_rate - group2_conversion_rate`.
            - `delta_ci_low`, `delta_ci_high` — Newcombe hybrid score interval of the
              delta.
            - `p_value` — pooled two-proportion z-test of equal rates; `NaN` when it
              is undefined (a group without start paths, or both rates 0 or 1).
            - `conditional` — True unless the start anchor is `"path_start"` (with
              no offset): the denominator is then the paths that reached the start
              anchor, and the p-value describes that selected population (see
              Notes).

        Notes
        -----
        With `diff`, the denominator is the paths that reached `start_anchor`.
        When the treatment can change who reaches it (a cart, a loop, a feature's
        event), the two groups are selected after exposure, not randomised: the
        same conditional comparison that `funnel_data(ci_level=...)` reports with
        `step_conditional=True` and no p-value. Such rows carry
        `conditional=True`; their delta and p-value describe the selected paths,
        they do not test the treatment. For the treatment's effect, compare the
        share of all assigned paths with `get_ab_test` and a `matches_pattern`
        metric (`{"metric": "matches_pattern", "metric_args": {"pattern":
        "cart->order_success"}}`), or start from `"path_start"`.

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

            # with a 95% interval, and treatment vs control with the delta's
            # interval and p-value, from the path start (an unconditional share)
            stream.get_conversion_rate("add_to_cart", "purchase", ci_level=0.95)
            stream.get_conversion_rate(
                "path_start", "purchase", diff=("exp_group", "treatment", "control")
            )
        """
        from retentioneering.tools.conversion import ConversionRate

        return ConversionRate(self).fit(
            start_anchor=start_anchor,
            end_anchor=end_anchor,
            within=within,
            path_col=path_col,
            ci_level=ci_level,
            diff=diff,
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
        )

    @_tracked("widget_ab_test")
    def ab_test(
        self,
        diff: T_Diff = None,
        metrics: list | None = None,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        strata_col: str | None = None,
        period=None,
        period_basis: str | None = None,
        period_population: str | None = None,
        horizon=None,
        test: str | None = None,
        correction: str | None = None,
        correction_scope: str | None = None,
        ci_level: float | None = None,
        power: float | None = None,
        n_bootstrap: "int | str | None" = None,
        bootstrap_weights: str | None = None,
        min_support: int | None = None,
        random_state: int | None = None,
        path_col: str | None = None,
        strata_pre_exposure: bool | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
        state_file: str | None = None,
    ):
        # The docstring is assembled below from get_ab_test's: one text per parameter.
        from retentioneering.widgets.ab_test import ABTestWidget, _UNSET

        args = {
            "diff": diff,
            "metrics": metrics,
            "strata_col": strata_col,
            "period": period,
            "period_basis": period_basis,
            "period_population": period_population,
            "horizon": horizon,
            "test": test,
            "correction": correction,
            "correction_scope": correction_scope,
            "ci_level": ci_level,
            "power": power,
            "n_bootstrap": n_bootstrap,
            "bootstrap_weights": bootstrap_weights,
            "min_support": min_support,
            "random_state": random_state,
            "path_col": path_col,
            "strata_pre_exposure": strata_pre_exposure,
            "height": height,
            "sidebar_open": sidebar_open,
        }
        return ABTestWidget(
            eventstream=self,
            assignment=assignment,
            state_file=state_file,
            **{k: v if v is not None else _UNSET for k, v in args.items()},
        )

    @_tracked("headless_ab_test")
    def ab_test_data(
        self,
        diff: T_Diff,
        metrics: list,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        strata_col: str | None = None,
        period=None,
        period_basis: str = "calendar",
        period_population: str = "itt",
        horizon=None,
        test: str = "auto",
        correction: str = "holm",
        correction_scope: str = "metric",
        ci_level: float = 0.95,
        power: float = 0.8,
        n_bootstrap: "int | str" = 1000,
        bootstrap_weights: str = "multinomial",
        min_support: int = 10,
        random_state: int = 42,
        path_col: str | None = None,
        strata_pre_exposure: bool | None = None,
    ) -> pd.DataFrame:
        # The docstring is get_ab_test's with an alias note, assembled below.
        return self.get_ab_test(
            diff,
            metrics,
            assignment=assignment,
            strata_col=strata_col,
            period=period,
            period_basis=period_basis,
            period_population=period_population,
            horizon=horizon,
            test=test,
            correction=correction,
            correction_scope=correction_scope,
            ci_level=ci_level,
            power=power,
            n_bootstrap=n_bootstrap,
            bootstrap_weights=bootstrap_weights,
            min_support=min_support,
            random_state=random_state,
            path_col=path_col,
            strata_pre_exposure=strata_pre_exposure,
        )

    _ab_summary, _ab_sections = _numpydoc_split(get_ab_test.__doc__ or "")
    _ab_title, _, _ab_rest = _ab_summary.partition("\n\n")
    ab_test_data.__doc__ = _numpydoc_join(
        f"{_ab_title}\n\n"
        "Documented alias of `get_ab_test`, under the name of the `ab_test`\n"
        "widget's headless twin: the same computation, arguments and result.\n"
        "The docs and the tutorial use `get_ab_test`; the widget calls this\n"
        f"method, so its numbers are the getter's.\n\n{_ab_rest}",
        list(_ab_sections.items()),
    )
    ab_test.__doc__ = _numpydoc_join(
        """Interactive A/B test scorecard for Jupyter notebooks.

The numbers of `get_ab_test`, drawn: a **Scorecard** tab with one row per metric
(each arm's value, the delta with its interval, the adjusted p-value, the MDE, the
verdict and any `support_warning`) and a forest plot of the deltas; a **Strata**
tab when `strata_col` is set (one row per stratum, with the share-balance and
heterogeneity p-values; exploratory rows marked); a **Periods** tab when `period`
is set (the delta and its interval over time; incomplete periods drawn hollow,
descriptive rows marked). An `inconclusive` verdict is not "no effect": its
interval shows which effects the data still allow.

The Scorecard is `ab_test_data(...)` without `strata_col` and `period`; the
Strata tab adds `strata_col` and `strata_pre_exposure`; the Periods tab adds
`period`, `period_basis` and `period_population`. Every other argument applies to
all three. `ab_test_data` is the widget's headless twin, an alias of
`get_ab_test`. The arms, the metrics, `ci_level`, `correction`, `test`,
`strata_col`, `period`, `period_basis` and `horizon` are also editable in the
sidebar; every change recomputes. `diff` and `metrics` may be left out and set
there (or restored from `state_file`).""",
        [
            (
                "Parameters",
                _ab_sections.get("Parameters", "")
                + """
height : int, default 560
    Widget height in pixels.
sidebar_open : bool, default True
    Whether the sidebar starts open.
state_file : str, optional
    JSON file the widget state is bound to; see
    [Saving widget state](/docs/widgets#saving-widget-state). `assignment` is a
    table, not state: pass it again when you reopen the widget.""",
            ),
            (
                "Examples",
                """    diff = ("exp_group", "treatment", "control")
    metrics = [
        {"name": "conversion", "metric": "has_event",
         "metric_args": {"event": "order_success"}},
        {"name": "revenue_per_user", "metric": "column_sum",
         "metric_args": {"column": "order_value"}},
    ]
    stream.ab_test(diff, metrics, assignment=assignment)

    # strata fixed at signup, and weekly ITT rows
    stream.ab_test(diff, metrics, strata_col="platform",
                   strata_pre_exposure=True, period="W")""",
            ),
            (
                "See Also",
                """get_ab_test : The scorecard as a DataFrame (`ab_test_data` is its alias).
get_sample_ratio : The sample ratio check; run it before reading any metric.
get_ab_bootstrap : Joint bootstrap replicates, for contrasts between periods.""",
            ),
        ],
    )
    del _ab_summary, _ab_sections, _ab_title, _ab_rest

    @_tracked("widget_ab_transitions")
    def ab_transitions(
        self,
        diff: T_Diff,
        edge_weight: str | None = None,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        edges: list | None = None,
        test: str | None = None,
        correction: str | None = None,
        ci_level: float | None = None,
        power: float | None = None,
        n_bootstrap: int | None = None,
        bootstrap_weights: str | None = None,
        min_support: int | None = None,
        random_state: int | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
        state_file: str | None = None,
    ):
        # The docstring is assembled below from get_ab_transitions'. None means
        # "not passed": a value saved in state_file is then restored, otherwise the
        # default of get_ab_transitions applies.
        from retentioneering.widgets._base import _UNSET
        from retentioneering.widgets.ab_transitions import ABTransitionsWidget

        args = {
            "edge_weight": edge_weight,
            "horizon": horizon,
            "path_col": path_col,
            "edges": edges,
            "test": test,
            "correction": correction,
            "ci_level": ci_level,
            "power": power,
            "n_bootstrap": n_bootstrap,
            "bootstrap_weights": bootstrap_weights,
            "min_support": min_support,
            "random_state": random_state,
            "height": height,
            "sidebar_open": sidebar_open,
        }
        return ABTransitionsWidget(
            self,
            diff,
            assignment=assignment,
            state_file=state_file,
            **{k: v if v is not None else _UNSET for k, v in args.items()},
        )

    @_tracked("headless_ab_transitions")
    def ab_transitions_data(
        self,
        diff: T_Diff,
        edge_weight: str = "unique_paths",
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        edges: list | None = None,
        test: str = "auto",
        correction: str = "bh",
        ci_level: float = 0.95,
        power: float = 0.8,
        n_bootstrap: int = 1000,
        bootstrap_weights: str = "multinomial",
        min_support: int = 10,
        random_state: int = 42,
    ) -> pd.DataFrame:
        # The docstring is get_ab_transitions' with an alias note, assembled below.
        return self.get_ab_transitions(
            diff,
            edge_weight,
            assignment=assignment,
            horizon=horizon,
            path_col=path_col,
            edges=edges,
            test=test,
            correction=correction,
            ci_level=ci_level,
            power=power,
            n_bootstrap=n_bootstrap,
            bootstrap_weights=bootstrap_weights,
            min_support=min_support,
            random_state=random_state,
        )

    @_tracked("widget_ab_steps")
    def ab_steps(
        self,
        diff: T_Diff,
        max_steps: int | None = None,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        correction: str | None = None,
        ci_level: float | None = None,
        power: float | None = None,
        min_support: int | None = None,
        path_pattern: str | None = None,
        anchor: str | dict | None = None,
        height: int | None = None,
        sidebar_open: bool | None = None,
        state_file: str | None = None,
    ):
        # The docstring is assembled below from get_ab_steps'; None as in
        # ab_transitions.
        from retentioneering.widgets._base import _UNSET
        from retentioneering.widgets.ab_steps import ABStepsWidget

        args = {
            "max_steps": max_steps,
            "horizon": horizon,
            "path_col": path_col,
            "correction": correction,
            "ci_level": ci_level,
            "power": power,
            "min_support": min_support,
            "path_pattern": path_pattern,
            "anchor": anchor,
            "height": height,
            "sidebar_open": sidebar_open,
        }
        return ABStepsWidget(
            self,
            diff,
            assignment=assignment,
            state_file=state_file,
            **{k: v if v is not None else _UNSET for k, v in args.items()},
        )

    @_tracked("headless_ab_steps")
    def ab_steps_data(
        self,
        diff: T_Diff,
        max_steps: int = 10,
        *,
        assignment: "pd.Series | pd.DataFrame | None" = None,
        horizon=None,
        path_col: str | None = None,
        correction: str = "bh",
        ci_level: float = 0.95,
        power: float = 0.8,
        min_support: int = 10,
        path_pattern: str | None = None,
        anchor: str | dict | None = None,
    ) -> pd.DataFrame:
        # The docstring is get_ab_steps' with an alias note, assembled below.
        return self.get_ab_steps(
            diff,
            max_steps,
            assignment=assignment,
            horizon=horizon,
            path_col=path_col,
            correction=correction,
            ci_level=ci_level,
            power=power,
            min_support=min_support,
            path_pattern=path_pattern,
            anchor=anchor,
        )

    _tr_title, _, _tr_rest = _tr_summary.partition("\n\n")
    _st_title, _, _st_rest = _st_summary.partition("\n\n")
    ab_transitions_data.__doc__ = _numpydoc_join(
        f"{_tr_title}\n\n"
        "Documented alias of `get_ab_transitions`, under the name of the\n"
        "`ab_transitions` widget's headless twin: the same computation, arguments\n"
        "and result. The docs use `get_ab_transitions`; the widget calls this\n"
        f"method, so its numbers are the getter's.\n\n{_tr_rest}",
        list(_tr_sections.items()),
    )
    ab_steps_data.__doc__ = _numpydoc_join(
        f"{_st_title}\n\n"
        "Documented alias of `get_ab_steps`, under the name of the `ab_steps`\n"
        "widget's headless twin: the same computation, arguments and result. The\n"
        "docs use `get_ab_steps`; the widget calls this method, so its numbers are\n"
        f"the getter's.\n\n{_st_rest}",
        list(_st_sections.items()),
    )
    _MATRIX_ENCODING = """Each cell is one row of the frame:

- **background**: the delta `group1 - group2` on a diverging scale symmetric
  around 0, red where group 1 is higher and blue where it is lower (the colours
  of the diff graph and the diff step matrix), its intensity proportional to
  `|delta|` up to the scale's maximum (the largest `|delta|` shown, or a smaller
  clip set in the sidebar so that small effects stay visible);
- **text**: the delta in the frame's units ({units}) and, below it in smaller
  type, its interval at `ci_level`;
- **wide outline**: black when the delta is significantly non-zero
  (`p_value_adjusted < alpha`, the verdict `increase` / `decrease`); light grey
  with a light grey cross over the cell when the test cannot say it is non-zero;
  dashed light grey with the cross and muted text when the row was not tested
  (support below `min_support`, no spread, no data; the tooltip says which); a
  blank cell has no row: {empty}.

Hover a cell for both arms' values, the delta and its interval, `rel_delta`,
`p_value`, `p_value_adjusted` with the correction and the family size, `mde`, the
supports, `support_warning`, `conditional`, the verdict and the row's `reading`.
The intervals are per cell, not simultaneous; with `correction="bonferroni"` the
sidebar can show the simultaneous ones instead. It is an exploratory screen:
read it after the scorecard, as a map of where the arms' paths part."""
    _MATRIX_DISPLAY = """
height : int, default 600
    Widget height in pixels.
sidebar_open : bool, default True
    Whether the sidebar starts open.
state_file : str, optional
    JSON file the widget state is bound to; see
    [Saving widget state](/docs/widgets#saving-widget-state). It keeps the data
    parameters and the view (intervals, filter, colour scale, sorting, hidden and
    pinned events); `diff` and the `assignment` table are passed again when you
    reopen the widget."""
    ab_transitions.__doc__ = _numpydoc_join(
        """Interactive A/B difference matrix of transitions for Jupyter notebooks.

The screen of `get_ab_transitions` drawn as a source x target matrix: one cell per
edge, its numbers those of `ab_transitions_data(...)` (the widget's headless twin,
an alias of `get_ab_transitions`). """
        + _MATRIX_ENCODING.format(
            units="percentage points for the shares, transitions per path for "
            "`avg_per_path`",
            empty="the transition never occurs in either arm",
        )
        + """

The sidebar edits `edge_weight`, `correction`, `ci_level` and `min_support`, and
every change recomputes; the other arguments are fixed at construction and shown
read-only there. The view (intervals on or off, only the rows and columns with a
significant cell, the colour-scale clip, sorting by name, largest `|delta|` or
number of significant cells, hidden and pinned events, and for the conditional
weights a hint that rows or columns sum to 100 %) changes in the browser without
a recompute, also in a static HTML export.""",
        [
            (
                "Parameters",
                _tr_sections.get("Parameters", "") + _MATRIX_DISPLAY,
            ),
            (
                "Examples",
                """    diff = ("exp_group", "treatment", "control")
    stream.ab_transitions(diff, assignment=assignment)

    # where transitions out of each event go (conditional on reaching it)
    stream.ab_transitions(diff, "proba_out", n_bootstrap=200)""",
            ),
            (
                "See Also",
                """get_ab_transitions : The screen as a DataFrame (`ab_transitions_data` is its alias).
get_ab_transitions_table : The same rows as a source x target table with explicit statistic columns, for export.
transition_graph : `transition_graph(diff=..., ab_transitions=frame)` draws the frame on the diff graph.""",
            ),
        ],
    )
    ab_steps.__doc__ = _numpydoc_join(
        """Interactive A/B difference step matrix for Jupyter notebooks.

The screen of `get_ab_steps` drawn as an event x step matrix: one cell per
`(event, step)`, its numbers those of `ab_steps_data(...)` (the widget's headless
twin, an alias of `get_ab_steps`). """
        + _MATRIX_ENCODING.format(
            units="percentage points of the arm's paths",
            empty="no compared path has the event at that step",
        )
        + """

The sidebar edits `max_steps`, `correction`, `ci_level` and `min_support`, and
every change recomputes; the other arguments are fixed at construction and shown
read-only there. The view (intervals on or off, only the rows and steps with a
significant cell, the colour-scale clip, sorting the events by name, largest
`|delta|` or number of significant cells, hidden and pinned events) changes in
the browser without a recompute, also in a static HTML export. Steps stay in
their order.""",
        [
            (
                "Parameters",
                _st_sections.get("Parameters", "") + _MATRIX_DISPLAY,
            ),
            (
                "Examples",
                """    diff = ("exp_group", "treatment", "control")
    stream.ab_steps(diff, max_steps=8, assignment=assignment)""",
            ),
            (
                "See Also",
                """get_ab_steps : The screen as a DataFrame (`ab_steps_data` is its alias).
get_ab_steps_table : The same rows as an event x step table with explicit statistic columns, for export.
step_matrix : `step_matrix(diff=...)` draws the arms' differences without tests.""",
            ),
        ],
    )
    del _tr_summary, _tr_sections, _tr_title, _tr_rest
    del _st_summary, _st_sections, _st_title, _st_rest
    del _MATRIX_ENCODING, _MATRIX_DISPLAY, _TABLE_STYLED

    @_tracked("widget_cluster_analysis")
    def cluster_analysis(
        self,
        features: list | None = None,
        method: str | None = None,
        method_args: dict | None = None,
        scaler: str | None = None,
        nmf_components=None,
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
            `cluster_analysis_data`: `n_clusters` for `"kmeans"` (also editable
            in the sidebar, so a value passed here is a starting point rather
            than a lock), `min_cluster_size` and `cluster_selection_epsilon` for
            `"hdbscan"` — which the sidebar has no fields for, making this the
            only way to set them.

            - `"kmeans"` — `n_clusters` (int, list of int, or a range string like
              `"3-8"`; a list or range runs a silhouette-scored grid search).
              Defaults to `"3-8"`. Optionally `n_init` and `random_state`, which
              the sidebar has no fields for either: they apply to every Apply
              and travel into the saved `add_clusters` call. With
              `random_state=None`, each Apply chooses a fresh seed; selecting
              a grid point or restoring `state_file` reuses the current seed.
            - `"hdbscan"` — `min_cluster_size`, `cluster_selection_epsilon`.
        scaler : {"minmax", "std"}, optional
            Feature scaler applied before clustering; default `"minmax"`.
        nmf_components : int, list of int, or str, optional
            Number of components for an optional NMF (non-negative matrix
            factorization) step applied to the scaled features before
            clustering; if omitted, NMF is skipped (the sidebar's "NMF
            Decomposition" toggle starts off). Passing it turns the toggle on
            and pre-fills the field, so it is a starting point you can edit
            rather than a lock. A list triggers a silhouette-scored grid search
            over the given values; a string is parsed like `n_clusters`
            (`"3"`, `"3-8"`, `"3,5,7"`, `"[3,5,7]"`).
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

        Notes
        -----
        Save Clusters and Copy code use the last displayed analysis, including
        its features, path column, and scaler. Sidebar edits take effect after
        Apply; copied code preserves an explicitly disabled scaler (`None`).

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
            nmf_components=nmf_components if nmf_components is not None else _UNSET,
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
            Parameters of the chosen `method`: `n_clusters` for `"kmeans"`
            (defaulting to the range `"3-8"`), `min_cluster_size` and
            `cluster_selection_epsilon` for `"hdbscan"`. Every value may be a
            single value or a list, and any list triggers a silhouette-scored
            grid search over it. A key that does not belong to `method` raises
            rather than being ignored.

            - `"kmeans"` — `n_clusters` (int, list of int, or a range string like
              `"3-8"`). Defaults to `"3-8"`, i.e. a grid search. Optionally
              `n_init` (`"auto"` or a positive int, default `"auto"`) and
              `random_state` (int or None, default 42): settings, not grid
              coordinates — every fit of the search uses them, and the ones you
              pass are carried into `best_params`. `random_state=None` chooses
              one random seed per call and returns that integer in `best_params`
              so the resulting split can be replayed.
            - `"hdbscan"` — `min_cluster_size` (int, default 5),
              `cluster_selection_epsilon` (float, default 0.0).
        scaler : {"minmax", "std"}, optional
            Feature scaler applied before clustering; default `"minmax"`.
        nmf_components : int, list of int, or str, optional
            Number of components for an optional NMF (non-negative matrix
            factorization) step applied to the scaled features before
            clustering; if omitted, NMF is skipped. A list triggers a
            silhouette-scored grid search over the given values. A string is
            parsed like `n_clusters`: `"3"`, a range `"3-8"`, or a list
            `"3,5,7"` / `"[3,5,7]"`.
        overview_metrics : list of dict, optional
            Metrics shown in the overview heatmap after clustering (independent
            of `features`); if omitted, `overview_df` only has segment_size and
            segment_share rows. Both `features` and `overview_metrics` accept
            metric configs from the same [Path Metrics](/docs/path-metrics) registry.
        path_col : str, optional
            Path ID column override; defaults to `schema.path_col`.
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
