"""A/B test scorecard over path metrics.

``ABTest`` turns a two-arm comparison into a flat scorecard: one row per metric (and per
stratum / period when requested) with the per-arm estimate, the delta with a confidence
interval, a p-value, the multiplicity-adjusted p-value, the minimum detectable effect at
the realised sample size and a verdict that is a pure function of the adjusted p-value.

Design (plan v2, lead decisions D3, D5, D6, D12, D13)
------------------------------------------------------
* The population is defined by the ``diff`` resolved at path level, or by an
  ``assignment`` table (intention-to-treat: silent paths count as 0 for 0/1 and count
  metrics and are excluded from value means and ratio denominators).
* All metrics come from one metric-registry pass (``MetricMatrix``); the bootstrap
  replicates are drawn once per call and shared by every bootstrap cell.
* Period rows are intention-to-treat per period by default. ``period_population="active"``
  rows are descriptive (``verdict="descriptive"``, no p-value).
* The multiplicity family is the set of rows of one metric; the ``verdict`` uses
  ``p_value_adjusted`` only; the confidence interval is per comparison and labelled so.
* Statistical dispatch: 0/1 metric -> pooled z-test with Wilson/Newcombe intervals;
  per-path count or value metric -> Welch t; ratio-of-sums metric -> joint path bootstrap
  (percentile CI, p from the bootstrap SE).

Examples
--------
>>> stream.get_ab_test(
...     ("exp_group", "treatment", "control"),
...     metrics=[
...         {"name": "conversion", "metric": "has_event", "metric_args": {"event": "order_success"}},
...         {"name": "revenue_per_user", "metric": "column_sum", "metric_args": {"column": "order_value"}},
...         {"name": "aov", "metric": "column_sum", "metric_args": {"column": "order_value"},
...          "denominator": {"metric": "event_count", "metric_args": {"event": "order_success"}}},
...     ],
...     assignment=assignment["arm"],
... )
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import warnings
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from scipy import stats

from retentioneering.exceptions import (
    EmptyEventstreamError,
    InvalidMetricConfigError,
    InvalidParameterError,
)
from retentioneering.tools import _bootstrap as bs
from retentioneering.tools import _stats
from retentioneering.tools._bootstrap import BootstrapResult
from retentioneering.tools._diff import (
    MISSING,
    MULTI,
    DiffGroups,
    align_assignment,
    normalize_assignment,
    per_path_label,
    resolve_diff,
    warn_user,
)

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream

__all__ = [
    "ABMetricSpec",
    "parse_metrics",
    "MetricMatrix",
    "ABTest",
    "COLUMNS",
    "BOOTSTRAP_COLUMNS",
    "TESTS",
    "CORRECTIONS",
    "VERDICTS",
    "PERIOD_BASES",
    "PERIOD_POPULATIONS",
    "BOOTSTRAP_WEIGHTS",
]

TESTS = ("auto", "z_test", "welch_t_test", "bootstrap")
CORRECTIONS = ("holm", "bonferroni", "bh", "none")
VERDICTS = ("increase", "decrease", "inconclusive", "descriptive", "insufficient_data")
PERIOD_BASES = ("calendar", "since_assignment")
PERIOD_POPULATIONS = ("itt", "active")
BOOTSTRAP_WEIGHTS = bs.WEIGHTS

#: Metrics whose per-path value is 0/1.
BINARY_METRICS = frozenset(
    {"has_event", "has_any_event", "has_all_events", "matches_pattern", "in_segment"}
)
#: Metrics that are non-negative per-path totals: a path with no events counts as 0.
COUNT_METRICS = frozenset({"event_count", "length", "active_days", "column_sum"})
#: Metrics that are undefined for a path with no events (NaN, left out of the mean).
VALUE_METRICS = frozenset({"duration", "time_between", "column_mean", "column_last"})

#: Column order of the scorecard. Optional columns (marked in the docstring of
#: ``Eventstream.get_ab_test``) are present only when the corresponding argument is used.
COLUMNS: tuple[str, ...] = (
    "metric",
    "stratum",
    "period",
    "period_start",
    "period_end",
    "period_complete",
    "group1_paths",
    "group2_paths",
    "group1_paths_no_events",
    "group2_paths_no_events",
    "excluded_paths",
    "immature_paths",
    "group1_value",
    "group1_ci_low",
    "group1_ci_high",
    "group2_value",
    "group2_ci_low",
    "group2_ci_high",
    "delta",
    "delta_se",
    "delta_ci_low",
    "delta_ci_high",
    "delta_ci_low_adj",
    "delta_ci_high_adj",
    "rel_delta",
    "rel_delta_ci_low",
    "rel_delta_ci_high",
    "p_value",
    "p_value_adjusted",
    "correction_family",
    "alpha_used",
    "mde",
    "rel_mde",
    "verdict",
    "test",
    "ci_basis",
    "p_basis",
    "valid_replicates",
    "low_support",
    "support_warning",
    "group1_share",
    "group2_share",
    "balance_p_value",
    "heterogeneity_p_value",
    "heterogeneity_df",
    "heterogeneity_strata_used",
    "stratum_vs_rest_p_value",
    "strata_pre_exposure",
    "strata_pre_exposure_source",
    "exploratory",
)

#: Column order of ``get_ab_bootstrap``.
BOOTSTRAP_COLUMNS: tuple[str, ...] = (
    "replicate",
    "metric",
    "stratum",
    "period",
    "contrast",
    "group1_value",
    "group2_value",
    "delta",
)

_TEST_OF_KIND = {
    "binary": "z_test",
    "count": "welch_t_test",
    "value": "welch_t_test",
    "ratio": "bootstrap",
}
_CI_BASIS = {"z_test": "wilson", "welch_t_test": "welch", "bootstrap": "path_bootstrap"}
_P_BASIS = {"z_test": "pooled_z", "welch_t_test": "welch", "bootstrap": "bootstrap_se"}

#: Ops that compute a segment from the path's own behaviour (post-exposure unless the
#: stream was truncated before the op).
_BEHAVIOUR_SEGMENT_MODES = ("metric_bins", "funnel_events")

_BOOTSTRAP_CHUNK = 128
#: Below this many supporting paths in an arm a bootstrap row gets
#: ``support_warning="low_support"``: percentile intervals under-cover there.
LOW_SUPPORT_PATHS = 30
#: ``support_warning`` values.
SUPPORT_WARNINGS = ("", "one_arm_only", "low_support")
_DAY = pd.Timedelta(days=1)
_HOUR = pd.Timedelta(hours=1)


# ======================================================================= specs
def _metric_column(config: dict[str, Any], role: str, label: str) -> str:
    """Validate one registry config and return its ``get_metrics`` column name."""
    from retentioneering.metrics.metric_builder import VALID_METRICS, MetricConfig

    metric = config.get("metric")
    if not isinstance(metric, str):
        raise InvalidMetricConfigError(
            f"{label}: '{role}' needs a registry metric name, got {metric!r}"
        )
    if metric.endswith("_bulk"):
        raise InvalidMetricConfigError(
            f"{label}: '{metric}' expands into one column per event or level; a "
            "scorecard metric is one number per path. List one metric per event instead."
        )
    if metric not in VALID_METRICS:
        raise InvalidMetricConfigError(
            f"{label}: unknown metric '{metric}'. Available: {sorted(VALID_METRICS)}"
        )
    if metric == "first_event_time":
        raise InvalidMetricConfigError(
            f"{label}: 'first_event_time' is a timestamp, not a number to compare; "
            "use 'time_between' or 'duration'."
        )
    args = config.get("metric_args", {})
    if args is not None and not isinstance(args, dict):
        raise InvalidMetricConfigError(
            f"{label}: 'metric_args' must be a dict, got {type(args).__name__}"
        )
    parsed = MetricConfig([{"metric": metric, "metric_args": args or {}}])
    names = parsed.parsed_configs[0].get("metric_names")
    if not names or len(names) != 1:
        raise InvalidMetricConfigError(
            f"{label}: '{metric}' with these metric_args produces "
            f"{'several columns' if names else 'one column per level'}; a scorecard "
            "metric must be one number per path (name a single level)."
        )
    return names[0]


def _kind_of(metric: str) -> str:
    if metric in BINARY_METRICS:
        return "binary"
    if metric in COUNT_METRICS:
        return "count"
    return "value"


@dataclass(frozen=True)
class ABMetricSpec:
    """One scorecard metric.

    Attributes
    ----------
    name : str
        Row label; defaults to the ``get_metrics`` column name of ``config``.
    config : dict
        Registry metric config (``metric``, ``metric_args``).
    denominator : dict or None
        Registry config whose per-path value divides the numerator (ratio-of-sums metric).
    test : str
        Resolved test: ``"z_test"``, ``"welch_t_test"`` or ``"bootstrap"``.
    kind : str
        ``"binary"`` (0/1 per path), ``"count"`` (a per-path total; 0 for a path with
        no events), ``"value"`` (float per path, NaN when undefined), ``"ratio"`` (has a
        denominator).
    """

    name: str
    config: dict[str, Any]
    denominator: dict[str, Any] | None
    test: str
    kind: str

    _KEYS = frozenset({"metric", "metric_args", "name", "denominator", "test"})

    @classmethod
    def from_user(
        cls, entry: dict[str, Any], default_test: str = "auto"
    ) -> "ABMetricSpec":
        """Validate a user entry.

        Raises
        ------
        InvalidMetricConfigError
            Not a dict; unknown key; unknown metric; a ``*_bulk`` metric or one that
            expands into several columns; ``first_event_time``; ``denominator`` on a 0/1
            metric; unknown ``test``; a test that does not fit the kind (``z_test`` on a
            non-0/1 metric, anything but ``bootstrap`` on a ratio metric).
        """
        if not isinstance(entry, dict):
            raise InvalidMetricConfigError(
                f"each metric must be a dict, got {type(entry).__name__}: {entry!r}"
            )
        label = f"metric {entry.get('name', entry.get('metric'))!r}"
        unknown = set(entry) - cls._KEYS
        if unknown:
            raise InvalidMetricConfigError(
                f"{label}: unknown key(s) {sorted(unknown)}; allowed: {sorted(cls._KEYS)}"
            )
        config = {
            "metric": entry.get("metric"),
            "metric_args": entry.get("metric_args") or {},
        }
        column = _metric_column(config, "metric", label)
        kind = _kind_of(config["metric"])

        denominator = entry.get("denominator")
        if denominator is not None:
            if not isinstance(denominator, dict):
                raise InvalidMetricConfigError(
                    f"{label}: 'denominator' must be a registry config dict"
                )
            if kind == "binary":
                raise InvalidMetricConfigError(
                    f"{label}: a 0/1 metric takes no denominator; it is already a "
                    "share of paths."
                )
            denominator = {
                "metric": denominator.get("metric"),
                "metric_args": denominator.get("metric_args") or {},
            }
            _metric_column(denominator, "denominator", label)
            kind = "ratio"

        test = entry.get("test", default_test)
        if test not in TESTS:
            raise InvalidMetricConfigError(
                f"{label}: unknown test {test!r}; allowed: {list(TESTS)}"
            )
        if test == "auto":
            test = _TEST_OF_KIND[kind]
        if test == "z_test" and kind != "binary":
            raise InvalidMetricConfigError(
                f"{label}: 'z_test' is for 0/1 metrics; this one is a {kind} metric "
                "(use 'welch_t_test' or 'bootstrap')."
            )
        if kind == "ratio" and test != "bootstrap":
            raise InvalidMetricConfigError(
                f"{label}: a ratio of sums has no closed-form test here; use "
                "test='bootstrap' (or 'auto')."
            )
        name = entry.get("name", column)
        if not isinstance(name, str) or not name:
            raise InvalidMetricConfigError(f"{label}: 'name' must be a non-empty str")
        return cls(
            name=name, config=config, denominator=denominator, test=test, kind=kind
        )

    def family_key(self) -> str:
        """Key of the multiplicity family (the metric name)."""
        return self.name


def parse_metrics(metrics: Any, default_test: str = "auto") -> list[ABMetricSpec]:
    """Validate the ``metrics`` argument (non-empty list, unique names)."""
    if not isinstance(metrics, (list, tuple)) or not metrics:
        raise InvalidMetricConfigError(
            "metrics must be a non-empty list of registry metric configs"
        )
    specs = [ABMetricSpec.from_user(entry, default_test) for entry in metrics]
    names = [spec.name for spec in specs]
    dup = sorted({n for n in names if names.count(n) > 1})
    if dup:
        raise InvalidMetricConfigError(
            f"duplicate metric name(s) {dup}: give each entry its own 'name'"
        )
    return specs


# ======================================================================= helpers
def _config_key(config: dict[str, Any]) -> str:
    return json.dumps(config, sort_keys=True, default=str)


def _sorted_ids(index: pd.Index) -> pd.Index:
    index = pd.Index(pd.unique(index))
    try:
        return index.sort_values()
    except TypeError:
        return pd.Index(sorted(index, key=str))


def _coverage(ts: pd.Series) -> tuple[pd.Timestamp, pd.Timestamp]:
    """The stream's observed range: from midnight of the first event's day to the last
    event's timestamp (the end of data: a window that closes after it is not
    observed in full)."""
    return ts.min().floor("D"), ts.max()


_MONTHS = (
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
)
_WEEKDAYS = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
_NUMBER = re.compile(r"\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*")


def _reject_bare_number(value: Any, param: str) -> None:
    """A duration needs a unit: ``14`` or ``"14"`` would otherwise be read as ns."""
    if isinstance(value, (bool, int, float, np.number)) or (
        isinstance(value, str) and _NUMBER.fullmatch(value)
    ):
        raise InvalidParameterError(
            param, value, ["pass a duration such as '14D' (a bare number has no unit)"]
        )


def _to_offset(value: Any):
    """A pandas offset from an offset alias, a period alias (``"M"``, ``"Q"``, ``"Y"``
    on pandas 3) or an offset object; None when ``value`` is neither."""
    from pandas.tseries.frequencies import to_offset

    try:
        with warnings.catch_warnings():
            # pandas 2.2 still parses "M" / "Q" / "Y" as offsets, with a FutureWarning
            warnings.simplefilter("ignore", FutureWarning)
            return to_offset(value)
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        try:
            return pd.PeriodDtype(value).freq
        except (TypeError, ValueError):
            pass
    return None


def _fixed_length(value: Any, param: str) -> pd.Timedelta:
    """A positive fixed-length duration from an offset alias, Timedelta or offset."""
    allowed = ["a fixed-length duration such as '7D', '14D', '12h' or 'W'"]
    _reject_bare_number(value, param)
    if isinstance(value, pd.Timedelta):
        length = value
    else:
        offset = _to_offset(value)
        if offset is None:
            try:
                length = pd.Timedelta(value)
            except (TypeError, ValueError):
                raise InvalidParameterError(param, value, allowed) from None
        elif isinstance(offset, pd.offsets.Week):
            length = pd.Timedelta(days=7 * offset.n)
        else:
            try:
                length = pd.Timedelta(offset.nanos, unit="ns")
            except ValueError:
                raise InvalidParameterError(param, value, allowed) from None
    if length <= pd.Timedelta(0):
        raise InvalidParameterError(param, value, allowed)
    return length


def _calendar_period(period: Any) -> tuple[pd.Timedelta | None, str | None, int]:
    """Resolve a calendar ``period``.

    Returns ``(length, None, 1)`` for a fixed length (``"7D"``, ``"12h"``), or
    ``(None, freq, n)`` for a calendar unit, where ``freq`` is the pandas period
    frequency of one unit (``"M"``, ``"Q-DEC"``, ``"Y-DEC"``, ``"W-SUN"``) and ``n`` the
    number of units per bin. Offset aliases map to period frequencies (``"ME"``,
    ``"MS"`` -> ``"M"``; ``"QE-*"`` -> ``"Q-*"``; ``"QS"`` -> the quarters that start in
    January, ``"Q-DEC"``; ``"YE"``, ``"YS"`` -> ``"Y-*"``).
    """
    allowed = ["a pandas offset alias such as 'D', 'W', 'M', 'Q', 'Y', '7D', '12h'"]
    _reject_bare_number(period, "period")
    offset = _to_offset(period)
    if offset is None or offset.n < 1:
        raise InvalidParameterError("period", period, allowed)
    n = int(offset.n)
    offsets = pd.offsets
    if isinstance(offset, offsets.Week):
        if offset.weekday is None:
            return pd.Timedelta(days=7 * n), None, 1
        return None, f"W-{_WEEKDAYS[offset.weekday]}", n
    if isinstance(offset, (offsets.MonthEnd, offsets.MonthBegin)):
        return None, "M", n
    if isinstance(offset, offsets.QuarterEnd):
        return None, f"Q-{_MONTHS[offset.startingMonth - 1]}", n
    if isinstance(offset, offsets.QuarterBegin):
        return None, f"Q-{_MONTHS[(offset.startingMonth - 2) % 12]}", n
    if isinstance(offset, offsets.YearEnd):
        return None, f"Y-{_MONTHS[offset.month - 1]}", n
    if isinstance(offset, offsets.YearBegin):
        return None, f"Y-{_MONTHS[(offset.month - 2) % 12]}", n
    try:
        length = pd.Timedelta(offset.nanos, unit="ns")
    except ValueError:
        length = None
    if length is not None:
        if length <= pd.Timedelta(0):
            raise InvalidParameterError("period", period, allowed)
        return length, None, 1
    if n != 1:
        raise InvalidParameterError(
            "period",
            period,
            [
                "a multiple of a fixed length, weeks, months, quarters or years "
                "('14D', '2W', '3M'); other calendar units take no multiple"
            ],
        )
    freq = offset.rule_code
    try:
        pd.PeriodDtype(freq)
    except (TypeError, ValueError):
        raise InvalidParameterError("period", period, allowed) from None
    return None, freq, 1


def _align_assigned_at(assigned_at: pd.Series, ts: pd.Series) -> pd.Series:
    """``assigned_at`` in the stream's timezone.

    A naive table on a tz-aware stream is read as wall time in the stream's timezone;
    a tz-aware table is converted to it. A tz-aware table on a naive stream is refused:
    the stream's timestamps carry no timezone to convert to.
    """
    tz = getattr(ts.dt, "tz", None)
    at_tz = assigned_at.dt.tz
    if tz is None and at_tz is None:
        return assigned_at
    if tz is not None:
        if at_tz is None:
            return assigned_at.dt.tz_localize(tz)
        return assigned_at.dt.tz_convert(tz)
    raise InvalidParameterError(
        "assignment",
        f"assigned_at in timezone {at_tz} on a stream with naive timestamps",
        [
            "naive assigned_at in the stream's local time, e.g. "
            "assigned_at.dt.tz_convert('<stream tz>').dt.tz_localize(None)"
        ],
    )


def _check_unit(stream, path_col: str, table: pd.DataFrame | None) -> None:
    """Refuse a path grain finer than the assigned unit (Deng et al. 2018).

    The table's unit is read from its index name when that names a path column (a
    table indexed by ``user_id`` with ``path_col="session_id"`` raises; one indexed
    by ``session_id`` is taken at its word), and otherwise from id overlap: as many
    or more table ids found among a coarser column's ids as among ``path_col``'s
    raises. A tie raises too: integer user and session ids often share one range,
    and the counts cannot tell the unit then (name the index to settle it).
    """
    if table is None:
        return
    cols = list(stream.schema.path_cols)
    position = cols.index(path_col)
    if position == 0:
        return
    ids = table.index
    if ids.name == path_col:
        return
    own = int(ids.isin(pd.unique(stream.df[path_col])).sum())
    for coarser in cols[:position]:
        named = ids.name == coarser
        hits = 0 if named else int(ids.isin(pd.unique(stream.df[coarser])).sum())
        if named or (hits > 0 and hits >= own):
            tie = (
                f" ({hits} table ids are {coarser} ids and {own} are {path_col} ids; "
                f"if the table is per {path_col}, name its index "
                f"'{path_col}')"
                if not named and hits == own
                else ""
            )
            raise InvalidParameterError(
                "path_col",
                path_col,
                [
                    f"'{coarser}' — the assignment table lists {coarser} ids, so "
                    f"{coarser} is the randomised unit; per-{path_col} intervals would "
                    "treat correlated paths of one unit as independent and come out "
                    f"too narrow (Deng et al. 2018){tie}"
                ],
            )


def prepare_assignment(
    stream, path_col: str, table: pd.DataFrame | None
) -> pd.DataFrame | None:
    """Unit guard, id matching (:func:`~retentioneering.tools._diff.align_assignment`)
    and timezone alignment of a normalised assignment table."""
    if table is None:
        return None
    _check_unit(stream, path_col, table)
    aligned = align_assignment(table, stream, path_col)
    if aligned.index is not table.index:
        _check_unit(stream, path_col, aligned)  # the ids now have the stream's type
    if "assigned_at" in aligned.columns:
        aligned["assigned_at"] = _align_assigned_at(
            aligned["assigned_at"], stream.df[stream.schema.timestamp_col]
        )
    return aligned


def _cuts_at_an_event(op: dict) -> bool:
    """Whether a ``truncate_paths`` op ends paths at an event (a no-op truncation to
    ``path_end`` does not)."""
    end = op.get("end_anchor", "path_end")
    anchors = end if isinstance(end, list) else [end]
    for anchor in anchors:
        pattern = anchor.get("pattern") if isinstance(anchor, dict) else anchor
        if pattern != "path_end":
            return True
    return False


def _strata_provenance(stream, strata_col: str) -> bool | None:
    """Whether ``strata_col`` was fixed before exposure, read from the stream's recipe.

    Returns True when the op that produced the column ran after a ``truncate_paths``
    that ends each path at an event (the cut-off the analyst chose as the exposure
    marker), or when the column came through ``add_segment(path_values=)`` from such
    a stream (see ``Eventstream.get_path_segments``); False when it was computed from
    the paths' behaviour (``add_clusters``, ``add_segment(metric_bins=|funnel_events=)``)
    without such a truncation before it; None when the recipe cannot tell (a source
    column, rules, a function, SQL).

    The recipe proves that a cut happened, not that it fell before exposure: the
    library does not know which event exposes a path to the treatment. A truncation
    that ends after exposure still reads as pre-exposure; that is the analyst's
    claim, as is an explicit ``strata_pre_exposure=True``.
    """
    lineage = stream.recipe()
    for position in range(len(lineage) - 1, -1, -1):
        op = lineage[position]
        if op.get("name") != strata_col or op.get("type") not in (
            "add_clusters",
            "add_segment",
        ):
            continue
        truncated = any(
            o.get("type") == "truncate_paths" and _cuts_at_an_event(o)
            for o in lineage[:position]
        )
        if op["type"] == "add_segment" and op.get("path_values") is not None:
            flag = op.get("_path_values_pre_exposure")
            if flag is None:
                flag = getattr(op["path_values"], "attrs", {}).get("pre_exposure")
            return flag if isinstance(flag, bool) else None
        if op["type"] == "add_clusters" or any(
            op.get(mode) is not None for mode in _BEHAVIOUR_SEGMENT_MODES
        ):
            return True if truncated else False
        return None
    return None


#: ``strata_pre_exposure_source`` values.
STRATA_SOURCES = ("declared", "recipe", "not_pre_exposure")


def _strata_flag(
    stream, strata_col: str, declared: bool | None, from_table: bool
) -> tuple[bool, str]:
    """``(strata_pre_exposure, strata_pre_exposure_source)``; warns when the auto mode
    cannot show that the strata were fixed before exposure."""
    if declared is not None:
        return declared, "declared" if declared else "not_pre_exposure"
    recipe = None if from_table else _strata_provenance(stream, strata_col)
    if recipe is True:
        return True, "recipe"
    if recipe is False:
        warn_user(
            f"strata_col '{strata_col}' was computed from the paths' behaviour "
            "on a stream that was not truncated before exposure: the stratum "
            "may be post-exposure, so its rows compare selected subgroups, not "
            "randomised ones (strata_pre_exposure=False)."
        )
    else:
        where = "the assignment table" if from_table else "the stream's recipe"
        warn_user(
            f"{where} does not show that strata_col '{strata_col}' was fixed before "
            "exposure, so its rows are exploratory (strata_pre_exposure=False). If it "
            "was (a platform at signup, a segment recorded at assignment), pass "
            "strata_pre_exposure=True."
        )
    return False, "not_pre_exposure"


# ======================================================================= matrix
@dataclass
class MetricMatrix:
    """Per-path metric values for every metric × stratum × period cell.

    Built from one metric-registry pass. Instead of materialising the
    ``(n_paths, n_cells)`` numerator and denominator matrices up front (hundreds of MB
    for strata × periods at 40k paths), the matrix keeps one ``(n_paths, n_periods)``
    array per metric and one population mask per segment, and assembles the columns a
    caller asks for (:meth:`columns`).

    Attributes
    ----------
    path_ids : pandas.Index
        Sorted path ids of both arms (silent paths included when ``assignment`` is
        given; immature paths of a ``horizon`` excluded).
    arm : ndarray (n_paths,)
        0 = group 1, 1 = group 2.
    specs : list of ABMetricSpec
    values : dict
        ``spec.name -> (num, den)``, each an ``(n_paths, n_periods)`` float array
        (``den`` None unless the spec is a ratio).
    has_events : ndarray (n_paths,)
        Whether the path has an event in the analysed window.
    segments : pandas.DataFrame
        One row per segment: ``stratum``, ``period`` (label or None), ``role``
        (``"row"`` or ``"rest"`` — the complement of a stratum within its period),
        ``period_pos``; ``masks`` holds the matching population masks.
    masks : ndarray (n_paths, n_segments) of bool
    cells : pandas.DataFrame
        One row per (spec, segment): ``metric``, ``spec_pos``, ``segment``,
        ``stratum``, ``period``, ``role``.
    periods : pandas.DataFrame or None
        One row per period: ``period, period_start, period_end, period_complete``.
    immature : ndarray (n_periods,) or None
        Paths excluded from each period because their window is not closed at the
        stream's end (``horizon`` or ``period_basis="since_assignment"``).
    strata : pandas.Series or None
        Stratum label per path.
    strata_pre_exposure : bool or None
        Whether the strata count as fixed before exposure (None without strata).
    strata_pre_exposure_source : str or None
        ``"declared"``, ``"recipe"`` or ``"not_pre_exposure"`` (see :func:`_strata_flag`).
    with_assignment : bool
        Whether an assignment table defined the population.
    excluded_paths : int
        From the diff resolution.
    """

    path_ids: pd.Index
    arm: np.ndarray
    specs: list[ABMetricSpec]
    values: dict[str, tuple[np.ndarray, np.ndarray | None]]
    has_events: np.ndarray
    segments: pd.DataFrame
    masks: np.ndarray
    cells: pd.DataFrame
    periods: pd.DataFrame | None = None
    immature: np.ndarray | None = None
    strata: pd.Series | None = None
    strata_pre_exposure: bool | None = None
    strata_pre_exposure_source: str | None = None
    with_assignment: bool = False
    excluded_paths: int = 0

    # ------------------------------------------------------------------ build
    @classmethod
    def build(
        cls,
        stream: "Eventstream",
        groups: DiffGroups,
        specs: list[ABMetricSpec],
        *,
        strata_col: str | None,
        period: Any | None,
        period_basis: str,
        period_population: str,
        assignment: pd.DataFrame | None,
        path_col: str | None,
        horizon: Any | None = None,
        strata_pre_exposure: bool | None = None,
        strata_labels: pd.Series | None = None,
    ) -> "MetricMatrix":
        """Run the metric registry once over the union of numerators and denominators.

        With ``period`` the stream is first re-keyed to ``(path, period)`` composite
        paths via :func:`retentioneering.metrics.composite.composite_path_stream`; with
        ``period_basis="since_assignment"`` the period of an event is the floor of
        ``(timestamp - assigned_at)`` to the period length. With ``assignment`` (a
        normalised table, see :func:`~retentioneering.tools._diff.normalize_assignment`)
        events before ``assigned_at`` are dropped and silent paths get zero rows. With
        ``horizon`` only events in ``[assigned_at, assigned_at + horizon)`` count and
        paths whose window ends after the stream's last event are left out. With
        ``strata_labels`` (a column of the assignment table) the strata come from the
        table instead of the stream's events.
        """
        from retentioneering.eventstream.eventstream import Eventstream

        schema = stream.schema
        path_col = path_col or schema.path_col
        ts_col = schema.timestamp_col
        df = stream.df
        cov_start, cov_end = _coverage(df[ts_col])

        path_ids = _sorted_ids(groups.ids1.append(groups.ids2))
        arm = np.where(path_ids.isin(groups.ids1), 0, 1)

        assigned_at = None
        if assignment is not None and "assigned_at" in assignment.columns:
            assigned_at = assignment["assigned_at"].reindex(path_ids)

        # ---- the analysed window: drop pre-assignment events, apply the horizon
        since = period is not None and period_basis == "since_assignment"
        if assigned_at is not None and (horizon is not None or since):
            undated = assigned_at.index[assigned_at.isna()]
            if len(undated):
                raise InvalidParameterError(
                    "assignment",
                    f"{len(undated)} compared path(s) without assigned_at, e.g. "
                    f"{list(undated[:3])}",
                    [
                        "an assigned_at for every compared path (horizon and "
                        "since-assignment periods count time from it)"
                    ],
                )
        keep = df[path_col].isin(path_ids).to_numpy()
        horizon_immature = 0
        length = None
        if assigned_at is not None:
            row_assigned = df[path_col].map(assigned_at)
            keep = (
                keep & ((df[ts_col] >= row_assigned) | row_assigned.isna()).to_numpy()
            )
            if horizon is not None:
                length = _fixed_length(horizon, "horizon")
                mature = (assigned_at + length <= cov_end).to_numpy()
                horizon_immature = int((~mature).sum())
                path_ids, arm, assigned_at = (
                    path_ids[mature],
                    arm[mature],
                    assigned_at[mature],
                )
                keep = keep & (df[ts_col] < row_assigned + length).to_numpy()
                keep = keep & df[path_col].isin(path_ids).to_numpy()
        if (arm == 0).sum() == 0 or (arm == 1).sum() == 0:
            raise EmptyEventstreamError(
                "an arm has no path whose horizon window is closed at the stream's end"
            )
        if keep.all():
            work = stream
        else:
            work = Eventstream(df.loc[keep].copy(), asdict(schema), preprocess=False)

        # ---- period keys per row
        periods = None
        immature = None
        period_keys = None
        if period is not None:
            period_keys, periods = _period_keys(
                work.df,
                ts_col,
                path_col,
                period,
                period_basis,
                assigned_at,
                cov_start,
                cov_end,
            )

        configs: dict[str, dict] = {}
        for spec in specs:
            configs.setdefault(_config_key(spec.config), spec.config)
            if spec.denominator is not None:
                configs.setdefault(_config_key(spec.denominator), spec.denominator)
        keys = list(configs)
        raw, active = _metric_values(
            work, list(configs.values()), path_col, path_ids, period_keys, periods
        )
        n_periods = 1 if periods is None else len(periods)
        has_events = active.any(axis=1)

        values: dict[str, tuple[np.ndarray, np.ndarray | None]] = {}
        for spec in specs:
            num = raw[:, :, keys.index(_config_key(spec.config))]
            if spec.kind == "value":
                num = num.copy()
            else:
                num = np.where(np.isnan(num), 0.0, num)
                if not np.issubdtype(num.dtype, np.number):
                    num = num.astype(float)
            den = None
            if spec.denominator is not None:
                den = raw[:, :, keys.index(_config_key(spec.denominator))]
                den = np.where(np.isfinite(den), den, 0.0)
            values[spec.name] = (num, den)

        # ---- period populations
        if periods is not None and since:
            mature = np.stack(
                [
                    (assigned_at + end <= cov_end).to_numpy()
                    for end in periods["period_end"]
                ],
                axis=1,
            )
            immature = (~mature).sum(axis=0)
            periods["period_complete"] = immature == 0
            if length is not None:
                # a bin that the horizon cuts covers less than its label says
                periods["period_complete"] &= periods["period_end"] <= length
        if periods is None:
            pops = np.ones((len(path_ids), 1), dtype=bool)
        elif period_population == "active":
            pops = active
        elif since:
            pops = mature
        else:
            if assigned_at is not None:
                entry = assigned_at
            else:
                entry = (
                    df.groupby(path_col, observed=True, sort=False)[ts_col]
                    .min()
                    .reindex(path_ids)
                )
            pops = np.stack(
                [
                    (entry.isna() | (entry < end)).to_numpy()
                    for end in periods["period_end"]
                ],
                axis=1,
            )
        if horizon is not None:
            base = np.zeros(n_periods, dtype=int) if immature is None else immature
            immature = base + horizon_immature

        # ---- strata
        strata = None
        strata_levels: list = [None]
        pre_exposure = source = None
        if strata_col is not None:
            if strata_labels is not None:
                labels = strata_labels.astype(object)
                labels = labels.where(labels.notna(), MISSING)
            else:
                labels = per_path_label(stream.df, strata_col, path_col)
            strata = labels.reindex(path_ids).fillna(MISSING).astype(object)
            multi = strata.index[strata == MULTI]
            if len(multi):
                raise InvalidParameterError(
                    "strata_col",
                    f"'{strata_col}' changes within {len(multi)} compared path(s), "
                    f"e.g. {list(multi[:3])}",
                    ["a segment constant per path"],
                )
            strata_levels = sorted(pd.unique(strata), key=str)
            pre_exposure, source = _strata_flag(
                stream, strata_col, strata_pre_exposure, strata_labels is not None
            )

        # ---- segments and cells
        seg_rows, seg_masks = [], []
        labels_arr = None if strata is None else strata.to_numpy()
        period_labels = [None] if periods is None else list(periods["period"])
        for stratum in strata_levels:
            for p_pos, p_label in enumerate(period_labels):
                in_stratum = (
                    np.ones(len(path_ids), dtype=bool)
                    if stratum is None
                    else labels_arr == stratum
                )
                seg_rows.append((stratum, p_label, "row", p_pos))
                seg_masks.append(pops[:, p_pos] & in_stratum)
                if stratum is not None:
                    seg_rows.append((stratum, p_label, "rest", p_pos))
                    seg_masks.append(pops[:, p_pos] & ~in_stratum)
        segments = pd.DataFrame(
            seg_rows, columns=["stratum", "period", "role", "period_pos"]
        )
        masks = np.stack(seg_masks, axis=1)
        cell_rows = []
        for spec_pos, spec in enumerate(specs):
            for seg_pos, seg in segments.iterrows():
                cell_rows.append(
                    (
                        spec.name,
                        spec_pos,
                        seg_pos,
                        seg["stratum"],
                        seg["period"],
                        seg["role"],
                    )
                )
        cells = pd.DataFrame(
            cell_rows,
            columns=["metric", "spec_pos", "segment", "stratum", "period", "role"],
        )
        return cls(
            path_ids=path_ids,
            arm=arm,
            specs=specs,
            values=values,
            has_events=has_events,
            segments=segments,
            masks=masks,
            cells=cells,
            periods=periods,
            immature=immature,
            strata=strata,
            strata_pre_exposure=pre_exposure,
            strata_pre_exposure_source=source,
            with_assignment=assignment is not None,
            excluded_paths=int(groups.excluded_paths),
        )

    # ------------------------------------------------------------------ access
    def column(self, cell: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """``(num, den, pop)`` of one cell (n_paths each).

        ``pop`` is the cell's population; ``den`` is 1 on it for 0/1 and count
        metrics, 1 on paths with a value for value metrics, the denominator metric for
        ratios; both are 0 outside the population.
        """
        row = self.cells.iloc[cell]
        spec = self.specs[row["spec_pos"]]
        seg = self.segments.iloc[row["segment"]]
        pop = self.masks[:, row["segment"]]
        num, den = self.values[spec.name]
        p = seg["period_pos"]
        x = num[:, p]
        if spec.kind == "ratio":
            return np.where(pop, x, 0.0), np.where(pop, den[:, p], 0.0), pop
        if spec.kind == "value":
            has = pop & np.isfinite(x)
            return np.where(has, x, 0.0), has.astype(float), pop
        return np.where(pop, x, 0.0), pop.astype(float), pop

    def columns(self, cells: list[int]) -> tuple[np.ndarray, np.ndarray]:
        """Numerator and denominator matrices ``(n_paths, len(cells))``."""
        num = np.empty((len(self.path_ids), len(cells)))
        den = np.empty_like(num)
        for j, cell in enumerate(cells):
            num[:, j], den[:, j], _ = self.column(cell)
        return num, den

    def fingerprint(self) -> str:
        """Hash of ``(path_ids, arm, cells)``; the bootstrap cache key."""
        h = hashlib.sha256()
        h.update(
            pd.util.hash_pandas_object(pd.Series(self.path_ids.astype(str))).values
        )
        h.update(self.arm.tobytes())
        h.update(self.cells.to_csv(index=False).encode())
        return h.hexdigest()[:16]


def _period_keys(
    df: pd.DataFrame,
    ts_col: str,
    path_col: str,
    period: Any,
    basis: str,
    assigned_at: pd.Series | None,
    cov_start: pd.Timestamp,
    cov_end: pd.Timestamp,
) -> tuple[np.ndarray, pd.DataFrame]:
    """Integer period key per row and the period table (sorted chronologically)."""
    ts = df[ts_col]
    if basis == "since_assignment":
        length = _fixed_length(period, "period")
        if length % _DAY == pd.Timedelta(0):
            unit, suffix = _DAY, ""
        elif length % _HOUR == pd.Timedelta(0):
            unit, suffix = _HOUR, "h"
        else:
            raise InvalidParameterError(
                "period", period, ["a whole number of days or hours"]
            )
        since = ts - df[path_col].map(assigned_at)
        keys = (since // length).to_numpy().astype(np.int64)
        uniq = np.unique(keys)
        size = length // unit
        rows = []
        for k in uniq:
            a = int(k) * size
            rows.append(
                {
                    "period": f"{a}-{a + size - 1}{suffix}",
                    "period_start": length * int(k),
                    "period_end": length * (int(k) + 1),
                    "period_complete": True,
                }
            )
        return keys, pd.DataFrame(rows, index=pd.Index(uniq, name="key"))

    length, freq, n = _calendar_period(period)
    if length is not None:
        keys = ((ts - cov_start) // length).to_numpy().astype(np.int64)
        uniq = np.unique(keys)
        rows = []
        for k in uniq:
            start = cov_start + length * int(k)
            end = start + length
            if length % _DAY == pd.Timedelta(0):
                label = (
                    f"{start:%Y-%m-%d}"
                    if length == _DAY
                    else f"{start:%Y-%m-%d}/{end - _DAY:%Y-%m-%d}"
                )
            else:
                label = f"{start:%Y-%m-%d %H:%M}"
            rows.append({"period": label, "period_start": start, "period_end": end})
    else:
        # calendar units in the stream's timezone: bin the local wall time
        tz = getattr(ts.dt, "tz", None)
        local = ts.dt.tz_localize(None) if tz is not None else ts
        keys = local.dt.to_period(freq).array.asi8.astype(np.int64)
        if n > 1:
            # n units per bin, counted from the unit holding the stream's first day
            first = cov_start.tz_localize(None) if tz is not None else cov_start
            origin = pd.Period(first, freq=freq).ordinal
            keys = origin + (keys - origin) // n * n
        uniq = np.unique(keys)
        rows = []
        for k in uniq:
            p = pd.Period(ordinal=int(k), freq=freq)
            start, end = p.start_time, (p + n).start_time
            if tz is not None:
                start, end = start.tz_localize(tz), end.tz_localize(tz)
            label = str(p) if n == 1 else f"{start:%Y-%m-%d}/{end - _DAY:%Y-%m-%d}"
            rows.append({"period": label, "period_start": start, "period_end": end})
    table = pd.DataFrame(rows, index=pd.Index(uniq, name="key"))
    table["period_complete"] = (table["period_start"] >= cov_start) & (
        table["period_end"] <= cov_end
    )
    return keys, table


def _metric_values(
    work,
    configs: list[dict],
    path_col: str,
    path_ids: pd.Index,
    period_keys: np.ndarray | None,
    periods: pd.DataFrame | None,
) -> tuple[np.ndarray, np.ndarray]:
    """``(values, active)``: values ``(n_paths, n_periods, n_configs)`` and the
    ``(n_paths, n_periods)`` mask of paths with an event in the period.

    Paths without events in a period are NaN (callers fill 0 where the metric is a
    total); the registry is not re-validated (configs were checked on the source
    stream, and an event absent from a slice is a zero, not a typo).
    """
    from retentioneering.metrics.composite import COMPOSITE_COL, composite_path_stream
    from retentioneering.metrics.metric_builder import MetricBuilder

    n_periods = 1 if periods is None else len(periods)
    out = np.full((len(path_ids), n_periods, len(configs)), np.nan)
    active = np.zeros((len(path_ids), n_periods), dtype=bool)
    if work.df.empty:
        return out, active
    if periods is None:
        frame = MetricBuilder(work).build_metrics(configs, path_col, validate=False)
        pos = path_ids.get_indexer(frame.index)
        ok = pos >= 0
        out[pos[ok], 0, :] = _numeric(frame).to_numpy()[ok]
        active[pos[ok], 0] = True
        return out, active
    derived, back = composite_path_stream(work, period_keys, path_col)
    frame = MetricBuilder(derived).build_metrics(configs, COMPOSITE_COL, validate=False)
    back = back.reindex(frame.index)
    pos = path_ids.get_indexer(back[path_col])
    ppos = periods.index.get_indexer(back["key"].astype(np.int64))
    ok = (pos >= 0) & (ppos >= 0)
    out[pos[ok], ppos[ok], :] = _numeric(frame).to_numpy()[ok]
    active[pos[ok], ppos[ok]] = True
    return out, active


def _numeric(frame: pd.DataFrame) -> pd.DataFrame:
    """Metric frame as floats; a non-numeric column is a config error."""
    out = {}
    for j, col in enumerate(frame.columns):
        series = frame.iloc[:, j]
        if series.dtype == bool or pd.api.types.is_bool_dtype(series):
            series = series.astype(float)
        if not pd.api.types.is_numeric_dtype(series):
            converted = pd.to_numeric(series, errors="coerce")
            if converted.notna().sum() < series.notna().sum():
                raise InvalidMetricConfigError(
                    f"metric column '{col}' is not numeric (dtype {series.dtype}); "
                    "a scorecard metric must be a number per path."
                )
            series = converted
        out[j] = series.astype(float).to_numpy()
    return pd.DataFrame(out, index=frame.index)


# ======================================================================= scorecard
@dataclass
class ABTest:
    """Scorecard computation. Built by ``Eventstream.get_ab_test`` / ``get_ab_bootstrap``
    through :meth:`from_stream`."""

    groups: DiffGroups
    specs: list[ABMetricSpec]
    matrix: MetricMatrix
    ci_level: float = 0.95
    power: float = 0.8
    correction: str = "holm"
    n_bootstrap: int | str = 1000
    bootstrap_weights: str = "multinomial"
    min_events: int = 10
    random_state: int = 42
    period_population: str = "itt"
    has_horizon: bool = False
    period_basis: str = "calendar"
    _replicates_cache: dict = field(default_factory=dict, repr=False)

    # ------------------------------------------------------------------ factory
    @classmethod
    def from_stream(
        cls,
        stream: "Eventstream",
        diff: Any,
        metrics: list[dict[str, Any]],
        *,
        assignment: pd.Series | pd.DataFrame | None = None,
        strata_col: str | None = None,
        period: Any | None = None,
        period_basis: str = "calendar",
        period_population: str = "itt",
        horizon: Any | None = None,
        test: str = "auto",
        correction: str = "holm",
        ci_level: float = 0.95,
        power: float = 0.8,
        n_bootstrap: int | str = 1000,
        bootstrap_weights: str = "multinomial",
        min_events: int = 10,
        random_state: int = 42,
        path_col: str | None = None,
        strata_pre_exposure: bool | None = None,
    ) -> "ABTest":
        """Validate the arguments, resolve the groups, build the matrix."""
        schema = stream.schema
        for name, value, allowed in (
            ("test", test, TESTS),
            ("correction", correction, CORRECTIONS),
            ("period_basis", period_basis, PERIOD_BASES),
            ("period_population", period_population, PERIOD_POPULATIONS),
            ("bootstrap_weights", bootstrap_weights, BOOTSTRAP_WEIGHTS),
        ):
            if not isinstance(value, str) or value not in allowed:
                raise InvalidParameterError(name, value, list(allowed))
        for name, value, low, allowed in (
            ("ci_level", ci_level, 0.5, "a float in [0.5, 1), e.g. 0.95"),
            ("power", power, 0.0, "a float in (0, 1)"),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not low <= value < 1
                or value <= 0
            ):
                raise InvalidParameterError(name, value, [allowed])
        if isinstance(min_events, bool) or not isinstance(
            min_events, (int, np.integer)
        ):
            raise InvalidParameterError("min_events", min_events, ["an int >= 1"])
        if min_events < 1:
            raise InvalidParameterError("min_events", min_events, ["an int >= 1"])
        if n_bootstrap != "auto" and (
            isinstance(n_bootstrap, bool)
            or not isinstance(n_bootstrap, (int, np.integer))
            or n_bootstrap < bs.MIN_REPLICATES
        ):
            # Fewer replicates give NaN intervals on every bootstrap row.
            raise InvalidParameterError(
                "n_bootstrap",
                n_bootstrap,
                [f"an int >= {bs.MIN_REPLICATES}", "auto"],
            )
        if isinstance(random_state, bool) or not isinstance(
            random_state, (int, np.integer)
        ):
            raise InvalidParameterError("random_state", random_state, ["an int"])
        path_col = path_col or schema.path_col
        if path_col not in schema.path_cols:
            raise InvalidParameterError("path_col", path_col, list(schema.path_cols))
        if period is None:
            # mode switches of period rows mean nothing without period= (ADR-0008)
            for name, value, default in (
                ("period_basis", period_basis, "calendar"),
                ("period_population", period_population, "itt"),
            ):
                if value != default:
                    raise InvalidParameterError(
                        name, value, [f"'{default}' without period", "a period="]
                    )
        if period is not None and path_col != schema.path_cols[0]:
            raise InvalidParameterError(
                "path_col",
                path_col,
                [f"'{schema.path_cols[0]}' (period rows need the primary path column)"],
            )
        if strata_pre_exposure is not None and not isinstance(
            strata_pre_exposure, bool
        ):
            raise InvalidParameterError(
                "strata_pre_exposure", strata_pre_exposure, ["None", "True", "False"]
            )
        if strata_pre_exposure is not None and strata_col is None:
            raise InvalidParameterError(
                "strata_pre_exposure",
                strata_pre_exposure,
                ["None without strata_col", "a strata_col="],
            )
        table_strata = (
            strata_col is not None
            and isinstance(assignment, pd.DataFrame)
            and strata_col in assignment.columns
        )
        if (
            strata_col is not None
            and not table_strata
            and strata_col not in schema.segment_cols
        ):
            raise InvalidParameterError(
                "strata_col",
                strata_col,
                list(schema.segment_cols) + ["a column of the assignment DataFrame"],
            )

        table = normalize_assignment(assignment) if assignment is not None else None
        has_time = table is not None and "assigned_at" in table.columns
        if period_basis == "since_assignment" and not has_time:
            raise InvalidParameterError(
                "period_basis",
                period_basis,
                [
                    "'calendar'",
                    "'since_assignment' with an assignment DataFrame "
                    "holding an 'assigned_at' column",
                ],
            )
        if horizon is not None and not has_time:
            raise InvalidParameterError(
                "horizon",
                horizon,
                [
                    "None",
                    "a duration with an assignment DataFrame holding 'assigned_at'",
                ],
            )
        if horizon is not None:
            _fixed_length(horizon, "horizon")
        table = prepare_assignment(stream, path_col, table)
        strata_labels = None
        if table_strata:
            # silent assigned paths keep their stratum (rows stay ITT per stratum)
            strata_labels = assignment[strata_col].set_axis(table.index, axis=0)
        specs = parse_metrics(metrics, test)
        _validate_on_stream(stream, specs)
        groups = resolve_diff(
            stream, diff, path_col, require_constant=True, assignment=table
        )
        matrix = MetricMatrix.build(
            stream,
            groups,
            specs,
            strata_col=strata_col,
            period=period,
            period_basis=period_basis,
            period_population=period_population,
            assignment=table,
            path_col=path_col,
            horizon=horizon,
            strata_pre_exposure=strata_pre_exposure,
            strata_labels=strata_labels,
        )
        return cls(
            groups=groups,
            specs=specs,
            matrix=matrix,
            ci_level=float(ci_level),
            power=float(power),
            correction=correction,
            n_bootstrap=n_bootstrap,
            bootstrap_weights=bootstrap_weights,
            min_events=int(min_events),
            random_state=int(random_state),
            period_population=period_population if period is not None else "itt",
            has_horizon=horizon is not None,
            period_basis=period_basis if period is not None else "calendar",
        )

    # ------------------------------------------------------------------ public
    @property
    def alpha(self) -> float:
        return 1.0 - self.ci_level

    def resolved_n_bootstrap(self) -> int:
        """``n_bootstrap``, with ``"auto"`` = ``max(1000, ceil(20 * m / alpha))``."""
        if self.n_bootstrap != "auto":
            return int(self.n_bootstrap)
        rows = self.matrix.cells[self.matrix.cells["role"] == "row"]
        m = int(rows.groupby("metric").size().max())
        return max(1000, math.ceil(20 * m / self.alpha))

    def fit(self) -> pd.DataFrame:
        """Return the scorecard (columns in ``COLUMNS`` order, optional ones dropped)."""
        cells = self.matrix.cells
        boot_cells = [
            i
            for i, row in cells.iterrows()
            if self.specs[row["spec_pos"]].test == "bootstrap"
            or self.specs[row["spec_pos"]].kind != "binary"
        ]
        reps = self._replicates(boot_cells)
        records, extras, rest = [], [], {}
        for i, row in cells.iterrows():
            record, extra = self._test_cell(i, reps.get(i))
            if row["role"] == "row":
                records.append(record)
                extras.append(extra)
            else:
                key = (row["metric"], row["stratum"], row["period"])
                rest[key] = (record["delta"], record["delta_se"])
        table = pd.DataFrame(records)
        table = self._adjust(table, extras)
        if self.matrix.strata is not None:
            table = self._strata_columns(table, extras, rest)
        return self._finalise(table)

    def bootstrap(
        self, contrast: dict[str, tuple[list[str], list[str]]] | None = None
    ) -> pd.DataFrame:
        """Return the long replicate table (``replicate, metric, [stratum], [period |
        contrast], group1_value, group2_value, delta``) for every metric, plus
        contrast rows."""
        cells = self.matrix.cells
        row_cells = list(cells.index[cells["role"] == "row"])
        n_boot = self.resolved_n_bootstrap()
        num, den = self.matrix.columns(row_cells)
        side_cells = None
        if contrast:
            num_s, den_s, side_cells = self._contrast_columns(contrast)
            num, den = np.hstack([num, num_s]), np.hstack([den, den_s])
        result = self._run_bootstrap(num, den, n_boot)
        has_stratum = self.matrix.strata is not None
        has_period = self.matrix.periods is not None
        frames = []
        replicate = np.arange(n_boot)
        for j, cell in enumerate(row_cells):
            row = cells.loc[cell]
            frame = pd.DataFrame({"replicate": replicate, "metric": row["metric"]})
            if has_stratum:
                frame["stratum"] = row["stratum"]
            if has_period:
                frame["period"] = row["period"]
            frame["group1_value"] = result.group1[:, j]
            frame["group2_value"] = result.group2[:, j]
            frame["delta"] = result.group1[:, j] - result.group2[:, j]
            frames.append(frame)
        out = pd.concat(frames, ignore_index=True)
        if contrast:
            offset = len(row_cells)
            side = BootstrapResult(
                result.group1[:, offset:],
                result.group2[:, offset:],
                result.valid[offset:],
                n_boot,
                result.weights,
            )
            extra = bs.contrast_rows(side, side_cells, contrast)
            out["contrast"] = np.nan
            out = pd.concat([out, extra], ignore_index=True)
        columns = [c for c in BOOTSTRAP_COLUMNS if c in out.columns]
        return out[columns]

    # ------------------------------------------------------------------ bootstrap
    def _run_bootstrap(self, num, den, n_boot) -> BootstrapResult:
        """Bootstrap in column chunks; replicates of a column do not depend on the chunk."""
        g1, g2, valid = [], [], []
        for start in range(0, num.shape[1], _BOOTSTRAP_CHUNK):
            stop = start + _BOOTSTRAP_CHUNK
            res = bs.bootstrap_paths(
                num[:, start:stop],
                den[:, start:stop],
                self.matrix.arm,
                n_boot,
                self.bootstrap_weights,
                random_state=self.random_state,
            )
            g1.append(res.group1)
            g2.append(res.group2)
            valid.append(res.valid)
        if not g1:
            empty = np.empty((n_boot, 0))
            return BootstrapResult(
                empty, empty, np.empty(0, int), n_boot, self.bootstrap_weights
            )
        return BootstrapResult(
            np.hstack(g1),
            np.hstack(g2),
            np.concatenate(valid),
            n_boot,
            self.bootstrap_weights,
        )

    def _replicates(self, cells: list[int] | None = None) -> dict[int, tuple]:
        """Draw (or reuse) the joint replicates; ``{cell: (group1, group2, valid)}``."""
        if cells is None:
            cells = list(self.matrix.cells.index)
        missing = [c for c in cells if c not in self._replicates_cache]
        if missing:
            n_boot = self.resolved_n_bootstrap()
            num, den = self.matrix.columns(missing)
            res = self._run_bootstrap(num, den, n_boot)
            for j, cell in enumerate(missing):
                self._replicates_cache[cell] = (
                    res.group1[:, j],
                    res.group2[:, j],
                    int(res.valid[j]),
                )
        return {c: self._replicates_cache[c] for c in cells}

    def _contrast_columns(self, contrast):
        """Pooled-side columns of every contrast: summed numerators and denominators."""
        matrix = self.matrix
        if matrix.periods is None:
            raise InvalidParameterError(
                "contrast", contrast, ["None (contrasts need period=...)"]
            )
        if not isinstance(contrast, dict):
            raise InvalidParameterError(
                "contrast", contrast, ["{'name': (periods_a, periods_b)}"]
            )
        known = list(matrix.periods["period"])
        cells = matrix.cells[matrix.cells["role"] == "row"]
        nums, dens, rows = [], [], []
        for name, sides in contrast.items():
            if not isinstance(sides, (list, tuple)) or len(sides) != 2:
                raise InvalidParameterError(
                    "contrast", {name: sides}, ["(periods_a, periods_b)"]
                )
            sides = [[p] if isinstance(p, str) else list(p) for p in sides]
            for periods in sides:
                unknown = [p for p in periods if p not in known]
                if unknown or not periods:
                    raise InvalidParameterError("contrast", unknown or periods, known)
            # both sides on the same paths: those in the population of every
            # contrasted period (S-04), so the contrast is a within-path change
            # and not a change of cohort mix between early and late bins
            both = cells[cells["period"].isin(sides[0] + sides[1])]
            common = {}
            for key, group in both.groupby(
                ["metric", "stratum"], sort=False, dropna=False
            ):
                common[_cell_key(key)] = np.logical_and.reduce(
                    [matrix.masks[:, seg] for seg in group["segment"]]
                )
            for label, periods in zip(("a", "b"), sides):
                part = cells[cells["period"].isin(periods)]
                for key, group in part.groupby(
                    ["metric", "stratum"], sort=False, dropna=False
                ):
                    n, d = matrix.columns(list(group.index))
                    keep = common[_cell_key(key)][:, None]
                    nums.append((n * keep).sum(axis=1))
                    dens.append((d * keep).sum(axis=1))
                    rows.append(
                        {
                            "metric": key[0],
                            "stratum": key[1],
                            "contrast": name,
                            "side": label,
                        }
                    )
        side_cells = pd.DataFrame(rows)
        if matrix.strata is None:
            side_cells = side_cells.drop(columns="stratum")
        return np.stack(nums, axis=1), np.stack(dens, axis=1), side_cells

    # ------------------------------------------------------------------ one cell
    def _test_cell(self, cell: int, reps: tuple | None) -> tuple[dict, dict]:
        """Estimates, CI, SE, p-value and support flags for one cell.

        Returns the public row fields and a dict of private helpers (``ci_at(level)``
        for adjusted intervals, ``mde_at(alpha)``, Cochran-Q support).
        """
        matrix = self.matrix
        row = matrix.cells.loc[cell]
        spec = self.specs[row["spec_pos"]]
        num, den, pop = matrix.column(cell)
        arm = matrix.arm
        in1, in2 = pop & (arm == 0), pop & (arm == 1)
        record: dict[str, Any] = {"metric": spec.name}
        if matrix.strata is not None:
            record["stratum"] = row["stratum"]
        if matrix.periods is not None:
            record["period"] = row["period"]
        record["group1_paths"] = int(in1.sum())
        record["group2_paths"] = int(in2.sum())
        if matrix.with_assignment:
            silent = ~matrix.has_events
            record["group1_paths_no_events"] = int((in1 & silent).sum())
            record["group2_paths_no_events"] = int((in2 & silent).sum())
        level = self.ci_level
        test = spec.test
        nan = float("nan")
        extra: dict[str, Any] = {"kind": spec.kind}

        # support: numerator events for 0/1 and ratio metrics, paths with a value
        # otherwise; the rule is pooled over the arms (S-02): a behaviour present in
        # one arm only is the strongest effect there is, not missing data
        if spec.kind in ("binary", "ratio"):
            s1 = int(((num > 0) & in1).sum())
            s2 = int(((num > 0) & in2).sum())
        else:
            s1 = int(((den > 0) & in1).sum())
            s2 = int(((den > 0) & in2).sum())
        extra["support"] = s1 + s2 >= self.min_events
        if spec.kind == "binary":
            n1, n2 = int(in1.sum()), int(in2.sum())
            extra["support"] = extra["support"] and (
                (n1 - s1) + (n2 - s2) >= self.min_events
            )
            extra["q_support"] = min(s1, s2) >= 5 and min(n1 - s1, n2 - s2) >= 5
        else:
            extra["q_support"] = min(s1, s2) >= 10
        if min(s1, s2) == 0:
            support_warning = "one_arm_only"
        elif test == "bootstrap" and min(s1, s2) < LOW_SUPPORT_PATHS:
            support_warning = "low_support"
        else:
            support_warning = ""

        valid = pd.NA
        low_support = False
        if test == "z_test":
            k1, n1 = float(num[in1].sum()), float(in1.sum())
            k2, n2 = float(num[in2].sum()), float(in2.sum())
            with np.errstate(invalid="ignore", divide="ignore"):
                p1 = k1 / n1 if n1 else nan
                p2 = k2 / n2 if n2 else nan
            l1, h1 = _stats.wilson_ci(k1, n1, level)
            l2, h2 = _stats.wilson_ci(k2, n2, level)
            dl, dh = _stats.newcombe_ci(k1, n1, k2, n2, level)
            _, p, se = _stats.two_proportion_z(k1, n1, k2, n2)
            rl, rh = _stats.delta_log_ratio_ci(k1, n1, k2, n2, level)
            pooled = (k1 + k2) / (n1 + n2) if n1 + n2 else nan
            extra["ci_at"] = lambda lv: _stats.newcombe_ci(k1, n1, k2, n2, lv)
            extra["mde_at"] = lambda a: _stats.mde_proportion(
                pooled, n1, n2, a, self.power
            )
            g1, g2 = p1, p2
        elif test == "welch_t_test":
            x1, x2 = num[in1 & (den > 0)], num[in2 & (den > 0)]
            n1, n2 = len(x1), len(x2)
            g1 = float(x1.mean()) if n1 else nan
            g2 = float(x2.mean()) if n2 else nan
            v1 = float(x1.var(ddof=1)) if n1 > 1 else nan
            v2 = float(x2.var(ddof=1)) if n2 > 1 else nan
            _, df, p, se = _stats.welch_t(g1, v1, n1, g2, v2, n2)
            l1, h1 = _t_interval(g1, v1, n1, level)
            l2, h2 = _t_interval(g2, v2, n2, level)
            se_f, df_f = float(se), float(df)

            def ci_at(lv, d=g1 - g2, se_f=se_f, df_f=df_f):
                if not (math.isfinite(se_f) and math.isfinite(df_f)):
                    return nan, nan
                q = stats.t.ppf(0.5 + lv / 2, df_f)
                return d - q * se_f, d + q * se_f

            dl, dh = ci_at(level)
            rl, rh = self._rel_ci(reps, g2)
            extra["ci_at"] = ci_at
            extra["mde_at"] = lambda a, se_f=se_f: _stats.mde_from_se(
                se_f, a, self.power
            )
        else:
            sn1, sd1 = float(num[in1].sum()), float(den[in1].sum())
            sn2, sd2 = float(num[in2].sum()), float(den[in2].sum())
            g1 = sn1 / sd1 if sd1 > 0 else nan
            g2 = sn2 / sd2 if sd2 > 0 else nan
            r1, r2, valid = reps
            n_boot = len(r1)
            low_support = (n_boot - valid) > 0.01 * n_boot
            d_reps = (r1 - r2)[:, None]
            l1, h1 = (x[0] for x in bs.percentile_ci(r1, level))
            l2, h2 = (x[0] for x in bs.percentile_ci(r2, level))
            dl, dh = (x[0] for x in bs.percentile_ci(d_reps, level))
            se = bs.bootstrap_se(d_reps)[0]
            p = bs.bootstrap_p(np.array([g1 - g2]), d_reps)[0]
            rl, rh = self._rel_ci(reps, g2)
            se_f = float(se)

            def ci_at(lv, d_reps=d_reps):
                low, high = bs.percentile_ci(d_reps, lv)
                return low[0], high[0]

            extra["ci_at"] = ci_at
            extra["mde_at"] = lambda a, se_f=se_f: _stats.mde_from_se(
                se_f, a, self.power
            )
            if low_support:
                # an absent estimate is NaN: no interval, no p, no SE, no MDE
                l1 = h1 = l2 = h2 = dl = dh = rl = rh = p = se = nan
                extra["ci_at"] = lambda lv: (nan, nan)
                extra["mde_at"] = lambda a: nan

        delta = g1 - g2
        if not (isinstance(g2, float) and g2 > 0):
            rel = rl = rh = nan
        else:
            rel = g1 / g2 - 1
        p = float(p)
        descriptive = self.period_population == "active"
        if descriptive or not extra["support"]:
            p = nan
        extra["descriptive"] = descriptive
        record.update(
            group1_value=float(g1),
            group1_ci_low=float(l1),
            group1_ci_high=float(h1),
            group2_value=float(g2),
            group2_ci_low=float(l2),
            group2_ci_high=float(h2),
            delta=float(delta),
            delta_se=float(se),
            delta_ci_low=float(dl),
            delta_ci_high=float(dh),
            rel_delta=float(rel),
            rel_delta_ci_low=float(rl),
            rel_delta_ci_high=float(rh),
            p_value=p,
            test=test,
            ci_basis=_CI_BASIS[test],
            p_basis=_P_BASIS[test],
            valid_replicates=valid if test == "bootstrap" else pd.NA,
            low_support=bool(low_support),
            support_warning=support_warning,
        )
        return record, extra

    def _rel_ci(self, reps: tuple | None, g2: float) -> tuple[float, float]:
        """Percentile interval of ``group1 / group2 - 1`` over the joint replicates."""
        if reps is None or not (isinstance(g2, float) and g2 > 0):
            return float("nan"), float("nan")
        r1, r2, _ = reps
        with np.errstate(invalid="ignore", divide="ignore"):
            ratio = np.where(r2 > 0, r1 / r2 - 1, np.nan)
        low, high = bs.percentile_ci(ratio[:, None], self.ci_level)
        return float(low[0]), float(high[0])

    # ------------------------------------------------------------------ table
    def _family_label(self, metric: str) -> str:
        if self.correction == "none":
            return "none"
        dims = []
        if self.matrix.strata is not None:
            dims.append("strata")
        if self.matrix.periods is not None:
            dims.append("periods")
        return f"metric={metric}" + (f"/{' x '.join(dims)}" if dims else "")

    def _adjust(self, rows: pd.DataFrame, extras: list[dict]) -> pd.DataFrame:
        """Apply the correction per family, add ``alpha_used``, ``mde`` at ``alpha_used``
        and ``verdict``."""
        adjust = {
            "holm": _stats.holm,
            "bonferroni": _stats.bonferroni,
            "bh": _stats.benjamini_hochberg,
            "none": lambda p: np.asarray(p, dtype=float),
        }[self.correction]
        rows = rows.copy()
        rows["p_value_adjusted"] = np.nan
        rows["correction_family"] = ""
        rows["alpha_used"] = self.alpha
        for metric, index in rows.groupby("metric", sort=False).groups.items():
            p = rows.loc[index, "p_value"].to_numpy(dtype=float)
            rows.loc[index, "p_value_adjusted"] = adjust(p)
            m = int(np.isfinite(p).sum())
            if self.correction != "none" and m > 1:
                rows.loc[index, "alpha_used"] = self.alpha / m
            rows.loc[index, "correction_family"] = self._family_label(metric)
        mde, low_adj, high_adj = [], [], []
        for pos, extra in enumerate(extras):
            a = float(rows["alpha_used"].iloc[pos])
            mde.append(
                float("nan") if extra["descriptive"] else float(extra["mde_at"](a))
            )
            if self.correction == "bonferroni":
                lo, hi = extra["ci_at"](1 - a)
                low_adj.append(float(lo))
                high_adj.append(float(hi))
        rows["mde"] = mde
        g2 = rows["group2_value"].to_numpy(dtype=float)
        with np.errstate(invalid="ignore", divide="ignore"):
            rows["rel_mde"] = np.where(g2 > 0, rows["mde"] / g2, np.nan)
        if self.correction == "bonferroni":
            rows["delta_ci_low_adj"] = low_adj
            rows["delta_ci_high_adj"] = high_adj
        significant = rows["p_value_adjusted"] < self.alpha
        verdict = np.where(
            significant,
            np.where(rows["delta"] > 0, "increase", "decrease"),
            "inconclusive",
        ).astype(object)
        for pos, extra in enumerate(extras):
            if extra["descriptive"]:
                verdict[pos] = "descriptive"
            elif not extra["support"]:
                verdict[pos] = "insufficient_data"
        rows["verdict"] = verdict
        return rows

    def _strata_columns(
        self, rows: pd.DataFrame, extras: list[dict], rest: dict
    ) -> pd.DataFrame:
        """Add shares by arm, chi-square balance p-value, Cochran Q with the support
        guard, the stratum-vs-rest test and the exploratory flag."""
        rows = rows.copy()
        matrix = self.matrix
        segs = matrix.segments
        arm = matrix.arm
        share1, share2, balance = {}, {}, {}
        row_segs = segs[segs["role"] == "row"]
        for p_pos in pd.unique(row_segs["period_pos"]):
            group = row_segs[row_segs["period_pos"] == p_pos]
            counts = np.array(
                [
                    [
                        (matrix.masks[:, s] & (arm == 0)).sum(),
                        (matrix.masks[:, s] & (arm == 1)).sum(),
                    ]
                    for s in group.index
                ],
                dtype=float,
            )
            with np.errstate(invalid="ignore", divide="ignore"):
                shares = counts / counts.sum(axis=0)
            _, p_bal, _ = _stats.chi2_balance(counts)
            for stratum, sh in zip(group["stratum"], shares):
                share1[(stratum, p_pos)] = sh[0]
                share2[(stratum, p_pos)] = sh[1]
            balance[p_pos] = p_bal
        if matrix.periods is not None:
            pos_of = {label: i for i, label in enumerate(matrix.periods["period"])}
            period_col = rows["period"]
            period_pos = [pos_of[label] for label in period_col]
        else:
            period_col = pd.Series([None] * len(rows), index=rows.index)
            period_pos = [0] * len(rows)
        keys = list(zip(rows["stratum"], period_pos))
        rows["group1_share"] = [share1[k] for k in keys]
        rows["group2_share"] = [share2[k] for k in keys]
        rows["balance_p_value"] = [balance[k[1]] for k in keys]

        q_support = np.array([e["q_support"] for e in extras], dtype=bool)
        het_p = np.full(len(rows), np.nan)
        het_df = np.zeros(len(rows), dtype=int)
        het_used = np.zeros(len(rows), dtype=int)
        group_keys = ["metric", "period"] if "period" in rows.columns else "metric"
        for _, index in rows.groupby(
            group_keys, sort=False, dropna=False
        ).groups.items():
            pos = rows.index.get_indexer(index)
            _, p_q, df_q, used = _stats.cochran_q(
                rows["delta"].to_numpy()[pos],
                rows["delta_se"].to_numpy()[pos],
                q_support[pos],
            )
            het_p[pos], het_df[pos], het_used[pos] = p_q, df_q, used
        rows["heterogeneity_p_value"] = het_p
        rows["heterogeneity_df"] = het_df
        rows["heterogeneity_strata_used"] = het_used

        vs_rest = []
        for i, (metric, stratum, period) in enumerate(
            zip(rows["metric"], rows["stratum"], period_col)
        ):
            d_r, se_r = rest.get((metric, stratum, period), (np.nan, np.nan))
            d_s, se_s = rows["delta"].iloc[i], rows["delta_se"].iloc[i]
            with np.errstate(invalid="ignore"):
                se = (
                    math.sqrt(se_s**2 + se_r**2)
                    if np.isfinite([se_s, se_r]).all()
                    else np.nan
                )
            if (
                not (np.isfinite(se) and se > 0 and np.isfinite(d_s - d_r))
                or extras[i]["descriptive"]
                or not extras[i]["support"]
            ):
                vs_rest.append(np.nan)
            else:
                vs_rest.append(float(2 * stats.norm.sf(abs(d_s - d_r) / se)))
        rows["stratum_vs_rest_p_value"] = vs_rest
        flag = matrix.strata_pre_exposure
        rows["strata_pre_exposure"] = pd.array([flag] * len(rows), dtype="boolean")
        rows["strata_pre_exposure_source"] = matrix.strata_pre_exposure_source
        rows["exploratory"] = flag is not True
        return rows

    def _finalise(self, rows: pd.DataFrame) -> pd.DataFrame:
        matrix = self.matrix
        rows = rows.copy()
        rows["excluded_paths"] = matrix.excluded_paths
        if matrix.periods is not None:
            meta = matrix.periods.set_index("period")
            rows["period_start"] = rows["period"].map(meta["period_start"])
            rows["period_end"] = rows["period"].map(meta["period_end"])
            rows["period_complete"] = (
                rows["period"].map(meta["period_complete"]).astype(bool)
            )
        if matrix.immature is not None:
            if matrix.periods is not None:
                lookup = dict(zip(matrix.periods["period"], matrix.immature))
                rows["immature_paths"] = rows["period"].map(lookup).astype(int)
            else:
                rows["immature_paths"] = int(matrix.immature[0])
        rows["valid_replicates"] = pd.array(rows["valid_replicates"], dtype="Int64")
        present = [c for c in COLUMNS if c in rows.columns]
        return rows[present].reset_index(drop=True)


def _cell_key(key: tuple) -> tuple:
    """A ``(metric, stratum)`` groupby key with a missing stratum as None (NaN keys
    from two groupbys are different objects and never match in a dict)."""
    return tuple(None if pd.isna(k) else k for k in key)


def _t_interval(mean: float, var: float, n: int, level: float) -> tuple[float, float]:
    """Student t interval of one arm's mean; NaN with fewer than two values."""
    if n < 2 or not math.isfinite(var):
        return float("nan"), float("nan")
    half = stats.t.ppf(0.5 + level / 2, n - 1) * math.sqrt(var / n)
    return mean - half, mean + half


def _validate_on_stream(stream, specs: list[ABMetricSpec]) -> None:
    """Check event names, columns and segment levels against the full stream."""
    from retentioneering.metrics.metric_builder import MetricBuilder

    builder = MetricBuilder(stream)
    available = set(stream.df[stream.schema.event_col].unique().tolist())
    for spec in specs:
        for config in (spec.config, spec.denominator):
            if config is None:
                continue
            try:
                builder.validate_metric_config(config, available_events=available)
            except InvalidMetricConfigError as exc:
                raise InvalidMetricConfigError(f"metric {spec.name!r}: {exc}") from None
