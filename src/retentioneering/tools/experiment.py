"""Assignment-aware event extraction and population contracts for experiments."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime

import numpy as np
import pandas as pd
from scipy import stats

from retentioneering import Eventstream
from retentioneering.metrics.metric_builder import (
    SCALAR_OUTCOME_CAPABILITIES,
    MetricBuilder,
    compile_scalar_outcome,
)
from retentioneering.paths import anchors
from retentioneering.paths.ordered_reach import _catalogue, _vocabulary, ordered_reach
from retentioneering.stats.experiments import (
    _binary_interval_config,
    _binary_intervals,
    _bootstrap_config,
    _draw_sums,
    _summary,
    _tests,
    adjust_pvalues,
)

__all__ = [
    "ExperimentResult",
    "adjust_pvalues",
    "experiment_data",
    "session_funnel_data",
]


def _clean(value):
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if isinstance(value, np.generic):
        return _clean(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    return value


def _snapshot(value):
    """Detached JSON-normalized configuration; never alias caller-owned objects."""
    return json.loads(json.dumps(_clean(value), allow_nan=False))


@dataclass
class ExperimentResult:
    summary: pd.DataFrame
    diagnostics: pd.DataFrame
    metadata: dict
    contributions: pd.DataFrame

    def __post_init__(self):
        self.metadata = _snapshot(self.metadata)

    def to_dict(self):
        """JSON-safe aggregates; no row identifiers or bootstrap samples."""
        return _clean(
            {
                "schema_version": "2.0",
                "summary": self.summary.to_dict("records"),
                "diagnostics": self.diagnostics.to_dict("records"),
                "metadata": self.metadata,
            }
        )

    def to_json(self, path):
        from pathlib import Path

        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, allow_nan=False) + "\n"
        )


def _component(stream, frame, roster_index, spec, path_col, catalog):
    if not isinstance(spec, dict) or set(spec) != {"metric", "metric_args"}:
        raise ValueError("Component requires exactly metric and metric_args")
    metric, args = spec["metric"], spec["metric_args"]
    if metric not in SCALAR_OUTCOME_CAPABILITIES:
        raise ValueError(f"Supported components: {sorted(SCALAR_OUTCOME_CAPABILITIES)}")
    parsed, _ = compile_scalar_outcome(spec)
    event_col = args.get("event_col") or stream.schema.event_col
    vocabulary = _vocabulary(stream, event_col, catalog)
    if metric == "matches_pattern":
        anchors.validate_pattern_tokens(
            anchors.normalize_pattern(args["pattern"], warn=False, param="pattern"),
            vocabulary,
            param="pattern",
        )
    elif args["event"] not in vocabulary:
        raise ValueError(f"Unknown event in source/declared catalogue: {args['event']}")
    if metric == "event_value_sum":
        if args["value_col"] not in stream.df:
            raise ValueError(f"Missing value column: {args['value_col']}")
        selected = frame.loc[frame[event_col].eq(args["event"])]
        values = pd.to_numeric(selected[args["value_col"]], errors="raise")
        if not np.isfinite(values.to_numpy(dtype=float)).all():
            raise ValueError(
                "Nonfinite/missing amount on a matching event; cannot fill with zero"
            )
    if frame.empty:
        return pd.Series(0.0, index=roster_index)
    window_stream = Eventstream(
        frame.copy(), schema=asdict(stream.schema), preprocess=False
    )
    result = (
        MetricBuilder(window_stream)
        ._build_metric(parsed, path_col, stream.schema.event_col)
        .iloc[:, 0]
    )
    return result.astype(float).reindex(roster_index, fill_value=0.0)


def _display(spec, name, kind):
    value = spec.get("display", {})
    if not isinstance(value, dict) or set(value) - {
        "label",
        "unit",
        "scale",
        "direction",
        "role",
    }:
        raise ValueError("display accepts label, unit, scale, direction, role")
    out = {
        "label": name,
        "unit": "proportion" if kind == "binary" else "value",
        "scale": 1.0,
        "direction": "neutral",
        "role": "exploratory",
        **value,
    }
    if not all(isinstance(out[k], str) and out[k] for k in ("label", "unit")):
        raise ValueError("Display label and unit must be nonempty strings")
    if (
        isinstance(out["scale"], bool)
        or not isinstance(out["scale"], (int, float))
        or not np.isfinite(out["scale"])
        or out["scale"] <= 0
    ):
        raise ValueError("Display scale must be finite and positive")
    if out["direction"] not in {"higher_is_better", "lower_is_better", "neutral"}:
        raise ValueError(
            "Display direction must be higher_is_better, lower_is_better, or neutral"
        )
    if out["role"] not in {"primary", "secondary", "guardrail", "exploratory"}:
        raise ValueError(
            "Display role must be primary, secondary, guardrail, or exploratory"
        )
    return out


def _metric_contributions(
    stream, frame, roster_index, metrics, steps, path_col, catalog
):
    specs, pairs, names, eligibility = [], [], set(), []
    cache = {}

    def component(source):
        key = json.dumps(source, sort_keys=True)
        if key not in cache:
            cache[key] = _component(
                stream, frame, roster_index, source, path_col, catalog
            )
        return cache[key]

    for m in metrics:
        if not isinstance(m, dict):
            raise TypeError("Every metric must be a dict")
        name, agg = m.get("name"), m.get("agg")
        if (
            not isinstance(name, str)
            or not name
            or name in names
            or name.startswith("funnel_")
        ):
            raise ValueError(
                "Metrics require unique nonempty names without the reserved funnel_ prefix"
            )
        names.add(name)
        if agg == "mean":
            if set(m) - {"display"} != {"name", "agg", "metric", "metric_args"}:
                raise ValueError(f"Unexpected/missing mean metric keys: {name}")
            a = component({"metric": m["metric"], "metric_args": m["metric_args"]})
            b = pd.Series(1.0, index=roster_index)
        elif agg in {"ratio_of_sums", "mean_of_ratios"}:
            keys = {"name", "agg", "numerator", "denominator"}
            if agg == "mean_of_ratios":
                keys.add("eligibility")
            if set(m) - {"display"} != keys or (
                agg == "mean_of_ratios" and m["eligibility"] != "denominator_positive"
            ):
                raise ValueError(f"Invalid ratio specification: {name}")
            a = component(m["numerator"])
            b = component(m["denominator"])
            if (b < 0).any():
                raise ValueError("Ratio denominator contributions must be nonnegative")
            if agg == "mean_of_ratios":
                eligible = b > 0
                eligibility.append(
                    {
                        "metric": name,
                        "excluded_units": int((~eligible).sum()),
                        "excluded_numerator": float(a[~eligible].sum()),
                    }
                )
                a, b = a.div(b.where(eligible)).fillna(0.0), eligible.astype(float)
        else:
            raise ValueError(f"Unknown aggregation: {agg}")
        pairs.extend([a.to_numpy(), b.to_numpy()])
        kind = (
            SCALAR_OUTCOME_CAPABILITIES[m["metric"]]["kind"]
            if agg == "mean"
            else "ratio"
        )
        specs.append(
            {
                "metric": name,
                "family": "scorecard",
                "aggregation": agg,
                "outcome_kind": kind,
                **_display(m, name, kind),
            }
        )
    if steps:
        reach = ordered_reach(
            stream, steps, path_col=path_col, frame=frame, event_catalog=catalog
        ).reindex(roster_index, fill_value=0.0)
        for j, step in enumerate(steps, 1):
            a = reach[f"reach_{j}"].to_numpy()
            for family in ("funnel_cumulative", "funnel_conditional"):
                b = (
                    np.ones(len(roster_index))
                    if family == "funnel_cumulative" or j == 1
                    else reach[f"reach_{j - 1}"].to_numpy()
                )
                pairs.extend([a, b])
                specs.append(
                    {
                        "metric": f"{family}_{j}",
                        "family": family,
                        "aggregation": "ratio_of_sums",
                        "outcome_kind": "binary"
                        if family == "funnel_cumulative"
                        else "ratio",
                        "label": step,
                        "unit": "proportion",
                        "scale": 1.0,
                        "direction": "neutral",
                        "role": "exploratory",
                        "step_index": j,
                        "step": step,
                    }
                )
    return np.column_stack(pairs), specs, eligibility


def experiment_data(
    stream,
    *,
    assignments,
    metrics,
    observation,
    variant_col="exp_group",
    control="control",
    treatment="treatment",
    unit_col=None,
    bootstrap=None,
    funnel_steps=None,
    expected_allocation=None,
    tests=None,
    event_catalog=None,
    binary_intervals=None,
):
    """Fixed-calendar two-arm analysis with authoritative assignments.

    Eventstream exposes this computation through its headless and widget methods.
    observation requires start, end, assignment_time_col, complete=True. Silent
    assigned units receive structural zeros only for supported outcomes. No
    automatic post-exposure filtering, optional stopping or winner selection.
    """
    path_col = unit_col or stream.schema.path_col
    catalog = _catalogue(stream, event_catalog)
    binary_config = _binary_interval_config(binary_intervals)
    segment_col = variant_col
    if path_col not in stream.schema.path_cols:
        raise ValueError(f"path_col must be in {stream.schema.path_cols}")
    if not isinstance(observation, dict) or set(observation) != {
        "start",
        "end",
        "assignment_time_col",
        "complete",
    }:
        raise ValueError("observation requires start/end/assignment_time_col/complete")
    if observation["complete"] is not True:
        raise ValueError("Complete event observation must be explicitly asserted")
    start, end = pd.Timestamp(observation["start"]), pd.Timestamp(observation["end"])
    if pd.isna(start) or pd.isna(end) or start >= end:
        raise ValueError("Expected finite start < end")
    a = assignments.copy()
    at = observation["assignment_time_col"]
    required = {path_col, segment_col, at}
    if not required.issubset(a.columns) or a[list(required)].isna().any().any():
        raise ValueError(f"Assignments require nonnull columns {sorted(required)}")
    if control == treatment or not a[path_col].is_unique:
        raise ValueError("Distinct arms and one assignment row per unit are required")
    if not a[segment_col].isin([control, treatment]).all():
        raise ValueError("Assignments contain undeclared arms")
    a[at] = pd.to_datetime(a[at], format="mixed")
    if a[at].isna().any():
        raise ValueError(
            "Assignments require nonnull valid times after timestamp parsing"
        )
    if (a[at] > start).any():
        raise ValueError(
            "All assignments must precede or equal the common window start"
        )
    a = a.sort_values(path_col, kind="stable").set_index(path_col)
    if not all((a[segment_col] == g).any() for g in (control, treatment)):
        raise ValueError("Both arms must be nonempty")
    full = stream.to_dataframe(exclude_start_end=False)
    if not full[path_col].isin(a.index).all():
        raise ValueError(
            "Event unit missing from supplied assignments; restrict cohort stream explicitly"
        )
    time = stream.schema.timestamp_col
    if full[time].isna().any():
        raise ValueError("Missing event time")
    post_assignment = full.loc[full[time].ge(full[path_col].map(a[at]))]
    if (
        segment_col in post_assignment
        and not post_assignment[segment_col]
        .eq(post_assignment[path_col].map(a[segment_col]))
        .all()
    ):
        raise ValueError("Event arm conflicts with assignment roster")
    # Timestamp comparison intentionally rejects naive/aware mismatch.
    frame = full.loc[full[time].ge(start) & full[time].lt(end)].copy()
    if (frame[time] < frame[path_col].map(a[at])).any():
        raise ValueError("Outcome event before assignment")
    if not isinstance(metrics, list) or not (metrics or funnel_steps):
        raise ValueError("Provide metrics and/or nonempty funnel_steps")
    config = _bootstrap_config(bootstrap)
    values, specs, eligibility = _metric_contributions(
        stream, frame, a.index, metrics, funnel_steps, path_col, catalog
    )
    if not np.isfinite(values).all():
        raise ValueError("Nonfinite user contribution")
    cm, tm = (
        a[segment_col].eq(control).to_numpy(),
        a[segment_col].eq(treatment).to_numpy(),
    )
    c, t = values[cm], values[tm]
    if config:
        cs, ts = np.random.SeedSequence(config["random_state"]).spawn(2)
        dc, dt = (
            _draw_sums(c, config, np.random.default_rng(cs)),
            _draw_sums(t, config, np.random.default_rng(ts)),
        )
    else:
        dc = dt = None
    summary = _summary(specs, c, t, dc, dt, len(c), len(t), config)
    _binary_intervals(summary, specs, c, t, binary_config)
    _tests(summary, specs, c, t, tests)
    diagnostics = []
    for group in (control, treatment):
        ids = a.index[a[segment_col].eq(group)]
        active = frame.loc[frame[path_col].isin(ids), path_col].nunique()
        diagnostics.append(
            {
                "arm": group,
                "n_assigned": len(ids),
                "n_observed": int(active),
                "n_without_events": len(ids) - int(active),
            }
        )
    srm = {"status": "not_requested"}
    if expected_allocation is not None:
        if isinstance(expected_allocation, bool) or not 0 < expected_allocation < 1:
            raise ValueError(
                "expected_allocation must be a treatment probability in (0,1)"
            )
        srm = {
            "method": "exact_binomial_two_sided",
            "scope": "supplied_assignment_roster",
            "scheme_assumption": "independent_bernoulli",
            "expected_treatment": expected_allocation,
            "p_value": float(
                stats.binomtest(len(t), len(a), expected_allocation).pvalue
            ),
        }
    cols = [
        f"{s['metric']}__{role}" for s in specs for role in ("numerator", "denominator")
    ]
    contributions = pd.DataFrame(values, index=a.index, columns=cols).join(
        a[[segment_col]]
    )
    meta = {
        "input_fingerprints": {
            "assignments": _frame_fingerprint(a),
            "source_events": _frame_fingerprint(full),
        },
        "api_status": "native_development",
        "population_basis": "assignment_roster",
        "path_col": path_col,
        "unit_col": path_col,
        "variant_col": variant_col,
        "control": control,
        "treatment": treatment,
        "observation": observation,
        "bootstrap": config,
        "sampling_design": "independent_within_arm_users",
        "intervals": "pointwise",
        "effect_direction": "treatment_minus_control",
        "metrics": metrics,
        "outcomes": specs,
        "event_catalog": catalog,
        "binary_intervals": binary_config,
        "funnel_steps": funnel_steps,
        "srm": srm,
        "tests": tests,
        "eligibility": eligibility,
        "warnings": ["low_resample_count"]
        if config and config["n_resamples"] < 1000
        else [],
    }
    return ExperimentResult(summary, pd.DataFrame(diagnostics), meta, contributions)


def session_funnel_data(
    stream,
    *,
    steps,
    unit_col="user_id",
    path_col="session_id",
    segment_col="exp_group",
    control="control",
    treatment="treatment",
    bootstrap=None,
    event_catalog=None,
    binary_intervals=None,
):
    """Descriptive path funnel; pooled cluster draws preserve group overlap.

    Denominators are observed paths, not assignments. Filter the time window
    explicitly first. A path can contribute to both dynamic segment levels.
    """
    for col in (unit_col, path_col):
        if col not in stream.schema.path_cols:
            raise ValueError(
                "unit_col and path_col must be declared nested path columns"
            )
    catalog = _catalogue(stream, event_catalog)
    if binary_intervals is not None:
        raise ValueError(
            "Binary intervals require independent assigned Bernoulli units; descriptive session ratios use cluster bootstrap"
        )
    if control == treatment:
        raise ValueError("Distinct group labels required")
    frame = stream.to_dataframe(exclude_start_end=False)
    if (
        segment_col not in frame
        or frame.groupby(path_col)[unit_col].nunique().gt(1).any()
    ):
        raise ValueError("Missing segment or a path is nested in multiple units")
    groups = [frame.loc[frame[segment_col].eq(g)].copy() for g in (control, treatment)]
    if any(g.empty for g in groups):
        raise ValueError("Both descriptive groups must contain observed paths")
    union = pd.Index(
        sorted(set(groups[0][unit_col]) | set(groups[1][unit_col])), name=unit_col
    )
    matrices, unit_counts, path_counts = [], [], []
    specs = []
    for frame_g in groups:
        reach = ordered_reach(
            stream, steps, path_col=path_col, frame=frame_g, event_catalog=catalog
        )
        owner = (
            frame_g[[path_col, unit_col]]
            .drop_duplicates()
            .set_index(path_col)[unit_col]
        )
        contributions, group_specs = [], []
        for j, step in enumerate(steps, 1):
            for family in ("funnel_cumulative", "funnel_conditional"):
                a = reach[f"reach_{j}"]
                b = (
                    pd.Series(1.0, index=reach.index)
                    if family == "funnel_cumulative" or j == 1
                    else reach[f"reach_{j - 1}"]
                )
                contributions.extend(
                    [
                        a.groupby(owner)
                        .sum()
                        .reindex(union, fill_value=0.0)
                        .to_numpy(),
                        b.groupby(owner)
                        .sum()
                        .reindex(union, fill_value=0.0)
                        .to_numpy(),
                    ]
                )
                group_specs.append(
                    {
                        "metric": f"{family}_{j}",
                        "family": family,
                        "aggregation": "ratio_of_sums",
                        "outcome_kind": "ratio",
                        "label": step,
                        "unit": "proportion",
                        "scale": 1.0,
                        "direction": "neutral",
                        "role": "exploratory",
                        "step_index": j,
                        "step": step,
                    }
                )
        matrices.append(np.column_stack(contributions))
        specs = group_specs
        unit_counts.append(frame_g[unit_col].nunique())
        path_counts.append(frame_g[path_col].nunique())
    c, t = matrices
    config = _bootstrap_config(bootstrap)
    if config:
        joined = _draw_sums(
            np.column_stack([c, t]),
            config,
            np.random.default_rng(config["random_state"]),
        )
        dc, dt = joined[:, : c.shape[1]], joined[:, c.shape[1] :]
    else:
        dc = dt = None
    summary = _summary(specs, c, t, dc, dt, *unit_counts, config)
    meta = {
        "api_status": "native_development",
        "population_basis": "observed_paths",
        "path_col": path_col,
        "unit_col": unit_col,
        "control": control,
        "treatment": treatment,
        "sampling_design": "pooled_union_clusters",
        "n_union_units": len(union),
        "n_overlapping_units": len(set(groups[0][unit_col]) & set(groups[1][unit_col])),
        "bootstrap": config,
        "binary_intervals": None,
        "event_catalog": catalog,
        "outcomes": specs,
        "intervals": "pointwise",
        "funnel_steps": steps,
    }
    diag = pd.DataFrame(
        {"arm": [control, treatment], "n_units": unit_counts, "n_paths": path_counts}
    )
    return ExperimentResult(summary, diag, meta, pd.DataFrame())


def _frame_fingerprint(frame):
    """Identity only; raw values and IDs are not included in result metadata."""
    digest = hashlib.sha256()
    digest.update(
        json.dumps([(str(c), str(frame[c].dtype)) for c in frame.columns]).encode()
    )
    try:
        values = pd.util.hash_pandas_object(frame, index=True).to_numpy().tobytes()
    except TypeError:
        values = frame.to_json(orient="split", date_format="iso").encode()
    digest.update(values)
    return digest.hexdigest()
