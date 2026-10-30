"""Which transitions and step-matrix cells differ between the two arms of an A/B test.

``get_ab_transitions`` tests every edge of the transition graph, and ``get_ab_steps``
every cell of the step matrix, between two arms, with intervals and a correction over
the edges (cells) that carry enough support: an exploratory screen with false
discovery rate control (Benjamini-Hochberg by default), run after the scorecard.

Design (ADR-0017, amendment "Transitions and steps")
----------------------------------------------------
* The arms, the population and the window are the scorecard's: ``diff`` resolved per
  path, the unit guard, an ``assignment`` table for intention to treat (silent
  assigned paths in the denominators with no transition), ``horizon`` with its
  immature paths (:func:`~retentioneering.tools.ab_test.prepare_population`,
  :func:`~retentioneering.tools.ab_test.analysed_window`).
* Each ``edge_weight`` is a per-path quantity whose arm value equals the matrix
  ``transition_graph_data(edge_weight, diff=...)`` draws for that arm (on the
  stream's population): the 0/1 "path has the edge" for ``unique_paths`` (the graph
  shows the number of such paths, the row their share), the edge count per path for
  ``avg_per_path``, and a ratio of sums for the probabilities and the share of total.
  The step matrix cell ``(event, k)`` is the 0/1 "the path's k-th event is ``event``".
* Per-path counts are computed once (the ``lead()`` query of ``TransitionMatrix``)
  and kept sparse; the tests are vectorised over edges with the scorecard's
  primitives (:mod:`~retentioneering.tools._stats`, the joint path bootstrap of
  :mod:`~retentioneering.tools._bootstrap`), and the support rule, verdict and
  reading are the scorecard's.
* An edge (cell) below the pooled support rule is reported but not tested and is not
  in the correction family; ``n_tested`` (in ``DataFrame.attrs`` and in every
  ``reading``) is the family size.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from scipy import sparse, stats

from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _bootstrap as bs
from retentioneering.tools import _stats
from retentioneering.tools._diff import resolve_diff
from retentioneering.tools.ab_test import (
    BOOTSTRAP_WEIGHTS,
    CORRECTIONS,
    MAX_INVALID_REPLICATES,
    TESTS,
    AnalysedWindow,
    _is_segment_diff,
    adjust_p,
    analysed_window,
    check_choice,
    check_levels,
    check_min_support,
    check_n_bootstrap,
    check_path_col,
    check_random_state,
    mde_alpha_of,
    prepare_population,
    resolve_test,
    row_reading,
    settle_support_warning,
    support_rule,
    verdicts,
    window_text,
)
from retentioneering.tools.transition_matrix import path_transition_counts

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream

__all__ = [
    "EDGE_WEIGHTS",
    "TRANSITION_COLUMNS",
    "STEP_COLUMNS",
    "ab_transitions",
    "ab_steps",
    "transitions_payload",
]

#: Edge weights ``get_ab_transitions`` tests, and the kind of per-path quantity each
#: one is (the scorecard's metric kinds).
EDGE_WEIGHTS = (
    "unique_paths",
    "avg_per_path",
    "proba_out",
    "proba_in",
    "share_of_total",
)
_KIND = {
    "unique_paths": "binary",
    "avg_per_path": "count",
    "proba_out": "ratio",
    "proba_in": "ratio",
    "share_of_total": "ratio",
}
#: Weights of ``transition_graph_data`` that are refused, with the reason.
_REFUSED_WEIGHTS = {
    "count": (
        "a count is not a per-path rate: it grows with the number of paths in an "
        "arm, so two arms of different sizes differ by construction. Use "
        "'avg_per_path' (the count per path, the same edge compared fairly)"
    ),
    "time_median": (
        "time between events is not tested in this release (a quantile of the "
        "transition times of the paths that make the transition is conditional "
        "on post-treatment behaviour and needs its own test); compare it "
        "descriptively with transition_graph_data(edge_weight='time_median', diff=...)"
    ),
    "time_q95": (
        "time between events is not tested in this release (a quantile of the "
        "transition times of the paths that make the transition is conditional "
        "on post-treatment behaviour and needs its own test); compare it "
        "descriptively with transition_graph_data(edge_weight='time_q95', diff=...)"
    ),
}

#: Column order shared by both tables after their key columns.
_STAT_COLUMNS: tuple[str, ...] = (
    "population",
    "group1_paths",
    "group2_paths",
    "group1_support",
    "group2_support",
    "conditional",
    "group1_silent_paths",
    "group2_silent_paths",
    "unassigned_paths",
    "immature_paths",
    "group1_value",
    "group2_value",
    "delta",
    "delta_se",
    "delta_ci_low",
    "delta_ci_high",
    "delta_ci_low_adj",
    "delta_ci_high_adj",
    "rel_delta",
    "p_value",
    "p_value_adjusted",
    "correction_family",
    "mde_alpha",
    "mde",
    "rel_mde",
    "verdict",
    "test",
    "ci_basis",
    "p_basis",
    "valid_replicates",
    "support_warning",
    "reading",
)
#: Column order of ``get_ab_transitions`` (optional columns dropped when absent).
TRANSITION_COLUMNS: tuple[str, ...] = (
    "source",
    "target",
    "edge_weight",
    "group1_edge_paths",
    "group2_edge_paths",
) + _STAT_COLUMNS
#: Column order of ``get_ab_steps`` (optional columns dropped when absent).
STEP_COLUMNS: tuple[str, ...] = ("event", "step") + _STAT_COLUMNS

_CI_BASIS = {"z_test": "wilson", "welch_t_test": "welch", "bootstrap": "path_bootstrap"}
_P_BASIS = {"z_test": "pooled_z", "welch_t_test": "welch", "bootstrap": "bootstrap_se"}


# ======================================================================= population
@dataclass
class _Population:
    """The compared paths: the scorecard's population and window."""

    window: AnalysedWindow
    path_col: str
    unassigned_paths: int
    with_assignment: bool
    has_events: np.ndarray
    label1: str
    label2: str
    diff_meta: dict

    @property
    def arm(self) -> np.ndarray:
        return self.window.arm

    @property
    def path_ids(self) -> pd.Index:
        return self.window.path_ids


def _diff_meta(diff: Any, groups) -> dict:
    """What a frame records about its ``diff``, for the graph to check it against."""
    if _is_segment_diff(diff):
        return {
            "diff": [str(diff[0]), str(groups.label1), str(groups.label2)],
            "diff_ids": None,
        }

    def digest(values) -> str:
        text = "\n".join(sorted(str(v) for v in pd.unique(pd.Index(list(values)))))
        return hashlib.sha256(text.encode()).hexdigest()[:16]

    return {"diff": None, "diff_ids": [digest(diff[0]), digest(diff[1])]}


def _population(
    stream: "Eventstream",
    diff: Any,
    *,
    assignment,
    horizon,
    path_col: str | None,
) -> _Population:
    """Resolve the arms and the analysed window exactly as ``get_ab_test`` does."""
    path_col = check_path_col(stream.schema, path_col)
    table = prepare_population(
        stream, diff, assignment, path_col=path_col, horizon=horizon
    )
    groups = resolve_diff(
        stream, diff, path_col, require_constant=True, assignment=table
    )
    window = analysed_window(stream, groups, table, path_col, horizon=horizon)
    active = pd.unique(window.work.df[path_col])
    has_events = window.path_ids.isin(active)
    return _Population(
        window=window,
        path_col=path_col,
        unassigned_paths=int(groups.unassigned_paths),
        with_assignment=table is not None,
        has_events=np.asarray(has_events, dtype=bool),
        label1=str(groups.label1),
        label2=str(groups.label2),
        diff_meta=_diff_meta(diff, groups),
    )


# ======================================================================= comparison
@dataclass
class _Quantity:
    """Per-path numerators of every row, and their denominators.

    ``num`` is ``(n_paths, n_rows)``; for a ratio, ``den`` is ``(n_paths, n_den)``
    and row ``r`` divides by column ``den_index[r]`` (a transition probability
    shares one denominator per event, the share of total one for all rows).
    """

    kind: str
    num: sparse.csr_matrix
    den: sparse.csr_matrix | None = None
    den_index: np.ndarray | None = None


def _arm_sums(matrix: sparse.csr_matrix, arm: np.ndarray, g: int) -> np.ndarray:
    return np.asarray(matrix[arm == g].sum(axis=0)).ravel().astype(float)


def _arm_nonzero(matrix: sparse.csr_matrix, arm: np.ndarray, g: int) -> np.ndarray:
    part = matrix[arm == g]
    return np.asarray((part != 0).sum(axis=0)).ravel().astype(np.int64)


def _compare(
    quantity: _Quantity,
    arm: np.ndarray,
    *,
    test: str,
    ci_level: float,
    power: float,
    correction: str,
    min_support: int,
    n_bootstrap: int,
    bootstrap_weights: str,
    random_state: int,
) -> tuple[pd.DataFrame, dict]:
    """Estimates, intervals, tests, correction and verdicts of every row.

    Returns the statistic columns and a dict of per-row helpers for the readings
    (support counts, the ``constant`` flag) plus ``n_tested``.
    """
    kind = quantity.kind
    num = quantity.num
    n_rows = num.shape[1]
    n1, n2 = int((arm == 0).sum()), int((arm == 1).sum())
    alpha = round(1.0 - ci_level, 12)
    nan = np.full(n_rows, np.nan)

    # supporting paths per arm: a transition (cell) at all, i.e. a non-zero numerator
    s1, s2 = _arm_nonzero(num, arm, 0), _arm_nonzero(num, arm, 1)
    sum1, sum2 = _arm_sums(num, arm, 0), _arm_sums(num, arm, 1)
    if kind == "ratio":
        # per denominator column, then gathered per row (never den[:, den_index])
        den, index = sparse.csr_matrix(quantity.den), quantity.den_index
        d1, d2 = _arm_sums(den, arm, 0)[index], _arm_sums(den, arm, 1)[index]
        paths1 = _arm_nonzero(den, arm, 0)[index]
        paths2 = _arm_nonzero(den, arm, 1)[index]
        with np.errstate(invalid="ignore", divide="ignore"):
            g1 = np.where(d1 > 0, sum1 / d1, np.nan)
            g2 = np.where(d2 > 0, sum2 / d2, np.nan)
    else:
        paths1, paths2 = np.full(n_rows, n1), np.full(n_rows, n2)
        g1 = sum1 / n1 if n1 else nan.copy()
        g2 = sum2 / n2 if n2 else nan.copy()
    rule = support_rule(kind, test, s1, s2, n1, n2, min_support)

    level = ci_level
    constant = np.zeros(n_rows, dtype=bool)
    invalid = np.zeros(n_rows, dtype=bool)
    valid = None
    if test == "z_test":
        k1, k2 = sum1, sum2
        _, p, se = _stats.two_proportion_z(k1, n1, k2, n2)
        dl, dh = _stats.newcombe_ci(k1, n1, k2, n2, level)
        pooled = (k1 + k2) / (n1 + n2)
        constant = (pooled == 0) | (pooled == 1)

        def ci_at(lv):
            return _stats.newcombe_ci(k1, n1, k2, n2, lv)

        def mde_at(a):
            return _stats.mde_proportion(pooled, n1, n2, a, power)

    elif test == "welch_t_test":
        sq = num.multiply(num).tocsr()
        q1, q2 = _arm_sums(sq, arm, 0), _arm_sums(sq, arm, 1)
        with np.errstate(invalid="ignore", divide="ignore"):
            v1 = np.where(n1 > 1, (q1 - n1 * g1**2) / (n1 - 1), np.nan)
            v2 = np.where(n2 > 1, (q2 - n2 * g2**2) / (n2 - 1), np.nan)
        v1, v2 = np.clip(v1, 0.0, None), np.clip(v2, 0.0, None)
        constant = (n1 > 1) & (n2 > 1) & (v1 == 0) & (v2 == 0)
        _, df, p, se = _stats.welch_t(g1, v1, n1, g2, v2, n2)
        se, df = np.asarray(se, dtype=float), np.asarray(df, dtype=float)

        def ci_at(lv):
            q = stats.t.ppf(0.5 + lv / 2, df)
            ok = np.isfinite(se) & np.isfinite(df)
            d = g1 - g2
            return np.where(ok, d - q * se, np.nan), np.where(ok, d + q * se, np.nan)

        dl, dh = ci_at(level)

        def mde_at(a):
            return _stats.mde_from_se(se, a, power)

    else:
        r1, r2, valid = _replicates(
            quantity, arm, n_bootstrap, bootstrap_weights, random_state
        )
        d_reps = r1 - r2
        invalid = (n_bootstrap - valid) > MAX_INVALID_REPLICATES * n_bootstrap
        se = bs.bootstrap_se(d_reps)
        p = bs.bootstrap_p(g1 - g2, d_reps)
        dl, dh = bs.percentile_ci(d_reps, level)
        constant = se == 0

        def ci_at(lv):
            low, high = bs.percentile_ci(d_reps, lv)
            gone = constant | invalid
            return np.where(gone, np.nan, low), np.where(gone, np.nan, high)

        def mde_at(a):
            return np.where(invalid, np.nan, _stats.mde_from_se(se, a, power))

        dl, dh = ci_at(level)
        p = np.where(invalid, np.nan, p)
        se = np.where(invalid, np.nan, se)

    se = np.where(constant, np.nan, np.asarray(se, dtype=float))
    warning, no_estimate = settle_support_warning(rule.warning, g1, g2, invalid)
    support = rule.support & ~no_estimate
    p = np.where(support, np.asarray(p, dtype=float), np.nan)
    tested = np.isfinite(p)
    n_tested = int(tested.sum())

    p_adjusted = np.full(n_rows, np.nan)
    if n_tested:
        p_adjusted[tested] = adjust_p(p[tested], correction)
    mde_alpha = mde_alpha_of(alpha, n_tested, correction)
    delta = g1 - g2
    with np.errstate(invalid="ignore", divide="ignore"):
        rel = np.where(g2 > 0, g1 / g2 - 1, np.nan)
        mde = np.asarray(mde_at(mde_alpha), dtype=float)
        rel_mde = np.where(g2 > 0, mde / g2, np.nan)
    out = {
        "group1_support": paths1.astype(int),
        "group2_support": paths2.astype(int),
        "group1_value": g1,
        "group2_value": g2,
        "delta": delta,
        "delta_se": se,
        "delta_ci_low": np.asarray(dl, dtype=float),
        "delta_ci_high": np.asarray(dh, dtype=float),
    }
    if correction == "bonferroni":
        if test == "bootstrap":
            # a percentile at 1 - alpha / m needs far more than n_bootstrap
            # replicates for a screen's m (it is the sample's min or max): the
            # simultaneous interval is normal on the bootstrap SE, the basis of
            # the p-value, so it excludes 0 exactly when p_value_adjusted < alpha
            z = stats.norm.ppf(1 - mde_alpha / 2)
            low_adj, high_adj = delta - z * se, delta + z * se
        else:
            low_adj, high_adj = ci_at(1 - mde_alpha)
        out["delta_ci_low_adj"] = np.asarray(low_adj, dtype=float)
        out["delta_ci_high_adj"] = np.asarray(high_adj, dtype=float)
    out.update(
        rel_delta=rel,
        p_value=p,
        p_value_adjusted=p_adjusted,
        mde_alpha=mde_alpha,
        mde=mde,
        rel_mde=rel_mde,
        verdict=verdicts(p_adjusted, delta, alpha, support=support),
        test=test,
        ci_basis=_CI_BASIS[test],
        p_basis=_P_BASIS[test],
        valid_replicates=(
            pd.array(valid, dtype="Int64")
            if valid is not None
            else pd.array([pd.NA] * n_rows, dtype="Int64")
        ),
        support_warning=warning.astype(str),
    )
    frame = pd.DataFrame(out)
    helpers = {
        "s1": s1,
        "s2": s2,
        "support_count": rule.count,
        "non_support_count": rule.non_support,
        "constant": constant,
        "n_tested": n_tested,
        "alpha": alpha,
    }
    return frame, helpers


def _replicates(quantity, arm, n_bootstrap, weights, random_state):
    """Joint path-bootstrap replicates of every row's arm value.

    One call of :func:`~retentioneering.tools._bootstrap.bootstrap_paths`: a
    ratio divides each edge's resampled count by its shared denominator column
    (``den_index``: transitions out of one event are resampled once for every
    edge from it), so a row's replicates equal those of ``bootstrap_paths`` on
    that edge's numerator and denominator alone; other quantities are per-path
    means (``den=None``).
    """
    num = sparse.csr_matrix(quantity.num)
    if quantity.kind == "ratio":
        result = bs.bootstrap_paths(
            num,
            sparse.csr_matrix(quantity.den),
            arm,
            n_bootstrap,
            weights,
            random_state=random_state,
            den_index=np.asarray(quantity.den_index, dtype=np.int64),
        )
    else:
        result = bs.bootstrap_paths(
            num, None, arm, n_bootstrap, weights, random_state=random_state
        )
    return result.group1, result.group2, result.valid


# ======================================================================= tables
def _validate_common(
    *,
    correction,
    ci_level,
    power,
    min_support,
) -> None:
    check_choice("correction", correction, CORRECTIONS)
    check_levels(ci_level, power)
    check_min_support(min_support)


def _population_columns(frame: pd.DataFrame, pop: _Population, horizon) -> None:
    arm = pop.arm
    frame["population"] = "assignment" if pop.with_assignment else "stream"
    frame["group1_paths"] = int((arm == 0).sum())
    frame["group2_paths"] = int((arm == 1).sum())
    if pop.with_assignment:
        silent = ~pop.has_events
        frame["group1_silent_paths"] = int((silent & (arm == 0)).sum())
        frame["group2_silent_paths"] = int((silent & (arm == 1)).sum())
    frame["unassigned_paths"] = pop.unassigned_paths
    if horizon is not None:
        frame["immature_paths"] = pop.window.horizon_immature


def _readings(
    frame: pd.DataFrame,
    helpers: dict,
    *,
    kind: str,
    in_pp: bool,
    correction: str,
    min_support: int,
    horizon,
    what: str,
    conditional_text,
) -> list[str]:
    alpha = helpers["alpha"]
    window = window_text(horizon)
    non_support = helpers["non_support_count"]
    texts = []
    for pos, row in enumerate(frame.to_dict("records")):
        texts.append(
            row_reading(
                row,
                kind=kind,
                correction=correction,
                alpha=alpha,
                min_support=min_support,
                support_count=int(helpers["support_count"][pos]),
                non_support_count=(
                    None if non_support is None else int(non_support[pos])
                ),
                window=window,
                constant=bool(helpers["constant"][pos]),
                in_pp=in_pp,
                conditional_text=(
                    conditional_text(row) if conditional_text is not None else None
                ),
                screen=(what, helpers["n_tested"]),
            )
        )
    return texts


def _order(frame: pd.DataFrame) -> pd.DataFrame:
    """Adjusted p-value ascending (untested rows last), then |delta| descending."""
    p = frame["p_value_adjusted"].to_numpy(dtype=float)
    d = np.abs(frame["delta"].to_numpy(dtype=float))
    order = np.lexsort(
        (
            np.arange(len(frame)),
            np.where(np.isfinite(d), -d, np.inf),
            np.where(np.isfinite(p), p, np.inf),
        )
    )
    return frame.iloc[order].reset_index(drop=True)


def _attrs(
    pop: _Population,
    *,
    tool: str,
    correction: str,
    ci_level: float,
    n_tested: int,
    n_rows: int,
    horizon,
    extra: dict,
) -> dict:
    out = {
        "tool": tool,
        "n_tested": n_tested,
        "n_rows": n_rows,
        "correction": correction,
        "ci_level": float(ci_level),
        "alpha": round(1.0 - float(ci_level), 12),
        "group1_label": pop.label1,
        "group2_label": pop.label2,
        "population": "assignment" if pop.with_assignment else "stream",
        "path_col": pop.path_col,
        "horizon": None if horizon is None else str(horizon),
        **pop.diff_meta,
    }
    out.update(extra)
    return out


def _check_edge_weight(edge_weight: Any) -> str:
    if isinstance(edge_weight, str) and edge_weight in _REFUSED_WEIGHTS:
        raise InvalidParameterError(
            "edge_weight",
            edge_weight,
            list(EDGE_WEIGHTS),
            reason=_REFUSED_WEIGHTS[edge_weight],
        )
    check_choice("edge_weight", edge_weight, EDGE_WEIGHTS)
    return edge_weight


def _resolve_edge_test(kind: str, test: Any, edge_weight: str) -> str:
    check_choice("test", test, TESTS)

    def error(message: str) -> InvalidParameterError:
        return InvalidParameterError("test", test, list(TESTS), reason=message)

    return resolve_test(kind, test, f"edge_weight {edge_weight!r}", error)


def _check_edges(edges: Any, available: set) -> list[tuple[str, str]]:
    allowed = ["a non-empty list of (source, target) event pairs"]
    if isinstance(edges, (str, bytes, dict)) or not hasattr(edges, "__iter__"):
        raise InvalidParameterError("edges", edges, allowed)
    pairs = []
    for edge in edges:
        if (
            isinstance(edge, (str, bytes))
            or not hasattr(edge, "__len__")
            or len(edge) != 2
        ):
            raise InvalidParameterError("edges", edge, allowed)
        source, target = edge
        for event in (source, target):
            if event not in available:
                raise InvalidParameterError("edges", event, sorted(available, key=str))
        pairs.append((source, target))
    if not pairs:
        raise InvalidParameterError("edges", edges, allowed)
    return list(dict.fromkeys(pairs))


def _node_order(events) -> list:
    start, end = EventTypes().PATH_START.name, EventTypes().PATH_END.name
    middle = sorted({e for e in events if e not in (start, end)}, key=str)
    return [start] + middle + [end]


def ab_transitions(
    stream: "Eventstream",
    diff: Any,
    edge_weight: str = "unique_paths",
    *,
    assignment=None,
    horizon=None,
    path_col: str | None = None,
    edges=None,
    test: str = "auto",
    correction: str = "bh",
    ci_level: float = 0.95,
    power: float = 0.8,
    n_bootstrap: int = 1000,
    bootstrap_weights: str = "multinomial",
    min_support: int = 10,
    random_state: int = 42,
) -> pd.DataFrame:
    """The table of ``Eventstream.get_ab_transitions`` (see its docstring)."""
    edge_weight = _check_edge_weight(edge_weight)
    kind = _KIND[edge_weight]
    test = _resolve_edge_test(kind, test, edge_weight)
    _validate_common(
        correction=correction, ci_level=ci_level, power=power, min_support=min_support
    )
    check_choice("bootstrap_weights", bootstrap_weights, BOOTSTRAP_WEIGHTS)
    check_n_bootstrap(n_bootstrap, allow_auto=False)
    check_random_state(random_state)
    declared = None
    if edges is not None:
        event_col = stream.schema.event_col
        available = set(pd.unique(stream.df[event_col]).tolist()) | {
            EventTypes().PATH_START.name,
            EventTypes().PATH_END.name,
        }
        declared = _check_edges(edges, available)

    pop = _population(
        stream, diff, assignment=assignment, horizon=horizon, path_col=path_col
    )
    path_col = pop.path_col
    counts = path_transition_counts(pop.window.work, path_col)
    nodes = _node_order(
        pd.unique(pd.concat([counts["source"], counts["target"]])).tolist()
        + ([e for pair in declared for e in pair] if declared else [])
    )
    node_pos = {node: i for i, node in enumerate(nodes)}
    observed = counts[["source", "target"]].drop_duplicates()
    edge_list = list(zip(observed["source"], observed["target"]))
    edge_list.sort(key=lambda e: (node_pos[e[0]], node_pos[e[1]]))
    edge_pos = {edge: i for i, edge in enumerate(edge_list)}
    n_paths, n_edges, n_nodes = len(pop.path_ids), len(edge_list), len(nodes)
    rows = pop.path_ids.get_indexer(counts[path_col])
    cols = np.array(
        [edge_pos[e] for e in zip(counts["source"], counts["target"])], dtype=np.int64
    )
    ok = rows >= 0
    full = sparse.csr_matrix(
        (counts["count"].to_numpy(dtype=float)[ok], (rows[ok], cols[ok])),
        shape=(n_paths, n_edges),
    )
    src = np.array([node_pos[s] for s, _ in edge_list], dtype=np.int64)
    tgt = np.array([node_pos[t] for _, t in edge_list], dtype=np.int64)
    if declared is not None:
        # the family is the declared list; an edge absent from every compared path
        # gets a row of zeros (nothing to compare: no_data)
        keep = np.array([edge_pos.get(e, -1) for e in declared], dtype=np.int64)
        present = keep >= 0
        select = sparse.csr_matrix(
            (np.ones(int(present.sum())), (keep[present], np.flatnonzero(present))),
            shape=(n_edges, len(declared)),
        )
        num = (full @ select).tocsr()
        row_edges = declared
        row_src = np.array([node_pos[s] for s, _ in declared], dtype=np.int64)
        row_tgt = np.array([node_pos[t] for _, t in declared], dtype=np.int64)
    else:
        num, row_edges, row_src, row_tgt = full, edge_list, src, tgt

    if edge_weight == "unique_paths":
        indicator = num.copy()
        indicator.data = (indicator.data > 0).astype(float)
        quantity = _Quantity(kind, indicator)
    elif edge_weight == "avg_per_path":
        quantity = _Quantity(kind, num)
    else:
        if edge_weight == "share_of_total":
            den = sparse.csr_matrix(np.asarray(full.sum(axis=1)))
            index = np.zeros(len(row_edges), dtype=np.int64)
        else:
            by = src if edge_weight == "proba_out" else tgt
            mapping = sparse.csr_matrix(
                (np.ones(n_edges), (np.arange(n_edges), by)), shape=(n_edges, n_nodes)
            )
            den = (full @ mapping).tocsr()
            index = row_src if edge_weight == "proba_out" else row_tgt
        quantity = _Quantity(kind, num, den, index)

    frame, helpers = _compare(
        quantity,
        pop.arm,
        test=test,
        ci_level=ci_level,
        power=power,
        correction=correction,
        min_support=min_support,
        n_bootstrap=n_bootstrap,
        bootstrap_weights=bootstrap_weights,
        random_state=random_state,
    )
    frame.insert(0, "source", [s for s, _ in row_edges])
    frame.insert(1, "target", [t for _, t in row_edges])
    frame.insert(2, "edge_weight", edge_weight)
    frame.insert(3, "group1_edge_paths", helpers["s1"].astype(int))
    frame.insert(4, "group2_edge_paths", helpers["s2"].astype(int))
    _population_columns(frame, pop, horizon)
    frame["conditional"] = kind == "ratio"
    frame["correction_family"] = (
        "none"
        if correction == "none"
        else f"edges={'declared' if declared is not None else 'all'}/{edge_weight}"
    )
    frame["reading"] = _readings(
        frame,
        helpers,
        kind=kind,
        in_pp=kind != "count",
        correction=correction,
        min_support=min_support,
        horizon=None if horizon is None else pop.window.horizon_length,
        what="edges",
        conditional_text=(
            (lambda row: _conditional_text(edge_weight, row))
            if kind == "ratio"
            else None
        ),
    )
    present = [c for c in TRANSITION_COLUMNS if c in frame.columns]
    out = _order(frame[present])
    out.attrs = _attrs(
        pop,
        tool="get_ab_transitions",
        correction=correction,
        ci_level=ci_level,
        n_tested=helpers["n_tested"],
        n_rows=len(out),
        horizon=horizon,
        extra={
            "edge_weight": edge_weight,
            "edges": "declared" if declared is not None else "all",
        },
    )
    return out


def _conditional_text(edge_weight: str, row: dict) -> str:
    """The ``conditional`` clause of a transition probability or share of total."""
    paths = (
        f"{row['group1_support']} of {row['group1_paths']} and "
        f"{row['group2_support']} of {row['group2_paths']} paths have one"
    )
    if edge_weight == "proba_out":
        return (
            "conditional on post-treatment behaviour: a share of the transitions out "
            f"of '{row['source']}' ({paths}), so it also moves when the treatment "
            f"changes who reaches '{row['source']}'; compares those transitions, not "
            "the arms"
        )
    if edge_weight == "proba_in":
        return (
            "conditional on post-treatment behaviour: a share of the transitions into "
            f"'{row['target']}' ({paths}), so it also moves when the treatment "
            f"changes how '{row['target']}' is reached; compares those transitions, "
            "not the arms"
        )
    return (
        "conditional on post-treatment behaviour: a share of all transitions, so it "
        "also moves when the treatment changes any other transition (composition); "
        "compares those transitions, not the arms"
    )


def ab_steps(
    stream: "Eventstream",
    diff: Any,
    max_steps: int = 10,
    *,
    assignment=None,
    horizon=None,
    path_col: str | None = None,
    correction: str = "bh",
    ci_level: float = 0.95,
    power: float = 0.8,
    min_support: int = 10,
) -> pd.DataFrame:
    """The table of ``Eventstream.get_ab_steps`` (see its docstring)."""
    if (
        isinstance(max_steps, bool)
        or not isinstance(max_steps, (int, np.integer))
        or max_steps < 1
    ):
        raise InvalidParameterError("max_steps", max_steps, ["an int >= 1"])
    _validate_common(
        correction=correction, ci_level=ci_level, power=power, min_support=min_support
    )
    pop = _population(
        stream, diff, assignment=assignment, horizon=horizon, path_col=path_col
    )
    path_col = pop.path_col
    schema = stream.schema
    path_end = EventTypes().PATH_END.name
    work = pop.window.work.df[
        [path_col, schema.event_col, schema.index, schema.subindex]
    ]
    work = work.sort_values([schema.index, schema.subindex], kind="stable")
    step = work.groupby(path_col, sort=False, observed=True).cumcount().to_numpy() + 1
    rows = pop.path_ids.get_indexer(work[path_col])
    lengths = np.zeros(len(pop.path_ids), dtype=np.int64)
    np.add.at(lengths, rows[rows >= 0], 1)
    within = (step <= max_steps) & (rows >= 0)
    events = work[schema.event_col].to_numpy()[within]
    steps = step[within]
    cell_list = sorted(set(zip(events, steps)), key=lambda c: (c[1], str(c[0])))
    cell_pos = {cell: i for i, cell in enumerate(cell_list)}
    cols = np.array([cell_pos[c] for c in zip(events, steps)], dtype=np.int64)
    n_paths = len(pop.path_ids)
    indicator = sparse.csr_matrix(
        (np.ones(len(cols)), (rows[within], cols)), shape=(n_paths, len(cell_list))
    )
    # ENDED: a path with fewer than k events is past its end at step k (the
    # path_end row of step_matrix_data); a silent assigned path is ended at step 1
    ended_rows, ended_cols = [], []
    for k in range(1, max_steps + 1):
        hit = np.flatnonzero(lengths < k)
        ended_rows.append(hit)
        ended_cols.append(np.full(len(hit), k - 1))
    ended = sparse.csr_matrix(
        (
            np.ones(sum(len(r) for r in ended_rows)),
            (np.concatenate(ended_rows), np.concatenate(ended_cols)),
        ),
        shape=(n_paths, max_steps),
    )
    used = np.asarray(ended.sum(axis=0)).ravel() > 0
    num = sparse.hstack([indicator, ended[:, np.flatnonzero(used)]]).tocsr()
    cell_list = cell_list + [(path_end, k + 1) for k in np.flatnonzero(used).tolist()]
    num.data = (num.data > 0).astype(float)

    frame, helpers = _compare(
        _Quantity("binary", num),
        pop.arm,
        test="z_test",
        ci_level=ci_level,
        power=power,
        correction=correction,
        min_support=min_support,
        n_bootstrap=bs.MIN_REPLICATES,
        bootstrap_weights="multinomial",
        random_state=0,
    )
    frame.insert(0, "event", [e for e, _ in cell_list])
    frame.insert(1, "step", np.array([k for _, k in cell_list], dtype=int))
    _population_columns(frame, pop, horizon)
    frame["conditional"] = False
    frame["correction_family"] = (
        "none" if correction == "none" else f"cells/max_steps={int(max_steps)}"
    )
    frame["reading"] = _readings(
        frame,
        helpers,
        kind="binary",
        in_pp=True,
        correction=correction,
        min_support=min_support,
        horizon=None if horizon is None else pop.window.horizon_length,
        what="cells",
        conditional_text=None,
    )
    present = [c for c in STEP_COLUMNS if c in frame.columns]
    out = _order(frame[present])
    out.attrs = _attrs(
        pop,
        tool="get_ab_steps",
        correction=correction,
        ci_level=ci_level,
        n_tested=helpers["n_tested"],
        n_rows=len(out),
        horizon=horizon,
        extra={"max_steps": int(max_steps)},
    )
    return out


# ======================================================================= graph payload
#: Columns ``transition_graph(ab_transitions=...)`` reads from the frame.
PAYLOAD_COLUMNS = (
    "source",
    "target",
    "edge_weight",
    "group1_value",
    "group2_value",
    "delta",
    "delta_ci_low",
    "delta_ci_high",
    "p_value_adjusted",
    "verdict",
)


def _json_float(value: Any) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if np.isfinite(value) else None


def transitions_payload(frame: Any, diff: Any, edge_weight: str | None) -> dict:
    """Validate a ``get_ab_transitions`` frame against a graph's ``diff`` and
    ``edge_weight`` and return the JSON-ready payload the graph draws from.

    Raises
    ------
    InvalidParameterError
        Not a ``get_ab_transitions`` frame (missing columns or ``attrs``), more than
        one ``edge_weight``, a graph ``edge_weight`` other than the frame's, a graph
        without ``diff``, or a ``diff`` other than the frame's (other arms, swapped
        arms, another segment column).
    """
    param = "ab_transitions"
    if not isinstance(frame, pd.DataFrame):
        raise InvalidParameterError(
            param, type(frame).__name__, ["a DataFrame returned by get_ab_transitions"]
        )
    missing = [c for c in PAYLOAD_COLUMNS if c not in frame.columns]
    if missing:
        raise InvalidParameterError(
            param,
            f"a DataFrame without {missing}",
            ["a DataFrame returned by get_ab_transitions (rows may be filtered)"],
        )
    meta = frame.attrs or {}
    if meta.get("tool") != "get_ab_transitions":
        raise InvalidParameterError(
            param,
            "a DataFrame without the attrs of get_ab_transitions",
            ["a DataFrame returned by get_ab_transitions (rows may be filtered)"],
            reason=(
                "the frame's attrs record the diff and the correction it was "
                "computed with; they are lost by pd.concat or a copy through other "
                "libraries"
            ),
        )
    weights = pd.unique(frame["edge_weight"]).tolist() or [meta.get("edge_weight")]
    if len(weights) != 1 or weights[0] != meta.get("edge_weight"):
        raise InvalidParameterError(
            param,
            f"edge weights {weights}",
            ["the rows of one get_ab_transitions call"],
        )
    frame_weight = weights[0]
    if edge_weight is not None and edge_weight != frame_weight:
        raise InvalidParameterError(
            "edge_weight",
            edge_weight,
            [frame_weight],
            reason=(
                f"ab_transitions tests edge_weight='{frame_weight}'; the graph must "
                "draw the same weight"
            ),
        )
    if diff is None:
        raise InvalidParameterError(
            "diff",
            None,
            ["the diff the frame was computed with"],
            reason="ab_transitions highlights edges of a diff graph",
        )
    if meta.get("diff") is not None:
        expected = meta["diff"]
        given = (
            [str(diff[0]), str(diff[1]), str(diff[2])]
            if _is_segment_diff(diff)
            else None
        )
        same_column = expected[0] == "arm" or given is None or given[0] == expected[0]
        if given is None or not same_column or given[1:] != expected[1:]:
            raise InvalidParameterError(
                "diff",
                diff,
                [
                    tuple(expected)
                    if expected[0] != "arm"
                    else ("<segment>",) + tuple(expected[1:])
                ],
                reason=(
                    "ab_transitions was computed for group1="
                    f"'{expected[1]}', group2='{expected[2]}'; the graph's groups "
                    "must be the same, in the same order (the delta's sign depends "
                    "on it)"
                ),
            )
    else:
        given = _diff_meta(diff, None) if not _is_segment_diff(diff) else None
        if given is None or given["diff_ids"] != meta.get("diff_ids"):
            raise InvalidParameterError(
                "diff",
                "a diff other than the frame's id lists",
                ["the (ids1, ids2) the frame was computed with"],
            )
    edges = []
    for row in frame.to_dict("records"):
        edges.append(
            {
                "source": str(row["source"]),
                "target": str(row["target"]),
                "group1_value": _json_float(row["group1_value"]),
                "group2_value": _json_float(row["group2_value"]),
                "delta": _json_float(row["delta"]),
                "ci_low": _json_float(row["delta_ci_low"]),
                "ci_high": _json_float(row["delta_ci_high"]),
                "p_adjusted": _json_float(row["p_value_adjusted"]),
                "verdict": str(row["verdict"]),
                "support_warning": str(row.get("support_warning", "") or ""),
                "conditional": bool(row.get("conditional", False)),
            }
        )
    return {
        "edge_weight": frame_weight,
        "correction": meta.get("correction"),
        "alpha": meta.get("alpha"),
        "ci_level": meta.get("ci_level"),
        "n_tested": meta.get("n_tested"),
        "population": meta.get("population"),
        "path_col": meta.get("path_col"),
        "group1_label": meta.get("group1_label"),
        "group2_label": meta.get("group2_label"),
        "edges": edges,
    }
