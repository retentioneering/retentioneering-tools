"""Arm-blind vocabulary compression for the A/B screens: ``get_ab_compression``.

A screen of many events splits the paths over many edges (cells), and most of them
are too thin to test. ``get_ab_compression`` proposes a smaller vocabulary for one
screen: it drops rare events that only interrupt popular transitions, relabels
other rare events to one ``other`` event, merges chains of events that almost always
follow each other and merges synonyms (events with the same neighbours). The result
is a **plan**, one row per event, applied with ``Eventstream.compress_events``.

Design (ADR-0017, amendment "Data sufficiency and vocabulary compression")
--------------------------------------------------------------------------
* **Arm-blind.** Every statistic that drives the plan is computed on the pooled
  paths of both arms; the arm of a path is never read. The only numbers taken from
  the design are the two arm sizes, which a permutation of the labels does not
  change (within a ``by`` segment, the segment's paths split in the design's
  allocation ratio). Choosing a vocabulary from the arms' differences would be a
  selection on the outcome; a filter on pooled support is independent of the test
  statistic under the null ("independent filtering", Bourgon, Gentleman and Huber,
  PNAS 2010), so Benjamini-Hochberg over the rows that remain keeps its guarantee.
* **A greedy path.** The plan is built one operation at a time on the current
  compressed paths, in an order that depends on the screen; after every operation
  the screen's numbers (cells, testable cells, median MDE) and the path metrics
  (transition mass retained, artificial transitions) are recorded in
  ``attrs["path"]``. ``target`` picks the step; ``step`` picks another one.
* **Pool thin cells, keep measured events apart.** A synonym merge needs a thin
  member (half or more of the paths through its cells are in untestable cells)
  and must leave fewer untestable cells without losing a testable one; a rare
  event with such a synonym is merged into it before it is dropped or relabelled.
  Two well-measured events with the same neighbours stay apart: merging them
  would hide a treatment that moves the paths from one to the other.
* **The default step keeps what is testable.** ``target="testable"`` takes the
  step with the most testable cells (then the fewest untestable ones), so a
  screen that is already testable keeps its vocabulary: merges and drops remove
  cells, and only pooling thin cells adds testable ones.
* **Through the processors.** A plan is a mapping ``event -> label | None``
  applied by ``compress_events``: ``edit_events`` deletes and renames, then
  ``collapse_events(loops=...)`` collapses each run of a merged label into one
  event. The numbers of the path are computed on the same composition, so the
  compressed stream's screen reproduces them.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _stats
from retentioneering.tools._diff import warn_user
from retentioneering.tools.ab_test import (
    CORRECTIONS,
    _provenance,
    check_choice,
    check_levels,
    check_min_support,
    mde_alpha_of,
)

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream

__all__ = [
    "SCREENS",
    "TARGETS",
    "ACTIONS",
    "OPERATIONS",
    "PLAN_COLUMNS",
    "PATH_COLUMNS",
    "ab_compression",
    "plan_mapping",
]

#: The screens a plan is built for.
SCREENS = ("transitions", "steps")
#: Named targets (a positive number is a median-MDE goal).
TARGETS = ("testable", "mde")
#: ``action`` of a plan row.
ACTIONS = ("keep", "merge", "drop", "other")
#: ``operation`` of a path row (``start`` is step 0, the stream as it is).
OPERATIONS = ("start", "merge_chain", "merge_synonym", "drop", "other")
#: Edge weights the transitions plan sizes its cells by.
_EDGE_WEIGHTS = (
    "unique_paths",
    "avg_per_path",
    "proba_out",
    "proba_in",
    "share_of_total",
)
#: A synonym's own self-loops above this share of its transitions block a merge:
#: collapsing the merged runs would erase them.
SELF_LOOP_MAX = 0.05
#: Default thresholds.
DEFAULT_SYNONYM_THRESHOLD = 0.1
DEFAULT_BRIDGE_THRESHOLD = 0.9

#: Columns of the plan (``by`` adds the segment column first).
PLAN_COLUMNS: tuple[str, ...] = (
    "event",
    "action",
    "label",
    "step",
    "operation",
    "path_share",
    "occurrences",
    "p_next",
    "p_prev",
    "js_distance",
    "self_loop_share",
    "bridge_fidelity",
    "reason",
)
#: Columns of ``attrs["path"]`` (``by`` adds the segment column first).
PATH_COLUMNS: tuple[str, ...] = (
    "step",
    "operation",
    "events",
    "label",
    "vocabulary",
    "cells",
    "testable",
    "untestable",
    "median_mde",
    "transitions",
    "mass_retained",
    "artificial_transitions",
    "artificial_mass",
    "reason",
)


# ======================================================================= data
@dataclass
class _Data:
    """The analysed events of one population, as integer codes grouped by path."""

    codes: np.ndarray  # original event code of every analysed event, path by path
    pid: np.ndarray  # path row (0 .. n_paths - 1) of every event
    n_paths: int
    n1: float
    n2: float
    events: list  # code -> event name
    _pairs: np.ndarray | None = field(default=None, repr=False)

    @property
    def original_pairs(self) -> np.ndarray:
        """The distinct direct transitions of the original paths (``source *
        (events + 2) + target``, with the synthetic start and end), computed once."""
        if self._pairs is None:
            n_orig = len(self.events)
            src, dst, _ = _transitions(self.codes, self.pid, n_orig)
            self._pairs = np.unique(src * (n_orig + 2) + dst)
        return self._pairs

    def subset(self, rows: np.ndarray, n1: float, n2: float) -> "_Data":
        """The events of the paths in ``rows`` (a boolean mask over path rows),
        renumbered."""
        new_pos = np.cumsum(rows) - 1
        keep = rows[self.pid]
        return _Data(
            codes=self.codes[keep],
            pid=new_pos[self.pid[keep]],
            n_paths=int(rows.sum()),
            n1=n1,
            n2=n2,
            events=self.events,
        )


@dataclass
class _State:
    """The vocabulary after some operations: a label (or -1: dropped) per event."""

    label_of: np.ndarray  # original code -> label id, -1 dropped
    names: list  # label id -> name
    kinds: list  # label id -> "event" | "merged" | "other"

    def copy(self) -> "_State":
        return _State(self.label_of.copy(), list(self.names), list(self.kinds))

    @property
    def collapsible(self) -> np.ndarray:
        return np.array([k == "merged" for k in self.kinds], dtype=bool)

    def members(self, label: int) -> list[int]:
        return np.flatnonzero(self.label_of == label).tolist()


def _positions(pid: np.ndarray) -> np.ndarray:
    """0-based position of every event within its path."""
    n = len(pid)
    if not n:
        return np.zeros(0, dtype=np.int64)
    starts = np.r_[True, pid[1:] != pid[:-1]]
    idx = np.arange(n)
    first = np.maximum.accumulate(np.where(starts, idx, 0))
    return idx - first


def _compress(
    data: _Data, state: _State, limit: int | None = None
) -> tuple[np.ndarray, np.ndarray]:
    """The compressed paths: labels and path rows (drop, rename, collapse merged
    runs, then the first ``limit`` events of each path)."""
    lab = state.label_of[data.codes]
    keep = lab >= 0
    lab, pid = lab[keep], data.pid[keep]
    collapsible = state.collapsible
    if collapsible.any() and len(lab):
        same = np.r_[False, (lab[1:] == lab[:-1]) & (pid[1:] == pid[:-1])]
        repeat = same & collapsible[lab]
        lab, pid = lab[~repeat], pid[~repeat]
    if limit is not None and len(lab):
        within = _positions(pid) < limit
        lab, pid = lab[within], pid[within]
    return lab, pid


def _transitions(
    lab: np.ndarray, pid: np.ndarray, n_labels: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Every transition of the paths with the synthetic start and end: ``(source,
    target, path row)``; ``n_labels`` is ``path_start``, ``n_labels + 1``
    ``path_end`` (a path without events has none, as in the screens)."""
    start, end = n_labels, n_labels + 1
    n = len(lab)
    if not n:
        empty = np.zeros(0, dtype=np.int64)
        return empty, empty, empty
    first = np.r_[True, pid[1:] != pid[:-1]]
    last = np.r_[pid[1:] != pid[:-1], True]
    inner = ~last[:-1]
    src = np.concatenate([np.full(int(first.sum()), start), lab[:-1][inner], lab[last]])
    dst = np.concatenate([lab[first], lab[1:][inner], np.full(int(last.sum()), end)])
    path = np.concatenate([pid[first], pid[:-1][inner], pid[last]])
    return src.astype(np.int64), dst.astype(np.int64), path.astype(np.int64)


def _unique_pairs(path: np.ndarray, key: np.ndarray, width: int):
    """Unique ``(path, key)`` pairs with their multiplicity."""
    if not len(key):
        empty = np.zeros(0, dtype=np.int64)
        return empty, empty, empty
    combined = path.astype(np.int64) * width + key.astype(np.int64)
    uniq, counts = np.unique(combined, return_counts=True)
    return uniq // width, uniq % width, counts


# ======================================================================= screen numbers
@dataclass
class _Screen:
    """What the plan reads off a screen: its cells, their support and MDE."""

    name: str
    edge_weight: str
    max_steps: int
    min_support: int
    alpha: float
    power: float
    correction: str


def _rule(support, non_support, min_support):
    ok = (support >= min_support) & (support > 0)
    if non_support is not None:
        ok &= non_support >= min_support
    return ok


def _cell_numbers(
    data: _Data, lab: np.ndarray, pid: np.ndarray, n_labels: int, screen: _Screen
) -> dict:
    """Cells, testable cells and the MDE of each testable one, pooled over the arms.

    0/1 cells (``unique_paths`` edges, step cells) follow the getter exactly: the
    pooled support rule and ``mde_proportion`` at the pooled share and the arm
    sizes. Count and ratio edges use the pooled spread (a ratio linearised), the
    arm-blind counterpart of the getter's Welch and bootstrap standard errors.
    """
    n = data.n_paths
    n1, n2 = data.n1, data.n2
    non_support = None
    se = None
    if screen.name == "steps":
        pos = _positions(pid)
        width = screen.max_steps
        within = pos < width
        keys = lab[within].astype(np.int64) * width + pos[within]
        cell_keys, support = np.unique(keys, return_counts=True)
        lengths = np.bincount(pid, minlength=n) if len(pid) else np.zeros(n, int)
        ended = np.array(
            [int((lengths < k).sum()) for k in range(1, width + 1)], dtype=np.int64
        )
        support = np.concatenate([support, ended[ended > 0]]).astype(float)
        non_support = n - support
        cells = len(support)
    else:
        src, dst, path = _transitions(lab, pid, n_labels)
        width = n_labels + 2
        key = src * width + dst
        p_rows, keys, counts = _unique_pairs(path, key, width * width)
        cell_keys, inverse = np.unique(keys, return_inverse=True)
        support = np.bincount(inverse, minlength=len(cell_keys)).astype(float)
        cells = len(cell_keys)
        weight = screen.edge_weight
        if weight == "unique_paths":
            non_support = n - support
        elif weight == "avg_per_path":
            total = np.bincount(inverse, weights=counts, minlength=cells)
            squares = np.bincount(
                inverse, weights=counts.astype(float) ** 2, minlength=cells
            )
            mean = total / n
            var = np.clip((squares - n * mean**2) / max(n - 1, 1), 0, None)
            se = np.sqrt(var * (1 / n1 + 1 / n2))
        else:
            # a ratio of sums R = sum(c) / sum(d), linearised: z = (c - R d) / mean(d)
            cell_src, cell_dst = cell_keys // width, cell_keys % width
            if weight == "share_of_total":
                den_of_path = np.bincount(path, minlength=n).astype(float)
                d = den_of_path[p_rows]
                den_total = np.full(cells, den_of_path.sum())
                den_sq = np.full(cells, (den_of_path**2).sum())
            else:
                node = src if weight == "proba_out" else dst
                cell_node = cell_src if weight == "proba_out" else cell_dst
                pn_rows, pn_nodes, pn_counts = _unique_pairs(path, node, width)
                node_total = np.bincount(pn_nodes, weights=pn_counts, minlength=width)
                node_sq = np.bincount(
                    pn_nodes, weights=pn_counts.astype(float) ** 2, minlength=width
                )
                lookup = pd.Series(
                    pn_counts.astype(float), index=pn_rows * width + pn_nodes
                )
                row_node = (cell_src if weight == "proba_out" else cell_dst)[inverse]
                d = lookup.reindex(p_rows * width + row_node).to_numpy()
                den_total = node_total[cell_node]
                den_sq = node_sq[cell_node]
            c = counts.astype(float)
            num_total = np.bincount(inverse, weights=c, minlength=cells)
            cross = np.bincount(inverse, weights=c * d, minlength=cells)
            c_sq = np.bincount(inverse, weights=c**2, minlength=cells)
            with np.errstate(invalid="ignore", divide="ignore"):
                ratio = num_total / den_total
                ss = c_sq - 2 * ratio * cross + ratio**2 * den_sq
                mean_d = den_total / n
                var = np.clip(ss, 0, None) / mean_d**2 / max(n - 1, 1)
                se = np.sqrt(var * (1 / n1 + 1 / n2))
    testable = _rule(support, non_support, screen.min_support)
    if se is not None:
        testable &= np.isfinite(se) & (se > 0)
    # per label: the share of its support (paths through its cells) that sits in
    # cells that are not testable
    k = len(cell_keys)
    if screen.name == "steps":
        ends = [cell_keys // screen.max_steps]
    else:
        ends = [cell_keys // (n_labels + 2), cell_keys % (n_labels + 2)]
    total = np.zeros(n_labels)
    thin = np.zeros(n_labels)
    weight = support[:k]
    flag = ~testable[:k]
    for label in ends:
        own = label < n_labels
        total += np.bincount(label[own], weights=weight[own], minlength=n_labels)[
            :n_labels
        ]
        thin += np.bincount(
            label[own], weights=(weight * flag)[own], minlength=n_labels
        )[:n_labels]
    with np.errstate(invalid="ignore", divide="ignore"):
        thin_share = np.where(total > 0, thin / total, np.nan)
    m = int(testable.sum())
    level = mde_alpha_of(screen.alpha, m, screen.correction)
    if se is None:
        mde = _stats.mde_proportion(support / n, n1, n2, level, screen.power)
    else:
        mde = _stats.mde_from_se(se, level, screen.power)
    mde = np.asarray(mde, dtype=float)[testable]
    mde = mde[np.isfinite(mde)]
    return {
        "cells": int(cells),
        "testable": m,
        "untestable": int(cells - m),
        "_thin": thin_share,
        "median_mde": float(np.median(mde)) if mde.size else float("nan"),
    }


def _supported_pairs(data: _Data, state: _State) -> np.ndarray:
    """Every direct transition of the original paths, in the labels of ``state``
    (pairs with a dropped event left out): what a compressed edge may be without
    being artificial."""
    n_orig = len(data.events)
    pairs = data.original_pairs
    o_src, o_dst = pairs // (n_orig + 2), pairs % (n_orig + 2)
    n_labels = len(state.names)
    to_label = np.concatenate([state.label_of, [n_labels, n_labels + 1]])
    l_src, l_dst = to_label[o_src], to_label[o_dst]
    ok = (l_src >= 0) & (l_dst >= 0)
    return np.unique(l_src[ok] * (n_labels + 2) + l_dst[ok])


def _path_numbers(
    data: _Data, state: _State, screen: _Screen, original_transitions: int
) -> dict:
    """One row of ``attrs["path"]`` (without the operation columns), with the
    cells' ``_thin`` shares the next step reads."""
    n_labels = len(state.names)
    lab, pid = _compress(data, state)
    numbers = _cell_numbers(data, lab, pid, n_labels, screen)
    src, dst, _ = _transitions(lab, pid, n_labels)
    keys = src * (n_labels + 2) + dst
    artificial = ~np.isin(keys, _supported_pairs(data, state))
    numbers.update(
        vocabulary=int(len(np.unique(lab))) if len(lab) else 0,
        transitions=int(len(keys)),
        mass_retained=(
            float((len(keys) - artificial.sum()) / original_transitions)
            if original_transitions
            else float("nan")
        ),
        artificial_transitions=int(len(np.unique(keys[artificial]))),
        artificial_mass=int(artificial.sum()),
    )
    return numbers


# ======================================================================= candidates
@dataclass
class _Stats:
    """Pooled transition statistics of the current compressed paths."""

    count: np.ndarray  # (L+2, L+2) transitions source -> target
    paths: np.ndarray  # (L+2, L+2) paths with the transition
    occurrences: np.ndarray  # (L,) events with the label
    path_share: np.ndarray  # (L,) share of the population's paths with the label
    lab: np.ndarray
    pid: np.ndarray


def _stats_of(data: _Data, state: _State, limit: int | None) -> _Stats:
    n_labels = len(state.names)
    lab, pid = _compress(data, state, limit)
    src, dst, path = _transitions(lab, pid, n_labels)
    width = n_labels + 2
    count = np.bincount(src * width + dst, minlength=width * width).reshape(
        width, width
    )
    p_rows, keys, _ = _unique_pairs(path, src * width + dst, width * width)
    paths = np.bincount(keys, minlength=width * width).reshape(width, width)
    occurrences = np.bincount(lab, minlength=n_labels)[:n_labels]
    _, labs, _ = _unique_pairs(pid, lab, n_labels)
    share = np.bincount(labs, minlength=n_labels)[:n_labels] / max(data.n_paths, 1)
    return _Stats(count, paths, occurrences, share, lab, pid)


@dataclass
class _Op:
    """One operation of the path."""

    operation: str
    labels: tuple  # label ids it acts on
    evidence: dict = field(default_factory=dict)
    reason: str = ""


def _chain_candidates(
    st: _Stats, mergeable: np.ndarray, threshold: float, min_support: int
):
    n = len(mergeable)
    count = st.count[:n, :]
    out_total = st.count[:n, :].sum(axis=1)
    in_total = st.count[:, :n].sum(axis=0)
    found = []
    for u in np.flatnonzero(mergeable & (out_total > 0)):
        row = count[u, :n]
        for v in np.flatnonzero(row > 0):
            if v == u or not mergeable[v] or in_total[v] == 0:
                continue
            p_next = row[v] / out_total[u]
            p_prev = row[v] / in_total[v]
            if (
                p_next >= threshold
                and p_prev >= threshold
                and st.paths[u, v] >= min_support
            ):
                found.append((min(p_next, p_prev), int(u), int(v), p_next, p_prev))
    found.sort(key=lambda f: (-f[0], f[1], f[2]))
    return found


def _js(p: np.ndarray, q: np.ndarray) -> float:
    """Jensen-Shannon distance in bits (0 = identical, 1 = disjoint)."""
    p = p / p.sum()
    q = q / q.sum()
    m = 0.5 * (p + q)
    with np.errstate(invalid="ignore", divide="ignore"):
        kl_p = np.where(p > 0, p * np.log2(p / m), 0.0).sum()
        kl_q = np.where(q > 0, q * np.log2(q / m), 0.0).sum()
    return float(math.sqrt(max(0.5 * (kl_p + kl_q), 0.0)))


def _synonym_candidates(st: _Stats, mergeable: np.ndarray, threshold: float):
    n = len(mergeable)
    out_total = st.count[:n, :].sum(axis=1)
    in_total = st.count[:, :n].sum(axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        loops = np.where(out_total > 0, np.diag(st.count)[:n] / out_total, 0.0)
    ok = mergeable & (out_total > 0) & (in_total > 0) & (loops <= SELF_LOOP_MAX)
    found = []
    idx = np.flatnonzero(ok)
    for i, u in enumerate(idx):
        for v in idx[i + 1 :]:
            cols = np.ones(st.count.shape[1], dtype=bool)
            cols[[u, v]] = False
            out_u, out_v = st.count[u, cols], st.count[v, cols]
            in_u, in_v = st.count[cols, u], st.count[cols, v]
            if min(out_u.sum(), out_v.sum(), in_u.sum(), in_v.sum()) == 0:
                continue
            distance = max(_js(out_u, out_v), _js(in_u, in_v))
            if distance <= threshold:
                found.append(
                    (distance, int(u), int(v), float(loops[u]), float(loops[v]))
                )
    found.sort(key=lambda f: (f[0], f[1], f[2]))
    return found


def _runs(lab: np.ndarray, pid: np.ndarray, u: int, n_labels: int):
    """``(previous, next)`` label of every run of ``u`` (path_start / path_end at
    the ends)."""
    n = len(lab)
    hit = lab == u
    if not hit.any():
        return np.zeros(0, dtype=np.int64), np.zeros(0, dtype=np.int64)
    same_prev = np.r_[False, pid[1:] == pid[:-1]]
    same_next = np.r_[pid[1:] == pid[:-1], False]
    prev_hit = np.r_[False, hit[:-1]] & same_prev
    next_hit = np.r_[hit[1:], False] & same_next
    starts = np.flatnonzero(hit & ~prev_hit)
    ends = np.flatnonzero(hit & ~next_hit)
    prev = np.where(same_prev[starts], lab[np.maximum(starts - 1, 0)], n_labels)
    nxt = np.where(same_next[ends], lab[np.minimum(ends + 1, n - 1)], n_labels + 1)
    return prev.astype(np.int64), nxt.astype(np.int64)


def _gap_paths(
    lab: np.ndarray, pid: np.ndarray, n_labels: int, skip: int
) -> np.ndarray:
    """Paths with ``a -> y -> b`` (one event ``y != skip`` between ``a`` and ``b``),
    as an ``(L+2, L+2)`` matrix (path_start / path_end at the ends)."""
    width = n_labels + 2
    n = len(lab)
    if n == 0:
        return np.zeros((width, width), dtype=np.int64)
    first = np.r_[True, pid[1:] != pid[:-1]]
    last = np.r_[pid[1:] != pid[:-1], True]
    # every path extended with a start slot before and an end slot after it
    n_paths = int(first.sum())
    full_lab = np.empty(n + 2 * n_paths, dtype=np.int64)
    full_pid = np.empty(n + 2 * n_paths, dtype=np.int64)
    pos = np.arange(n) + np.cumsum(first.astype(np.int64)) * 2 - 1
    full_lab[pos] = lab
    full_pid[pos] = pid
    full_lab[pos[first] - 1] = n_labels
    full_pid[pos[first] - 1] = pid[first]
    full_lab[pos[last] + 1] = n_labels + 1
    full_pid[pos[last] + 1] = pid[last]
    a, y, b = full_lab[:-2], full_lab[1:-1], full_lab[2:]
    same = (full_pid[:-2] == full_pid[1:-1]) & (full_pid[1:-1] == full_pid[2:])
    ok = same & (y != skip) & (y < n_labels)
    keys = a[ok] * width + b[ok]
    _, uniq_keys, _ = _unique_pairs(full_pid[1:-1][ok], keys, width * width)
    return np.bincount(uniq_keys, minlength=width * width).reshape(width, width)


def _rare_decision(
    st: _Stats,
    u: int,
    n_labels: int,
    screen: _Screen,
    bridge_threshold: float,
) -> tuple[str, dict, str]:
    """``("drop" | "other", evidence, reason)`` for a rare label ``u``."""
    lab, pid = st.lab, st.pid
    prev, nxt = _runs(lab, pid, u, n_labels)
    runs = len(prev)
    # a path made of nothing but u would be emptied by a drop
    if len(lab):
        lengths = np.bincount(pid)
        own = np.bincount(pid[lab == u], minlength=len(lengths))
        emptied = int(((own == lengths) & (lengths > 0)).sum())
    else:
        emptied = 0
    if runs == 0:
        return "other", {"bridge_fidelity": float("nan")}, "it has no run"
    if screen.name == "transitions":
        bridged = st.paths[prev, nxt] >= screen.min_support
        what = (
            f"of its {runs} runs sit between two events that also follow each other "
            f"directly in at least {screen.min_support} paths"
        )
    else:
        gap = _gap_paths(lab, pid, n_labels, skip=u)
        direct = st.paths[prev, nxt]
        bridged = (direct > 0) & (direct >= gap[prev, nxt])
        what = (
            f"of its {runs} runs are insertions (the paths without it go from the "
            "event before to the event after directly at least as often as through "
            "one other event)"
        )
    fidelity = float(bridged.mean())
    evidence = {"bridge_fidelity": fidelity}
    share = f"{fidelity:.0%} {what}"
    if emptied:
        return (
            "other",
            evidence,
            f"{share}, but dropping it would empty {emptied} path(s), so it is "
            "relabelled instead",
        )
    if fidelity >= bridge_threshold:
        if screen.name == "transitions":
            tail = "dropping it joins transitions that already exist"
        else:
            tail = (
                "dropping it realigns the later steps with the paths that never had it"
            )
        return "drop", evidence, f"{share} (>= {bridge_threshold:.0%}): {tail}"
    if screen.name == "transitions":
        tail = (
            "dropping it would join events that rarely or never follow each other "
            "directly (artificial transitions), so it is relabelled instead"
        )
    else:
        tail = (
            "it mostly takes the place of another event, so dropping it would "
            "shift the later events to steps where the other paths have something "
            "else; relabelled, it keeps its position"
        )
    return "other", evidence, f"{share} (< {bridge_threshold:.0%}): {tail}"


# ======================================================================= the path
def _label_name(state: _State, labels: tuple, joiner: str, taken: set) -> str:
    name = joiner.join(state.names[i] for i in labels)
    while name in taken:
        name = f"{name} (merged)"
    return name


def _apply(state: _State, op: _Op, other_id: int | None, other_name: str, taken: set):
    """The state after ``op``; returns ``(state, label name, other_id)``."""
    new = state.copy()
    if op.operation == "drop":
        (u,) = op.labels
        new.label_of[new.label_of == u] = -1
        return new, None, other_id
    if op.operation == "other":
        (u,) = op.labels
        if other_id is None:
            new.names.append(other_name)
            new.kinds.append("other")
            other_id = len(new.names) - 1
        new.label_of[new.label_of == u] = other_id
        return new, other_name, other_id
    joiner = "+" if op.operation == "merge_chain" else "/"
    name = _label_name(state, op.labels, joiner, taken)
    new.names.append(name)
    new.kinds.append("merged")
    target = len(new.names) - 1
    for u in op.labels:
        new.label_of[new.label_of == u] = target
    return new, name, other_id


def _mergeable(state: _State, protected: set, st: _Stats) -> np.ndarray:
    """Labels an operation may act on: present in the paths, not ``other``, not
    holding a protected event."""
    n = len(state.names)
    ok = np.array([k != "other" for k in state.kinds], dtype=bool)
    ok &= st.occurrences[:n] > 0
    for code in protected:
        label = state.label_of[code]
        if label >= 0:
            ok[label] = False
    return ok


#: A label is thin when at least this share of the paths through its cells are in
#: cells that are not testable.
THIN_SHARE = 0.5


def _build_path(
    data: _Data,
    screen: _Screen,
    *,
    protected: set,
    min_path_share: float,
    chain_threshold: float,
    synonym_threshold: float,
    bridge_threshold: float,
    max_events: int | None,
    other_name: str,
) -> tuple[list[dict], list[_State], list[_Op]]:
    """The greedy path: the numbers and the state after every operation.

    Each step applies one operation, the first the screen's order finds:

    - ``transitions``: a chain, then a rare event, then a synonym of a thin event;
    - ``steps``: a rare event, then a synonym of a thin event, then a chain.

    A rare event (the rarest first) is merged into a synonym when one pools its
    thin cells, else dropped or relabelled ``other`` by the screen's rule
    (:func:`_rare_decision`); a merge of synonyms needs a thin member and must
    leave fewer untestable cells without losing a testable one. With
    ``max_events`` the rarest events left are then handled the same way until the
    vocabulary fits.
    """
    n_orig = len(data.events)
    state = _State(
        label_of=np.arange(n_orig, dtype=np.int64),
        names=[str(e) for e in data.events],
        kinds=["event"] * n_orig,
    )
    taken = {str(e) for e in data.events}
    src, _, _ = _transitions(data.codes, data.pid, n_orig)
    original_transitions = len(src)
    # the numbers of the current state, ``_thin`` included (the path rows drop it)
    current = _path_numbers(data, state, screen, original_transitions)

    def public(numbers: dict) -> dict:
        return {k: v for k, v in numbers.items() if k != "_thin"}

    rows = [
        {
            "step": 0,
            "operation": "start",
            "events": [],
            "label": None,
            **public(current),
            "reason": "the stream as it is",
        }
    ]
    states = [state]
    ops: list[_Op] = []
    other_id = None
    order = (
        ("chain", "rare", "synonym")
        if screen.name == "transitions"
        else ("rare", "synonym", "chain")
    )
    rejected: set = set()

    def synonym_op(st, mergeable, current, only=None) -> _Op | None:
        thin = current["_thin"]
        for distance, u, v, loop_u, loop_v in _synonym_candidates(
            st, mergeable, synonym_threshold
        ):
            if only is not None and only not in (u, v):
                continue
            if only is None and not (thin[u] >= THIN_SHARE or thin[v] >= THIN_SHARE):
                continue
            if st.occurrences[v] > st.occurrences[u]:
                u, v, loop_u, loop_v = v, u, loop_v, loop_u
            key = (state.names[u], state.names[v])
            if key in rejected:
                continue
            after, _, _ = _apply(
                state, _Op("merge_synonym", (u, v)), other_id, other_name, taken
            )
            numbers = _cell_numbers(
                data, *_compress(data, after), len(after.names), screen
            )
            # a merge of synonyms pools thin cells: fewer untestable cells, no
            # testable cell lost
            if not (
                numbers["untestable"] < current["untestable"]
                and numbers["testable"] >= current["testable"]
            ):
                rejected.add(key)
                continue
            pooled = current["untestable"] - numbers["untestable"]
            return _Op(
                "merge_synonym",
                (u, v),
                {"js_distance": distance, "self_loop_share": max(loop_u, loop_v)},
                (
                    f"'{state.names[u]}' and '{state.names[v]}' have the same "
                    "neighbours: incoming and outgoing transitions at Jensen-Shannon "
                    f"distance {distance:.3f} (<= {synonym_threshold:g}), self-loops "
                    f"{loop_u:.0%} / {loop_v:.0%}; merging them pools {pooled} thin "
                    f"{'cell' if pooled == 1 else 'cells'} without losing a testable "
                    "one"
                ),
            )
        return None

    def rare_op(st, mergeable, current, u, why_rare) -> _Op:
        merge = synonym_op(st, mergeable, current, only=u)
        if merge is not None:
            merge.evidence["path_share"] = float(st.path_share[u])
            merge.reason = f"{why_rare}; it is merged into its synonym: {merge.reason}"
            return merge
        action, evidence, why = _rare_decision(
            st, u, len(state.names), screen, bridge_threshold
        )
        evidence["path_share"] = float(st.path_share[u])
        return _Op(action, (u,), evidence, f"{why_rare}; {why}")

    while True:
        st = _stats_of(data, state, None)
        mergeable = _mergeable(state, protected, st)
        op = None
        for kind in order:
            if kind == "chain":
                found = _chain_candidates(
                    st, mergeable, chain_threshold, screen.min_support
                )
                if found:
                    _, u, v, p_next, p_prev = found[0]
                    op = _Op(
                        "merge_chain",
                        (u, v),
                        {"p_next": p_next, "p_prev": p_prev},
                        (
                            f"P(next = {state.names[v]} | {state.names[u]}) = "
                            f"{p_next:.2f} and P(prev = {state.names[u]} | "
                            f"{state.names[v]}) = {p_prev:.2f} (both >= "
                            f"{chain_threshold:g}): they almost always occur together"
                        ),
                    )
            elif kind == "synonym":
                op = synonym_op(st, mergeable, current)
            else:
                share = st.path_share
                rare = np.flatnonzero(mergeable & (share < min_path_share))
                if len(rare):
                    u = int(rare[np.lexsort((rare, share[rare]))][0])
                    op = rare_op(
                        st,
                        mergeable,
                        current,
                        u,
                        f"in {share[u]:.2%} of paths (< min_path_share "
                        f"{min_path_share:.2%})",
                    )
            if op is not None:
                break
        if op is None and max_events is not None:
            vocabulary = rows[-1]["vocabulary"]
            left = np.flatnonzero(mergeable)
            if vocabulary > max_events and len(left):
                share = st.path_share
                u = int(left[np.lexsort((left, share[left]))][0])
                op = rare_op(
                    st,
                    mergeable,
                    current,
                    u,
                    f"the vocabulary ({vocabulary}) is above max_events {max_events} "
                    f"and it is the rarest event left ({share[u]:.2%} of paths)",
                )
        if op is None:
            break
        names = [state.names[i] for i in op.labels]
        members = [str(data.events[c]) for i in op.labels for c in state.members(i)]
        state, label, other_id = _apply(state, op, other_id, other_name, taken)
        if label is not None:
            taken.add(label)
        ops.append(op)
        states.append(state)
        current = _path_numbers(data, state, screen, original_transitions)
        rows.append(
            {
                "step": len(ops),
                "operation": op.operation,
                "events": names,
                "members": members,
                "label": label,
                **public(current),
                "reason": op.reason,
            }
        )
    return rows, states, ops


def _choose(rows: list[dict], target: Any, max_events: int | None) -> int:
    """The step ``target`` picks (see ``get_ab_compression``)."""
    candidates = rows
    if max_events is not None:
        fit = [r for r in rows if r["vocabulary"] <= max_events]
        if fit:
            candidates = fit
        else:
            warn_user(
                f"no step of the compression path reaches max_events={max_events}: "
                f"the smallest vocabulary is {min(r['vocabulary'] for r in rows)} "
                "(protected events, keep and the declared edges are never merged "
                "or dropped)"
            )
            candidates = [min(rows, key=lambda r: (r["vocabulary"], r["step"]))]

    def mde(row):
        value = row["median_mde"]
        return value if math.isfinite(value) else math.inf

    if target == "testable":
        # the most testable cells; among those, the fewest untestable ones, then
        # the lowest median MDE, then the earliest step
        best = max(
            candidates,
            key=lambda r: (r["testable"], -r["untestable"], -mde(r), -r["step"]),
        )
    elif target == "mde":
        best = min(candidates, key=lambda r: (mde(r), -r["testable"], r["step"]))
    else:
        reached = [r for r in candidates if mde(r) <= float(target)]
        best = (
            min(reached, key=lambda r: r["step"])
            if reached
            else min(candidates, key=lambda r: (mde(r), r["step"]))
        )
    return int(best["step"])


# ======================================================================= plan frame
def _plan_rows(
    data: _Data,
    states: list[_State],
    ops: list[_Op],
    step: int,
    stats0: _Stats,
    protected_reason: dict,
    screen: _Screen,
) -> list[dict]:
    """One row per original event: its action and label at ``step``."""
    final = states[step]
    rows = []
    n_orig = len(data.events)
    # the operation that last touched each original event, up to `step`
    last_op: dict[int, int] = {}
    for k in range(1, step + 1):
        before, after = states[k - 1], states[k]
        changed = np.flatnonzero(before.label_of != after.label_of)
        for code in changed:
            last_op[int(code)] = k
    later: dict[int, int] = {}
    for k in range(step + 1, len(states)):
        before, after = states[k - 1], states[k]
        for code in np.flatnonzero(before.label_of != after.label_of):
            later.setdefault(int(code), k)
    out_total = stats0.count[:n_orig, :].sum(axis=1)
    loops = np.where(
        out_total > 0, np.diag(stats0.count)[:n_orig] / np.maximum(out_total, 1), 0.0
    )
    for code in range(n_orig):
        if stats0.occurrences[code] == 0:
            continue  # not in these paths (another segment's event)
        event = str(data.events[code])
        label_id = int(final.label_of[code])
        kind = final.kinds[label_id] if label_id >= 0 else None
        row: dict[str, Any] = {
            "event": event,
            "path_share": float(stats0.path_share[code]),
            "occurrences": int(stats0.occurrences[code]),
            "self_loop_share": float(loops[code]),
            "p_next": np.nan,
            "p_prev": np.nan,
            "js_distance": np.nan,
            "bridge_fidelity": np.nan,
        }
        if code in last_op:
            k = last_op[code]
            op = ops[k - 1]
            row["step"] = k
            row["operation"] = op.operation
            for key in ("p_next", "p_prev", "js_distance", "bridge_fidelity"):
                if key in op.evidence:
                    row[key] = float(op.evidence[key])
            if label_id < 0:
                row["action"], row["label"] = "drop", None
                row["reason"] = f"dropped at step {k}: {op.reason}"
            elif kind == "other":
                row["action"], row["label"] = "other", final.names[label_id]
                row["reason"] = (
                    f"relabelled '{final.names[label_id]}' at step {k}: {op.reason}"
                )
            else:
                row["action"], row["label"] = "merge", final.names[label_id]
                row["reason"] = (
                    f"merged into '{final.names[label_id]}' at step {k}: {op.reason}"
                )
        else:
            row["step"] = 0
            row["operation"] = "keep"
            row["action"], row["label"] = "keep", event
            if code in protected_reason:
                row["reason"] = f"kept: {protected_reason[code]}"
            elif code in later:
                row["reason"] = (
                    f"kept at this step: step {later[code]} of the path changes it, "
                    f"after the chosen step {step}"
                )
            else:
                row["reason"] = (
                    "kept: not rare (path share "
                    f"{stats0.path_share[code]:.1%}), not in a chain, no synonym "
                    "whose merge pools thin cells"
                )
        rows.append(row)
    return rows


def plan_mapping(plan: Any) -> tuple[dict, list[str], list[str]]:
    """``(mapping, merged labels, other labels)`` of a plan.

    ``plan`` is a ``get_ab_compression`` frame (or its rows as a list of dicts with
    ``event``, ``action`` and ``label``), or a mapping ``{event: label | None}``.
    In a mapping, a label shared by two or more events is a merge (its runs
    collapse); in a frame the ``action`` column says it, and ``other`` rows keep
    their runs.
    """
    allowed = [
        "a get_ab_compression plan (DataFrame), its rows as a list of dicts with "
        "event, action and label, or a {event: label | None} mapping"
    ]
    if isinstance(plan, pd.DataFrame):
        missing = [c for c in ("event", "action", "label") if c not in plan.columns]
        if missing:
            raise InvalidParameterError(
                "plan", f"a DataFrame without {missing}", allowed
            )
        by = (plan.attrs or {}).get("by")
        if by is not None and by in plan.columns and plan[by].nunique(dropna=False) > 1:
            raise InvalidParameterError(
                "plan",
                f"a plan of {plan[by].nunique(dropna=False)} '{by}' segments",
                [
                    "the rows of one segment, e.g. plan[plan['"
                    + str(by)
                    + "'] == value]"
                ],
                reason=(
                    "a plan per segment maps the same event differently in each "
                    "segment; filter the stream to the segment and apply its rows"
                ),
            )
        records = plan[["event", "action", "label"]].to_dict("records")
    elif isinstance(plan, dict):
        mapping = {}
        for event, label in plan.items():
            if not isinstance(event, str) or not (
                label is None or isinstance(label, str)
            ):
                raise InvalidParameterError("plan", {event: label}, allowed)
            mapping[event] = label
        shared = pd.Series([v for k, v in mapping.items() if v is not None and v != k])
        counts = shared.value_counts()
        merged = sorted(counts[counts >= 2].index.tolist())
        return mapping, merged, []
    elif isinstance(plan, (list, tuple)):
        records = list(plan)
        if not all(
            isinstance(r, dict) and {"event", "action", "label"} <= set(r)
            for r in records
        ):
            raise InvalidParameterError("plan", "a list that is not plan rows", allowed)
    else:
        raise InvalidParameterError("plan", type(plan).__name__, allowed)
    mapping: dict = {}
    merged: set = set()
    others: set = set()
    for record in records:
        event, action, label = record["event"], record["action"], record["label"]
        if action not in ACTIONS:
            raise InvalidParameterError("plan", f"action {action!r}", list(ACTIONS))
        if isinstance(label, float) and math.isnan(label):
            label = None
        if action == "drop":
            mapping[str(event)] = None
        elif action == "keep":
            mapping[str(event)] = str(event)
        else:
            if label is None:
                raise InvalidParameterError(
                    "plan", f"a {action} row of '{event}' without a label", allowed
                )
            mapping[str(event)] = str(label)
            (merged if action == "merge" else others).add(str(label))
    clash = merged & others
    if clash:
        raise InvalidParameterError(
            "plan",
            f"labels {sorted(clash)} used by both merge and other rows",
            allowed,
        )
    return mapping, sorted(merged), sorted(others)


# ======================================================================= homogeneity
def _homogeneity(data: _Data) -> dict:
    """How alike the paths of a population are, on the original vocabulary."""
    if not len(data.codes):
        return {
            "paths": data.n_paths,
            "events": 0,
            "effective_vocabulary": float("nan"),
            "transition_entropy": float("nan"),
            "distinct_path_share": float("nan"),
        }
    p = np.bincount(data.codes) / len(data.codes)
    p = p[p > 0]
    effective = float(np.exp(-(p * np.log(p)).sum()))
    n_orig = len(data.events)
    src, dst, _ = _transitions(data.codes, data.pid, n_orig)
    width = n_orig + 2
    joint = np.bincount(src * width + dst, minlength=width * width).reshape(
        width, width
    )
    total = joint.sum()
    row = joint.sum(axis=1, keepdims=True)
    with np.errstate(invalid="ignore", divide="ignore"):
        cond = np.where(joint > 0, joint / row, 1.0)
        entropy = float(-(joint / total * np.log2(cond)).sum())
    # distinct paths: hash every path's code sequence
    order = np.lexsort((np.arange(len(data.pid)), data.pid))
    codes = data.codes[order].astype(np.int64)
    pid = data.pid[order]
    bounds = np.flatnonzero(np.r_[True, pid[1:] != pid[:-1], True])
    seqs = {tuple(codes[a:b]) for a, b in zip(bounds[:-1], bounds[1:])}
    active = len(bounds) - 1
    return {
        "paths": int(data.n_paths),
        "active_paths": int(active),
        "events": int(len(np.unique(data.codes))),
        "effective_vocabulary": effective,
        "transition_entropy": entropy,
        "distinct_path_share": float(len(seqs) / active) if active else float("nan"),
    }


# ======================================================================= entry point
def _check_unit_interval(name: str, value: Any, *, open_low: bool = True) -> float:
    allowed = ["a number in (0, 1]" if open_low else "a number in [0, 1]"]
    if isinstance(value, bool) or not isinstance(value, (int, float, np.number)):
        raise InvalidParameterError(name, value, allowed)
    value = float(value)
    low_ok = value > 0 if open_low else value >= 0
    if not (math.isfinite(value) and low_ok and value <= 1):
        raise InvalidParameterError(name, value, allowed)
    return value


def _check_events(name: str, value: Any, available: set) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if isinstance(value, (bytes, dict)) or not hasattr(value, "__iter__"):
        raise InvalidParameterError(name, value, ["a list of event names"])
    events = list(value)
    for event in events:
        if event not in available:
            raise InvalidParameterError(name, event, sorted(available, key=str))
    return events


def ab_compression(
    stream: "Eventstream",
    diff: Any,
    screen: str = "transitions",
    *,
    assignment=None,
    horizon=None,
    path_col: str | None = None,
    edge_weight: str | None = None,
    max_steps: int | None = None,
    edges=None,
    min_path_share: float = 0.01,
    chain_threshold: float = 0.8,
    synonym_threshold: float = DEFAULT_SYNONYM_THRESHOLD,
    bridge_threshold: float = DEFAULT_BRIDGE_THRESHOLD,
    max_events: int | None = None,
    target: Any = "testable",
    step: int | None = None,
    keep=None,
    other: str = "other",
    by: str | None = None,
    by_pre_exposure: bool | None = None,
    correction: str = "bh",
    ci_level: float = 0.95,
    power: float = 0.8,
    min_support: int = 10,
) -> pd.DataFrame:
    """The plan of ``Eventstream.get_ab_compression`` (see its docstring)."""
    from retentioneering.tools.ab_transitions import _check_edges, _population

    check_choice("screen", screen, SCREENS)
    if screen == "transitions":
        if max_steps is not None:
            raise InvalidParameterError(
                "max_steps",
                max_steps,
                ["None (screen='transitions' takes edge_weight)"],
                reason="max_steps belongs to screen='steps'",
            )
        edge_weight = "unique_paths" if edge_weight is None else edge_weight
        check_choice("edge_weight", edge_weight, _EDGE_WEIGHTS)
        max_steps_value = 0
    else:
        for name, value in (("edge_weight", edge_weight), ("edges", edges)):
            if value is not None:
                raise InvalidParameterError(
                    name,
                    value,
                    ["None (screen='steps' takes max_steps)"],
                    reason=f"{name} belongs to screen='transitions'",
                )
        max_steps = 10 if max_steps is None else max_steps
        if (
            isinstance(max_steps, bool)
            or not isinstance(max_steps, (int, np.integer))
            or max_steps < 1
        ):
            raise InvalidParameterError("max_steps", max_steps, ["an int >= 1"])
        max_steps_value = int(max_steps)
    check_choice("correction", correction, CORRECTIONS)
    check_levels(ci_level, power)
    check_min_support(min_support)
    min_path_share = _check_unit_interval(
        "min_path_share", min_path_share, open_low=False
    )
    chain_threshold = _check_unit_interval("chain_threshold", chain_threshold)
    synonym_threshold = _check_unit_interval(
        "synonym_threshold", synonym_threshold, open_low=False
    )
    bridge_threshold = _check_unit_interval("bridge_threshold", bridge_threshold)
    if max_events is not None and (
        isinstance(max_events, bool)
        or not isinstance(max_events, (int, np.integer))
        or max_events < 1
    ):
        raise InvalidParameterError("max_events", max_events, ["None", "an int >= 1"])
    if not (
        (isinstance(target, str) and target in TARGETS)
        or (
            isinstance(target, (int, float, np.number))
            and not isinstance(target, bool)
            and math.isfinite(float(target))
            and float(target) > 0
        )
    ):
        raise InvalidParameterError(
            "target", target, list(TARGETS) + ["a positive median MDE to reach"]
        )
    if step is not None and (
        isinstance(step, bool) or not isinstance(step, (int, np.integer)) or step < 0
    ):
        raise InvalidParameterError("step", step, ["None", "an int >= 0"])
    if not isinstance(other, str) or not other or "->" in other:
        raise InvalidParameterError(
            "other", other, ["a non-empty event name without '->'"]
        )
    if by_pre_exposure is not None and not isinstance(by_pre_exposure, bool):
        raise InvalidParameterError(
            "by_pre_exposure", by_pre_exposure, ["None", "True", "False"]
        )
    if by_pre_exposure is not None and by is None:
        raise InvalidParameterError(
            "by_pre_exposure", by_pre_exposure, ["None without by"]
        )

    schema = stream.schema
    event_col = schema.event_col
    start_name, end_name = EventTypes().PATH_START.name, EventTypes().PATH_END.name
    available = set(map(str, pd.unique(stream.df[event_col]))) | {start_name, end_name}
    keep_events = _check_events("keep", keep, available)
    declared = _check_edges(edges, available) if edges is not None else []
    if by is not None and by not in schema.segment_cols:
        raise InvalidParameterError("by", by, list(schema.segment_cols))

    pop = _population(
        stream, diff, assignment=assignment, horizon=horizon, path_col=path_col
    )
    path_col = pop.path_col
    work = pop.window.work.df[[path_col, event_col, schema.index, schema.subindex]]
    work = work.sort_values([schema.index, schema.subindex], kind="stable")
    rows_of = pop.path_ids.get_indexer(work[path_col])
    ok = rows_of >= 0
    order = np.argsort(rows_of[ok], kind="stable")
    category = pd.Categorical(work[event_col])
    raw = np.asarray(category.codes, dtype=np.int64)[ok][order]
    labels = np.asarray([str(c) for c in category.categories], dtype=object)
    used, codes = np.unique(raw, return_inverse=True)
    names_used = labels[used]
    # codes in the order of the event names
    by_name = np.argsort(names_used.astype(str), kind="stable")
    rank = np.empty(len(by_name), dtype=np.int64)
    rank[by_name] = np.arange(len(by_name))
    events, codes = names_used[by_name], rank[codes]
    if other in set(events.tolist()):
        raise InvalidParameterError(
            "other",
            other,
            ["a name that is not an event of the stream"],
            reason=f"'{other}' is already an event; relabelling into it would merge them",
        )
    n1 = float((pop.arm == 0).sum())
    n2 = float((pop.arm == 1).sum())
    data = _Data(
        codes=codes.astype(np.int64),
        pid=rows_of[ok][order].astype(np.int64),
        n_paths=len(pop.path_ids),
        n1=n1,
        n2=n2,
        events=events.tolist(),
    )
    code_of = {e: i for i, e in enumerate(data.events)}
    protected_reason: dict[int, str] = {}
    for event in (start_name, end_name):
        if event in code_of:
            protected_reason[code_of[event]] = "a path boundary event"
    for event in keep_events:
        if event in code_of:
            protected_reason[code_of[event]] = "declared in keep"
    for pair in declared:
        for event in pair:
            if event in code_of:
                protected_reason.setdefault(
                    code_of[event], "an event of the declared edges"
                )
    protected = set(protected_reason)

    alpha = round(1.0 - float(ci_level), 12)
    scr = _Screen(
        name=screen,
        edge_weight=edge_weight or "unique_paths",
        max_steps=max_steps_value,
        min_support=int(min_support),
        alpha=alpha,
        power=float(power),
        correction=correction,
    )
    params = dict(
        protected=protected,
        min_path_share=min_path_share,
        chain_threshold=chain_threshold,
        synonym_threshold=synonym_threshold,
        bridge_threshold=bridge_threshold,
        max_events=None if max_events is None else int(max_events),
        other_name=other,
    )

    if by is None:
        groups = [(None, data)]
        by_flag, by_source = None, None
    else:
        by_flag, by_source = _by_flag(stream, by, by_pre_exposure)
        segment = (
            stream.df.groupby(path_col, observed=True)[by].first().reindex(pop.path_ids)
        )
        values = segment.astype(object).to_numpy()
        groups = []
        share1 = n1 / max(n1 + n2, 1)
        for value in sorted({v for v in values if not pd.isna(v)}, key=str):
            rows = values == value
            n_seg = float(rows.sum())
            groups.append(
                (value, data.subset(rows, n_seg * share1, n_seg * (1 - share1)))
            )
        missing = int(pd.isna(segment).sum())
        if missing:
            warn_user(
                f"{missing} compared path(s) have no '{by}' value (no event in the "
                "stream): they are in no segment's plan"
            )

    plan_parts, path_parts, homogeneity, chosen, mappings = [], [], [], {}, {}
    for value, part in groups:
        rows, states, ops = _build_path(part, scr, **params)
        pick = (
            _choose(rows, target, params["max_events"]) if step is None else int(step)
        )
        if pick >= len(states):
            raise InvalidParameterError(
                "step", step, [f"an int from 0 to {len(states) - 1}"]
            )
        stats0 = _stats_of(part, states[0], None)
        plan_rows = _plan_rows(part, states, ops, pick, stats0, protected_reason, scr)
        frame = pd.DataFrame(plan_rows)
        path = pd.DataFrame(rows)
        if by is not None:
            frame.insert(0, by, value)
            path.insert(0, by, value)
        plan_parts.append(frame)
        path_parts.append(path)
        key = "all" if value is None else str(value)
        chosen[key] = pick
        present = np.flatnonzero(stats0.occurrences[: len(part.events)] > 0)
        mappings[key] = [
            {
                str(part.events[c]): (
                    None if s.label_of[c] < 0 else s.names[int(s.label_of[c])]
                )
                for c in present
            }
            for s in states
        ]
        h = _homogeneity(part)
        h.update(
            segment=key,
            chosen_step=pick,
            steps=len(states) - 1,
            vocabulary_before=rows[0]["vocabulary"],
            vocabulary_after=rows[pick]["vocabulary"],
            cells_before=rows[0]["cells"],
            cells_after=rows[pick]["cells"],
            testable_before=rows[0]["testable"],
            testable_after=rows[pick]["testable"],
            median_mde_before=rows[0]["median_mde"],
            median_mde_after=rows[pick]["median_mde"],
        )
        homogeneity.append(h)

    plan = pd.concat(plan_parts, ignore_index=True)
    lead = [by] if by is not None else []
    plan = plan[lead + [c for c in PLAN_COLUMNS if c in plan.columns]]
    plan["step"] = plan["step"].astype(int)
    path = pd.concat(path_parts, ignore_index=True)
    path["events"] = path["events"].map(lambda e: ", ".join(map(str, e)))
    # a path without a step (nothing qualifies) has no members column yet
    members = path["members"] if "members" in path.columns else None
    path["members"] = (
        members.map(lambda e: ", ".join(map(str, e)) if isinstance(e, list) else "")
        if members is not None
        else ""
    )
    path = path[lead + [c for c in PATH_COLUMNS if c in path.columns] + ["members"]]
    plan.attrs = {
        "tool": "get_ab_compression",
        "screen": screen,
        "edge_weight": edge_weight if screen == "transitions" else None,
        "max_steps": max_steps_value if screen == "steps" else None,
        "path_col": path_col,
        "population": "assignment" if pop.with_assignment else "stream",
        "horizon": None if horizon is None else str(horizon),
        "group1_paths": int(n1),
        "group2_paths": int(n2),
        "min_path_share": min_path_share,
        "chain_threshold": chain_threshold,
        "synonym_threshold": synonym_threshold,
        "bridge_threshold": bridge_threshold,
        "max_events": params["max_events"],
        "target": target,
        "other": other,
        "keep": keep_events,
        "edges": [list(p) for p in declared] if declared else None,
        "min_support": int(min_support),
        "correction": correction,
        "ci_level": float(ci_level),
        "power": float(power),
        "by": by,
        "by_pre_exposure": by_flag,
        "by_pre_exposure_source": by_source,
        "chosen_step": chosen["all"] if by is None else chosen,
        "path": path,
        "mappings": mappings,
        "homogeneity": homogeneity if by is not None else homogeneity[0],
    }
    return plan


def _by_flag(stream, by: str, declared: bool | None) -> tuple[bool, str]:
    """Whether the ``by`` segment was fixed before exposure, as the scorecard's
    ``strata_pre_exposure`` (declared, or read from the recipe); warns otherwise."""
    if declared is not None:
        return declared, "declared" if declared else "not_pre_exposure"
    flag, how = _provenance(stream, by)
    if how == "declared":
        return bool(flag), "declared" if flag else "not_pre_exposure"
    if flag is True:
        return True, "recipe"
    warn_user(
        f"the stream's recipe does not show that by='{by}' was fixed before exposure. "
        "A segment the treatment can change makes each segment's vocabulary depend "
        "on the arms (by_pre_exposure=False). If it was fixed before exposure (a "
        "platform at signup, a segment recorded at assignment), pass "
        "by_pre_exposure=True."
    )
    return False, "not_pre_exposure"
