"""Funnel: sequential step counts, optionally with intervals and tests.

Without ``ci_level`` the output is the historic one plus the path totals. With
``ci_level`` every rate gets a Wilson interval and, in diff mode, every delta a
Newcombe interval; only the overall-rate deltas (unconditional denominators) get a
pooled z-test p-value. Step-to-step deltas after the first step are conditional on
reaching the previous step, which the treatment itself can change, so they are
descriptive: their interval stays, flagged ``step_conditional``, and they never get a
p-value.
"""

from dataclasses import dataclass

import numpy as np

from retentioneering import engine
from retentioneering.engine.dialect import event_position
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _stats
from retentioneering.tools.conversion import _check_ci_level
from retentioneering.tools.types import T_Diff
from retentioneering.utils.sql_quoting import quote_literal

if False:
    from retentioneering.eventstream.eventstream import Eventstream  # noqa: F401


#: Multiple-comparison corrections over the overall-step p-values.
CORRECTIONS = ("none", "holm", "bonferroni", "bh")

_ADJUST = {
    "holm": _stats.holm,
    "bonferroni": _stats.bonferroni,
    "bh": _stats.benjamini_hochberg,
}

#: How every interval and p-value of a CI-mode result is computed.
CI_BASIS = {"rate": "wilson", "delta": "newcombe", "p_value": "pooled_z"}


@dataclass
class Funnel:
    eventstream: "Eventstream"

    def fit(
        self,
        steps: list[str],
        diff: T_Diff = None,
        path_col: str | None = None,
        ci_level: float | None = None,
        correction: str = "none",
    ) -> dict:
        """Funnel counts and rates; see `Eventstream.funnel_data` for the contract."""
        ci_level = _check_ci_level(ci_level)
        if correction not in CORRECTIONS:
            raise InvalidParameterError("correction", correction, list(CORRECTIONS))
        if correction != "none" and ci_level is None:
            # p-values exist only with ci_level: a correction alone would be ignored
            raise InvalidParameterError(
                "correction", correction, ["'none' without ci_level", "a ci_level="]
            )
        path_col = path_col or self.eventstream.schema.path_col
        if path_col not in self.eventstream.schema.path_cols:
            raise InvalidParameterError(
                "path_col", path_col, self.eventstream.schema.path_cols
            )
        event_col = self.eventstream.schema.event_col
        available_events = set(self.eventstream.df[event_col].unique().tolist())
        for step in steps:
            if step not in available_events:
                raise InvalidParameterError("steps", step, sorted(available_events))

        if diff is None:
            result = self._fit_single(steps, path_col)
            if ci_level is not None:
                _add_single_ci(result, ci_level)
            return result
        else:
            if ci_level is None:
                # Descriptive mode keeps the widgets' event-level split (D2).
                stream1, stream2 = self.eventstream._split_two(diff, path_col=path_col)
                excluded_paths = None
            else:
                # Intervals and tests need disjoint paths: resolve at path level
                # and refuse a path that falls in both groups.
                from retentioneering.tools._diff import resolve_diff

                groups = resolve_diff(
                    self.eventstream, diff, path_col, require_constant=True
                )
                stream1 = self.eventstream.filter_events(
                    keep={path_col: list(groups.ids1)}
                )
                stream2 = self.eventstream.filter_events(
                    keep={path_col: list(groups.ids2)}
                )
                excluded_paths = groups.excluded_paths
            f1 = Funnel(stream1)._fit_single(steps, path_col)
            f2 = Funnel(stream2)._fit_single(steps, path_col)

            combined = []
            for i, step in enumerate(steps):
                s1, s2 = f1["steps"][i], f2["steps"][i]
                combined.append(
                    {
                        "step": step,
                        "funnel1_unique_paths": s1["unique_paths"],
                        "funnel1_conversion_rate": s1["conversion_rate"],
                        "funnel1_step_conversion_rate": s1["step_conversion_rate"],
                        "funnel2_unique_paths": s2["unique_paths"],
                        "funnel2_conversion_rate": s2["conversion_rate"],
                        "funnel2_step_conversion_rate": s2["step_conversion_rate"],
                        "delta_unique_paths": s1["unique_paths"] - s2["unique_paths"],
                        "delta_conversion_rate": s1["conversion_rate"]
                        - s2["conversion_rate"],
                        "delta_step_conversion_rate": s1["step_conversion_rate"]
                        - s2["step_conversion_rate"],
                    }
                )
            result = {
                "steps": combined,
                "funnel1_total_paths": f1["total_paths"],
                "funnel2_total_paths": f2["total_paths"],
            }
            if ci_level is not None:
                _add_diff_ci(result, ci_level, correction, excluded_paths)
            return result

    def _fit_single(self, steps: list[str], path_col: str) -> dict:
        event_col = self.eventstream.schema.event_col
        index_col = self.eventstream.schema.index

        df = self.eventstream.df
        total_paths = int(df[path_col].nunique())
        if not steps:
            return {"steps": [], "total_paths": total_paths}

        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        index_col_q = engine.quote_ident(index_col)
        subindex_col_q = engine.quote_ident(self.eventstream.schema.subindex)
        position = event_position(index_col_q, subindex_col_q)
        joined_position = event_position(f"df.{index_col_q}", f"df.{subindex_col_q}")

        # Sequential funnel semantics: a path reaches step k iff there
        # exist positions p1 < p2 < ... < pk with event(p_j) = steps[j].
        # A position is (index, subindex): synthetic markers can share their
        # source event's index, but still precede it in the stream's order.
        # Chained CTEs: step_k holds, per path, the earliest occurrence of
        # steps[k-1] that comes strictly after the path's step_{k-1} match.
        # This correctly handles repeated events and duplicate step names.
        #
        # path_cols is validated (coarsest-first, strictly nested) at
        # Eventstream construction time, and path_col is restricted to
        # schema.path_cols above, so comparing full event positions is
        # correct at any accepted grain (see ADR-0004).
        ctes = []
        for step_num, step_event in enumerate(steps, start=1):
            ev = quote_literal(step_event)
            if step_num == 1:
                ctes.append(
                    f"step_1 AS ("
                    f"SELECT {path_col_q} AS path_id, MIN({position}) AS idx "
                    f"FROM df "
                    f"WHERE {event_col_q} = {ev} "
                    f"GROUP BY {path_col_q}"
                    f")"
                )
            else:
                prev = f"step_{step_num - 1}"
                ctes.append(
                    f"step_{step_num} AS ("
                    f"SELECT df.{path_col_q} AS path_id, MIN({joined_position}) AS idx "
                    f"FROM df "
                    f"JOIN {prev} ON df.{path_col_q} = {prev}.path_id "
                    f"WHERE df.{event_col_q} = {ev} AND {joined_position} > {prev}.idx "
                    f"GROUP BY df.{path_col_q}"
                    f")"
                )
        counts = ", ".join(
            f"(SELECT COUNT(*) FROM step_{i}) AS count_{i}"
            for i in range(1, len(steps) + 1)
        )
        query = f"WITH {', '.join(ctes)} SELECT {counts}"
        row = engine.run(query, df=df).iloc[0]

        funnel_data = []
        prev_count = None
        for step_num, step_event in enumerate(steps, start=1):
            count = int(row[f"count_{step_num}"])
            conversion_rate = count / total_paths if total_paths > 0 else 0.0
            step_conversion_rate = (
                conversion_rate
                if prev_count is None
                else (count / prev_count if prev_count > 0 else 0.0)
            )
            funnel_data.append(
                {
                    "step": step_event,
                    "unique_paths": count,
                    "conversion_rate": conversion_rate,
                    "step_conversion_rate": step_conversion_rate,
                }
            )
            prev_count = count

        return {"steps": funnel_data, "total_paths": total_paths}


def _counts(steps: list[dict], prefix: str, total: int):
    """Per-step successes and the two denominators (overall, previous step)."""
    k = np.array([s[f"{prefix}unique_paths"] for s in steps], dtype=float)
    n_overall = np.full_like(k, float(total))
    n_step = np.concatenate([[float(total)], k[:-1]])
    return k, n_overall, n_step


def _put(steps: list[dict], key: str, values) -> None:
    for step, value in zip(steps, np.atleast_1d(values)):
        step[key] = float(value)


def _add_single_ci(result: dict, ci_level: float) -> None:
    steps = result["steps"]
    k, n_all, n_step = _counts(steps, "", result["total_paths"])
    for name, n in (("conversion_rate", n_all), ("step_conversion_rate", n_step)):
        low, high = _stats.wilson_ci(k, n, ci_level)
        _put(steps, f"{name}_ci_low", low)
        _put(steps, f"{name}_ci_high", high)
    result["ci_level"] = ci_level
    result["ci_basis"] = {"rate": CI_BASIS["rate"]}


def _add_diff_ci(
    result: dict, ci_level: float, correction: str, excluded_paths: int
) -> None:
    steps = result["steps"]
    k1, n1, ns1 = _counts(steps, "funnel1_", result["funnel1_total_paths"])
    k2, n2, ns2 = _counts(steps, "funnel2_", result["funnel2_total_paths"])
    for group, k, n, ns in (("funnel1", k1, n1, ns1), ("funnel2", k2, n2, ns2)):
        for name, denominator in (
            ("conversion_rate", n),
            ("step_conversion_rate", ns),
        ):
            low, high = _stats.wilson_ci(k, denominator, ci_level)
            _put(steps, f"{group}_{name}_ci_low", low)
            _put(steps, f"{group}_{name}_ci_high", high)

    low, high = _stats.newcombe_ci(k1, n1, k2, n2, ci_level)
    _put(steps, "delta_conversion_rate_ci_low", low)
    _put(steps, "delta_conversion_rate_ci_high", high)
    p_value = np.atleast_1d(_stats.two_proportion_z(k1, n1, k2, n2)[1])
    _put(steps, "delta_conversion_rate_p_value", p_value)
    adjusted = p_value if correction == "none" else _ADJUST[correction](p_value)
    _put(steps, "delta_conversion_rate_p_value_adjusted", adjusted)

    # Step-to-step deltas: interval only. After the first step the denominator
    # (paths that reached the previous step) is itself an outcome of the
    # treatment, so the comparison is descriptive and gets no p-value.
    low, high = _stats.newcombe_ci(k1, ns1, k2, ns2, ci_level)
    _put(steps, "delta_step_conversion_rate_ci_low", low)
    _put(steps, "delta_step_conversion_rate_ci_high", high)
    for i, step in enumerate(steps):
        step["step_conditional"] = i > 0

    result["ci_level"] = ci_level
    result["ci_basis"] = dict(CI_BASIS)
    result["correction"] = correction
    # spelled as get_ab_test spells it: "metric=<what>/<dimension>", "none"
    result["correction_family"] = (
        "none" if correction == "none" else "metric=delta_conversion_rate/steps"
    )
    result["excluded_paths"] = int(excluded_paths)
