from dataclasses import dataclass

from retentioneering import engine
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools.types import T_Diff
from retentioneering.utils.sql_quoting import quote_literal

if False:
    from retentioneering.eventstream.eventstream import Eventstream  # noqa: F401


@dataclass
class Funnel:
    eventstream: "Eventstream"

    def fit(
        self,
        steps: list[str],
        diff: T_Diff = None,
        path_col: str | None = None,
    ) -> dict:
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
            return self._fit_single(steps, path_col)
        else:
            stream1, stream2 = self.eventstream._split_two(diff, path_col=path_col)
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
            return {"steps": combined}

    def _fit_single(self, steps: list[str], path_col: str) -> dict:
        event_col = self.eventstream.schema.event_col
        index_col = self.eventstream.schema.index

        if not steps:
            return {"steps": []}

        df = self.eventstream.df
        total_paths = int(df[path_col].nunique())
        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        index_col_q = engine.quote_ident(index_col)

        # Sequential funnel semantics: a path reaches step k iff there
        # exist event indices i1 < i2 < ... < ik with event(i_j) = steps[j].
        # Chained CTEs: step_k holds, per path, the earliest occurrence of
        # steps[k-1] that comes strictly after the path's step_{k-1} match.
        # This correctly handles repeated events and duplicate step names.
        #
        # path_cols is validated (coarsest-first, strictly nested) at
        # Eventstream construction time, and path_col is restricted to
        # schema.path_cols above, so comparing index_col directly is
        # correct at any accepted grain (see ADR-0004).
        ctes = []
        for step_num, step_event in enumerate(steps, start=1):
            ev = quote_literal(step_event)
            if step_num == 1:
                ctes.append(
                    f"step_1 AS ("
                    f"SELECT {path_col_q} AS path_id, MIN({index_col_q}) AS idx "
                    f"FROM df "
                    f"WHERE {event_col_q} = {ev} "
                    f"GROUP BY {path_col_q}"
                    f")"
                )
            else:
                prev = f"step_{step_num - 1}"
                ctes.append(
                    f"step_{step_num} AS ("
                    f"SELECT df.{path_col_q} AS path_id, MIN(df.{index_col_q}) AS idx "
                    f"FROM df "
                    f"JOIN {prev} ON df.{path_col_q} = {prev}.path_id "
                    f"WHERE df.{event_col_q} = {ev} AND df.{index_col_q} > {prev}.idx "
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

        return {"steps": funnel_data}
