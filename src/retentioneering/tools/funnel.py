from dataclasses import dataclass

from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools.types import T_Diff

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
            f1 = Funnel(stream1)._fit_single(steps, path_col, self.eventstream)
            f2 = Funnel(stream2)._fit_single(steps, path_col, self.eventstream)

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

    def _fit_single(self, steps: list[str], path_col: str, source_stream=None) -> dict:
        if not steps:
            return {"steps": []}
        from retentioneering.paths.ordered_reach import ordered_reach

        total_paths = int(self.eventstream.df[path_col].nunique())
        source = self.eventstream if source_stream is None else source_stream
        counts = ordered_reach(
            source, steps, path_col=path_col, frame=self.eventstream.df
        ).sum()

        funnel_data = []
        prev_count = None
        for step_num, step_event in enumerate(steps, start=1):
            count = int(counts[f"reach_{step_num}"])
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
