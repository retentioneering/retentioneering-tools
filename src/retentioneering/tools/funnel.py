from dataclasses import dataclass

import duckdb

from retentioneering.tools.types import T_Diff

if False:
    from retentioneering.eventstream.eventstream import Eventstream  # noqa: F401


def _sql_str(value: str) -> str:
    """Escape a string value for safe embedding in a DuckDB SQL literal."""
    return "'" + value.replace("'", "''") + "'"


@dataclass
class Funnel:
    eventstream: "Eventstream"

    def fit(
        self,
        steps: list[str],
        diff: T_Diff = None,
        path_id_col: str | None = None,
    ) -> dict:
        path_id_col = path_id_col or self.eventstream.schema.path_col
        event_col   = self.eventstream.schema.event_col
        index_col   = self.eventstream.schema.index

        if diff is None:
            df = self.eventstream.df
            total_paths = int(df[path_id_col].nunique())
            funnel_data = []

            for step_idx, step_event in enumerate(steps):
                ev = _sql_str(step_event)
                if step_idx == 0:
                    query = f"""
                        SELECT COUNT(DISTINCT {path_id_col}) AS count
                        FROM df
                        WHERE {event_col} = {ev}
                    """
                else:
                    prev_conditions = " AND ".join([
                        f"SUM(CASE WHEN {event_col} = {_sql_str(steps[i])} THEN 1 ELSE 0 END) > 0"
                        for i in range(step_idx)
                    ])
                    order_conditions = " AND ".join([
                        f"MAX(CASE WHEN {event_col} = {_sql_str(steps[i])} THEN {index_col} ELSE 0 END) < "
                        f"MAX(CASE WHEN {event_col} = {_sql_str(steps[i+1])} THEN {index_col} ELSE 0 END)"
                        for i in range(step_idx)
                    ])
                    query = f"""
                        SELECT COUNT(DISTINCT {path_id_col}) AS count
                        FROM (
                            SELECT {path_id_col}
                            FROM df
                            GROUP BY {path_id_col}
                            HAVING {prev_conditions}
                              AND SUM(CASE WHEN {event_col} = {ev} THEN 1 ELSE 0 END) > 0
                              AND {order_conditions}
                        )
                    """
                count = int(duckdb.query(query).df()["count"].iloc[0])
                funnel_data.append({
                    "step": step_event,
                    "unique_paths": count,
                    "conversion_rate": count / total_paths if total_paths > 0 else 0.0,
                })

            return {"steps": funnel_data}

        else:
            stream1, stream2 = self.eventstream.split_two(diff, path_id_col=path_id_col)
            f1 = Funnel(stream1).fit(steps=steps, path_id_col=path_id_col)
            f2 = Funnel(stream2).fit(steps=steps, path_id_col=path_id_col)

            combined = []
            for i, step in enumerate(steps):
                s1, s2 = f1["steps"][i], f2["steps"][i]
                combined.append({
                    "step": step,
                    "funnel1_unique_paths":    s1["unique_paths"],
                    "funnel1_conversion_rate": s1["conversion_rate"],
                    "funnel2_unique_paths":    s2["unique_paths"],
                    "funnel2_conversion_rate": s2["conversion_rate"],
                    "delta_unique_paths":      s2["unique_paths"]    - s1["unique_paths"],
                    "delta_conversion_rate":   s2["conversion_rate"] - s1["conversion_rate"],
                })
            return {"steps": combined}
