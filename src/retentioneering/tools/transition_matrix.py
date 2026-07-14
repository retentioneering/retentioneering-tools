from dataclasses import dataclass
from typing import TYPE_CHECKING, get_args

import pandas as pd

from retentioneering import engine
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import InvalidParameterError, EmptyEventstreamError
from .types import T_Diff, T_TransitionMatrixValues

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream

TRANSITION_MATRIX_VALUES_OPTIONS = get_args(T_TransitionMatrixValues)


@dataclass
class TransitionMatrix:
    eventstream: "Eventstream"

    def fit(
        self,
        values: T_TransitionMatrixValues,
        diff: T_Diff = None,
        path_col: str | None = None,
    ) -> pd.DataFrame:
        path_col = path_col or self.eventstream.schema.path_col
        if path_col not in self.eventstream.schema.path_cols:
            raise InvalidParameterError(
                "path_col", path_col, self.eventstream.schema.path_cols
            )
        event_col = self.eventstream.schema.event_col
        timestamp_col = self.eventstream.schema.timestamp_col
        index_col = self.eventstream.schema.index
        subindex_col = self.eventstream.schema.subindex
        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        timestamp_col_q = engine.quote_ident(timestamp_col)
        index_col_q = engine.quote_ident(index_col)
        subindex_col_q = engine.quote_ident(subindex_col)
        # path_cols is validated (coarsest-first, strictly nested) at Eventstream
        # construction time, and path_col is restricted to schema.path_cols
        # above, so ordering by index_col is correct at any accepted grain
        # (see ADR-0004).
        order_by = f"{index_col_q}, {subindex_col_q}"
        time_values = ["time_median", "time_q95"]

        if self.eventstream.is_empty():
            raise EmptyEventstreamError(
                "Cannot calculate transition matrix for empty eventstream"
            )

        if diff is None:
            df = self.eventstream.add_start_end_events(path_col=path_col).df
            event_types = EventTypes()
            tm = pd.DataFrame()

            if values in [
                "count",
                "share_of_total",
                "avg_per_path",
                "proba_out",
                "proba_in",
            ]:
                query = f"""
                select {event_col_q}, next_{event_col}, count(*) as cnt
                from (
                    select
                        {event_col_q},
                        lead({event_col_q}) over (
                            partition by {path_col_q}
                            order by {order_by}
                        ) as next_{event_col},
                        {path_col_q}
                    from df
                ) where next_{event_col} is not null
                group by {event_col_q}, next_{event_col}
                """
                tm_abs = (
                    engine.run(query, df=df)
                    .pivot(index=event_col, columns=f"next_{event_col}", values="cnt")
                    .fillna(0)
                )

                if values == "count":
                    tm = tm_abs.astype(int)
                elif values == "share_of_total":
                    total = tm_abs.sum().sum()
                    tm = tm_abs / total
                elif values == "avg_per_path":
                    total_paths = df[path_col].nunique()
                    tm = tm_abs / total_paths
                elif values == "proba_out":
                    tm = tm_abs.div(tm_abs.sum(axis=1), axis=0).fillna(0)
                elif values == "proba_in":
                    tm = tm_abs.div(tm_abs.sum(axis=0), axis=1).fillna(0)

            elif values == "unique_paths":
                query = f"""
                select {event_col_q}, next_{event_col}, count(distinct {path_col_q}) as cnt
                from (
                    select
                        {event_col_q},
                        lead({event_col_q}) over (
                            partition by {path_col_q}
                            order by {order_by}
                        ) as next_{event_col},
                        {path_col_q}
                    from df
                ) where next_{event_col} is not null
                group by {event_col_q}, next_{event_col}
                """
                tm = (
                    engine.run(query, df=df)
                    .pivot(index=event_col, columns=f"next_{event_col}", values="cnt")
                    .fillna(0)
                    .astype(int)
                )

            elif values in ["time_median", "time_q95"]:
                if values == "time_median":
                    agg_func = "median(timedelta)"
                else:
                    agg_func = "quantile_cont(timedelta, 0.95)"

                query = f"""
                select {event_col_q}, next_{event_col}, {agg_func} as timedelta
                from (
                    select
                        {event_col_q},
                        lead({event_col_q}) over (
                            partition by {path_col_q}
                            order by {order_by}
                        ) as next_{event_col},
                        lead({timestamp_col_q}) over (
                            partition by {path_col_q}
                            order by {order_by}
                        ) as next_{timestamp_col},
                        date_diff('second', {timestamp_col_q}, next_{timestamp_col}) as timedelta
                    from df
                ) where next_{event_col} is not null
                group by {event_col_q}, next_{event_col}
                """
                timedeltas = engine.run(query, df=df)
                timedeltas = timedeltas.set_index([event_col, f"next_{event_col}"])[
                    "timedelta"
                ]
                timedeltas = pd.to_timedelta(timedeltas, unit="s")
                tm = timedeltas.unstack()

            else:
                raise InvalidParameterError(
                    "values", values, list(TRANSITION_MATRIX_VALUES_OPTIONS)
                )

            path_start = event_types.PATH_START.name
            path_end = event_types.PATH_END.name
            events = (
                tm.columns.drop([path_start, path_end], errors="ignore")
                .sort_values()
                .tolist()
            )
            event_order = [path_start] + events + [path_end]
            fill_value = 0 if values not in time_values else pd.NaT
            tm = tm.reindex(
                index=event_order, columns=event_order, fill_value=fill_value
            )
            return tm

        else:
            stream1, stream2 = self.eventstream._split_two(diff, path_col=path_col)
            tm1 = TransitionMatrix(stream1).fit(values, path_col=path_col)
            tm2 = TransitionMatrix(stream2).fit(values, path_col=path_col)
            index = tm1.index.union(tm2.index)
            columns = tm1.columns.union(tm2.columns)
            fill_value = 0 if values not in time_values else pd.NaT
            tm1 = tm1.reindex(index=index, columns=columns, fill_value=fill_value)
            tm2 = tm2.reindex(index=index, columns=columns, fill_value=fill_value)
            return tm1 - tm2, tm1, tm2
