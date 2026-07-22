from dataclasses import dataclass
from typing import TYPE_CHECKING

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.exceptions import (
    EmptyEventstreamError,
    InvalidParameterError,
)

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream


def _sql_str(value: str) -> str:
    """Escape an event name as a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


@dataclass
class RouteStats:
    """Statistics of a route — a strict contiguous sequence of transitions
    A→B→…→N — over the eventstream's paths.

    One SQL pass: a chain of ``lead()`` columns finds every occurrence of
    the route (overlapping occurrences count, consistent with transition
    counts), which yields the occurrence count, the number of distinct
    paths containing the route, and per-occurrence durations for the time
    quantiles.
    """

    eventstream: "Eventstream"

    def fit(self, nodes: list[str], path_col: str | None = None) -> dict:
        """
        Parameters
        ----------
        nodes:
            The route: two or more event names, in order. Matching is strict
            and contiguous — no gaps, no stutter collapsing.
        path_col:
            Path ID column override; defaults to `schema.path_col`.

        Returns
        -------
        dict with:
          - ``n_paths`` — total number of paths in the stream,
          - ``unique_paths`` — paths containing the route at least once,
          - ``unique_paths_share`` — the above divided by ``n_paths``,
          - ``occurrences`` — route traversals (a path may contain several),
          - ``avg_per_path`` — occurrences / n_paths,
          - ``time_median`` / ``time_q95`` — seconds from the route's first
            to last event within one traversal (None when no occurrences),
          - ``proba`` — Markov probability of the route: the product of
            ``P(next | source)`` over its consecutive event pairs.
        """
        path_col = path_col or self.eventstream.schema.path_col
        if path_col not in self.eventstream.schema.path_cols:
            raise InvalidParameterError(
                "path_col", path_col, self.eventstream.schema.path_cols
            )
        nodes = [str(n) for n in (nodes or [])]
        if len(nodes) < 2:
            raise InvalidParameterError(
                "nodes", nodes, ["a route of two or more event names"]
            )
        if self.eventstream.is_empty():
            raise EmptyEventstreamError(
                "Cannot compute path stats for empty eventstream"
            )

        schema = self.eventstream.schema
        event_q = engine.quote_ident(schema.event_col)
        ts_q = engine.quote_ident(schema.timestamp_col)
        path_q = engine.quote_ident(path_col)
        index_q = engine.quote_ident(schema.index)
        subindex_q = engine.quote_ident(schema.subindex)

        # The graph always renders path_start/path_end, so routes may
        # include them — match against the same boundary-enriched stream
        # the transition matrix uses.
        df = self.eventstream.add_start_end_events(path_col=path_col).df

        steps = len(nodes) - 1
        lead_events = ",\n                ".join(
            f"lead({event_q}, {i}) over w as e{i}" for i in range(1, steps + 1)
        )
        conditions = " and ".join(
            [f"{event_q} = {_sql_str(nodes[0])}"]
            + [f"e{i} = {_sql_str(nodes[i])}" for i in range(1, steps + 1)]
        )
        duration = dialect.epoch(f"t_end - {ts_q}")

        query = f"""
        with seq as (
            select
                {path_q},
                {event_q},
                {ts_q},
                {lead_events},
                lead({ts_q}, {steps}) over w as t_end
            from df
            window w as (
                partition by {path_q} order by {index_q}, {subindex_q}
            )
        ),
        hits as (
            select {path_q}, {duration} as duration
            from seq
            where {conditions}
        )
        select
            (select count(distinct {path_q}) from df) as n_paths,
            count(*) as occurrences,
            count(distinct {path_q}) as unique_paths,
            median(duration) as time_median,
            quantile_cont(duration, 0.95) as time_q95
        from hits
        """
        row = engine.run(query, df=df).iloc[0]

        # Markov product of the route's edge probabilities:
        # prod over consecutive pairs of count(a→b) / count(a→*).
        sources = sorted({nodes[i] for i in range(len(nodes) - 1)})
        source_list = ", ".join(_sql_str(s) for s in sources)
        trans_query = f"""
        with seq as (
            select
                {event_q} as src,
                lead({event_q}) over (
                    partition by {path_q} order by {index_q}, {subindex_q}
                ) as dst
            from df
        )
        select src, dst, count(*) as cnt
        from seq
        where src in ({source_list}) and dst is not null
        group by src, dst
        """
        trans = engine.run(trans_query, df=df)
        pair_counts = {
            (r.src, r.dst): int(r.cnt) for r in trans.itertuples(index=False)
        }
        out_totals: dict[str, int] = {}
        for (src, _), cnt in pair_counts.items():
            out_totals[src] = out_totals.get(src, 0) + cnt
        proba = 1.0
        for a, b in zip(nodes, nodes[1:]):
            total = out_totals.get(a, 0)
            proba *= pair_counts.get((a, b), 0) / total if total else 0.0

        n_paths = int(row["n_paths"])
        occurrences = int(row["occurrences"])
        unique_paths = int(row["unique_paths"])
        return {
            "n_paths": n_paths,
            "unique_paths": unique_paths,
            "unique_paths_share": unique_paths / n_paths if n_paths else 0.0,
            "occurrences": occurrences,
            "avg_per_path": occurrences / n_paths if n_paths else 0.0,
            "time_median": float(row["time_median"])
            if occurrences and row["time_median"] == row["time_median"]
            else None,
            "time_q95": float(row["time_q95"])
            if occurrences and row["time_q95"] == row["time_q95"]
            else None,
            "proba": proba,
        }
