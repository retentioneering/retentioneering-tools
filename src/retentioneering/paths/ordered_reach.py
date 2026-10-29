"""Shared closed ordered reach for path tools and experiment outcomes."""

import pandas as pd

from retentioneering import engine
from retentioneering.engine.dialect import event_position
from retentioneering.utils.sql_quoting import quote_literal


def _catalogue(stream, event_catalog=None):
    if event_catalog is None:
        return {}
    if not isinstance(event_catalog, dict):
        raise TypeError("event_catalog must map event-column names to lists of names")
    result = {}
    for column, names in event_catalog.items():
        if not isinstance(column, str) or column not in stream.df:
            raise ValueError(f"Unknown event_catalog column: {column}")
        if not isinstance(names, list) or not all(
            isinstance(name, str) and name for name in names
        ):
            raise ValueError(
                "Each event_catalog value must be a list of nonempty event names"
            )
        result[column] = sorted(set(names))
    return result


def _vocabulary(stream, column, catalog):
    if column not in stream.df:
        raise ValueError(f"Missing event column: {column}")
    return set(stream.df[column].dropna()) | set(catalog.get(column, []))


def ordered_reach(stream, steps, path_col=None, frame=None, event_catalog=None):
    """Per-path closed ordered reach, preserving Retentioneering index/subindex.

    The catalogue is validated against the original stream, before filtering.
    Duplicate step names remain distinct by ordinal. Empty frames are valid.
    """
    path_col = path_col or stream.schema.path_col
    if path_col not in stream.schema.path_cols:
        raise ValueError(f"path_col must be in {stream.schema.path_cols}")
    if (
        not isinstance(steps, list)
        or not steps
        or not all(isinstance(s, str) for s in steps)
    ):
        raise ValueError("steps must be a nonempty list of event names")
    event = stream.schema.event_col
    unknown = set(steps) - _vocabulary(stream, event, _catalogue(stream, event_catalog))
    if unknown:
        raise ValueError(
            f"Unknown funnel events in source catalogue: {sorted(unknown)}"
        )
    df = stream.df if frame is None else frame
    columns = [f"reach_{j}" for j in range(1, len(steps) + 1)]
    if df.empty:
        return pd.DataFrame(
            columns=columns, index=pd.Index([], name=path_col), dtype=float
        )
    pq, eq = engine.quote_ident(path_col), engine.quote_ident(event)
    ix, sub = (
        engine.quote_ident(stream.schema.index),
        engine.quote_ident(stream.schema.subindex),
    )
    pos, jpos = event_position(ix, sub), event_position(f"df.{ix}", f"df.{sub}")
    ctes = [f"paths AS (SELECT DISTINCT {pq} AS path_id FROM df)"]
    for j, step in enumerate(steps, 1):
        ev = quote_literal(step)
        if j == 1:
            sql = f"SELECT {pq} AS path_id, MIN({pos}) AS idx FROM df WHERE {eq}={ev} GROUP BY {pq}"
        else:
            sql = (
                f"SELECT df.{pq} AS path_id, MIN({jpos}) AS idx FROM df "
                f"JOIN step_{j - 1} prev ON df.{pq}=prev.path_id "
                f"WHERE df.{eq}={ev} AND {jpos}>prev.idx GROUP BY df.{pq}"
            )
        ctes.append(f"step_{j} AS ({sql})")
    flags = ", ".join(
        f"CASE WHEN s{j}.path_id IS NULL THEN 0 ELSE 1 END AS reach_{j}"
        for j in range(1, len(steps) + 1)
    )
    joins = " ".join(
        f"LEFT JOIN step_{j} s{j} ON paths.path_id=s{j}.path_id"
        for j in range(1, len(steps) + 1)
    )
    out = engine.run(
        f"WITH {', '.join(ctes)} SELECT paths.path_id, {flags} FROM paths {joins}",
        df=df,
    )
    return out.rename(columns={"path_id": path_col}).set_index(path_col).astype(float)
