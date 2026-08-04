from typing import Tuple

import pandas as pd

from retentioneering import engine
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import InvalidParameterError, PreprocessingConfigError
from retentioneering.paths import anchors

PROCESSOR_NAME = "truncate_paths"


class TruncatePaths(DataProcessor):
    """
    Truncate paths to the window between two anchors (inclusive).

    Parameters
    ----------
    start_event : str or dict or list
        Where the window opens. An event name, an anchor spec, or a list of
        either — see `Eventstream.truncate_paths` for the spec's keys and for
        what a list means.
    end_event : str or dict or list
        Where the window closes, same forms as `start_event`. Searched *after*
        the resolved start, so a path whose end anchor only occurs before its
        start anchor is dropped rather than yielding an inverted window.
    path_col : str, optional
        Path ID column name. If None, taken from schema.
    event_col : str, optional
        Event column name. If None, taken from schema.
    """

    def __init__(
        self,
        start_event,
        end_event,
        path_col: str | None = None,
        event_col: str | None = None,
    ) -> None:
        self.start_specs = self._parse(start_event, "start_event")
        self.end_specs = self._parse(end_event, "end_event")
        self.path_col = path_col
        self.event_col = event_col
        super().__init__()

    @staticmethod
    def _parse(value, param: str) -> list[anchors.AnchorSpec]:
        # The bare-empty-string case keeps its long-standing message; anything
        # else surfaces as this processor's own config error rather than as the
        # anchors module's InvalidParameterError.
        if value == "" or value is None:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"Parameter '{param}' must be a non-empty string."
            )
        try:
            return anchors.parse_specs(value, param=param)
        except InvalidParameterError as exc:
            raise PreprocessingConfigError(PROCESSOR_NAME, exc.message) from exc

    def _bounds(
        self,
        df: pd.DataFrame,
        schema: EventstreamSchema,
        specs: list[anchors.AnchorSpec],
        side: str,
        path_col: str,
        not_before: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """
        Resolve every spec on one side and keep the narrowest window they imply:
        the latest of the start bounds, the earliest of the end bounds.

        A spec that does not resolve for a given path simply does not constrain
        it, which is what makes a boundary sentinel usable as a fallback
        (``["purchase", "path_end"]`` cuts at the first purchase, or at the end
        of the path when there is none). A path constrained by no spec at all on
        this side is dropped.
        """
        resolved = [
            anchors.resolve_bound(
                df,
                schema,
                spec,
                side=side,
                path_col=path_col,
                event_col=self.event_col,
                not_before=not_before,
            )
            for spec in specs
        ]
        stacked = pd.concat(resolved, ignore_index=True)
        if stacked.empty:
            return stacked.assign(bound=pd.Series(dtype="int64"))
        agg = "max" if side == "start" else "min"
        return stacked.groupby(path_col, as_index=False, observed=True)["bound"].agg(
            agg
        )

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        path_col = self.path_col or schema.path_col
        if path_col not in schema.path_cols:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"path_col '{path_col}' must be one of schema.path_cols: {schema.path_cols}.",
            )

        path_col_q = engine.quote_ident(path_col)
        index_col_q = engine.quote_ident(schema.index)
        timestamp_col_q = engine.quote_ident(schema.timestamp_col)
        subindex_col_q = engine.quote_ident(schema.subindex)

        start = self._bounds(df, schema, self.start_specs, "start", path_col)

        # The end anchor may not land before the window opened, so a path whose
        # only end event precedes its start event is dropped rather than
        # yielding an inverted window. The floor is passed into the matcher
        # instead of being applied by truncating the frame first: the end
        # anchor's *pattern* still has the whole path to match against, so a
        # pattern straddling the window's start (`"catalog->.*->cart"` anchored
        # on the cart) resolves to the occurrence the window opened on, not to a
        # later one whose lead-in happens to fall inside the window.
        end = self._bounds(
            df, schema, self.end_specs, "end", path_col, not_before=start
        )

        if start.empty or end.empty:
            result = df.iloc[0:0].copy()
        else:
            bounds = start.merge(end, on=path_col, suffixes=("_start", "_end"))
            # path_cols is validated (coarsest-first, strictly nested) at
            # Eventstream construction time, and path_col is restricted to
            # schema.path_cols above, so comparing schema.index directly is
            # correct at any accepted grain (see ADR-0004).
            result = engine.run(
                f"""
                SELECT df.*
                FROM df
                JOIN bounds b ON df.{path_col_q} = b.{path_col_q}
                WHERE df.{index_col_q} BETWEEN b.bound_start AND b.bound_end
                ORDER BY df.{path_col_q}, df.{timestamp_col_q}, df.{subindex_col_q}
                """,
                df=df,
                bounds=bounds,
            )

        # Restore categorical dtypes and remove unused categories
        for col in schema.event_cols + schema.segment_cols:
            if col in result.columns:
                result[col] = result[col].astype("category")
                result[col] = result[col].cat.remove_unused_categories()
                result[col] = result[col].cat.as_unordered()

        return result, schema
