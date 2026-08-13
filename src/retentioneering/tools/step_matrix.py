from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING, Tuple

import pandas as pd

from retentioneering import engine
from retentioneering.paths import anchors
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import EmptyEventstreamError, InvalidParameterError
from .types import T_Diff

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream


from retentioneering.exceptions import PatternNoMatchError  # noqa: F401 — re-exported for backwards compat


@dataclass
class StepMatrix:
    eventstream: "Eventstream"

    def fit(
        self,
        max_steps: int = 10,
        diff: T_Diff = None,
        path_col: str | None = None,
        path_pattern: str | None = None,
        anchor: str | dict | None = None,
    ) -> Tuple[pd.DataFrame, ...]:
        path_col = path_col or self.eventstream.schema.path_col

        if self.eventstream.is_empty():
            raise EmptyEventstreamError(
                "Cannot calculate step matrix for empty eventstream"
            )

        if path_col not in self.eventstream.schema.path_cols:
            raise InvalidParameterError(
                "path_col", path_col, self.eventstream.schema.path_cols
            )

        if anchor is not None and path_pattern is not None:
            raise InvalidParameterError(
                "anchor",
                "given together with path_pattern",
                ["anchor (one centred block)", "path_pattern (one block per part)"],
            )

        if anchor is not None:
            if diff is None:
                return tuple(
                    self._process_anchor_matrix(max_steps, None, anchor, path_col)
                )
            sms, sms1, sms2 = self._process_anchor_matrix(
                max_steps, diff, anchor, path_col
            )
            return tuple(sms), tuple(sms1), tuple(sms2)

        if path_pattern is None:
            if diff is None:
                sm = self._regular(max_steps, path_col)
                return (sm,)
            else:
                sms, sms1, sms2 = self._process_diff_matrix(max_steps, diff, path_col)
                return tuple(sms), tuple(sms1), tuple(sms2)
        else:
            path_pattern = self._normalize_path_pattern(path_pattern)
            event_col = self.eventstream.schema.event_col
            available_events = set(self.eventstream.df[event_col].unique().tolist())
            self._validate_path_pattern_tokens(path_pattern, available_events)
            if diff is None:
                sms = self._process_pattern_matrix(
                    max_steps, None, path_pattern, path_col
                )
                return tuple(sms)
            else:
                sms, sms1, sms2 = self._process_pattern_matrix(
                    max_steps, diff, path_pattern, path_col
                )
                return tuple(sms), tuple(sms1), tuple(sms2)

    @staticmethod
    def _normalize_path_pattern(path_pattern: str) -> str:
        """Strip a redundant leading ".*->" and/or trailing "->.*" (see `paths.anchors`)."""
        return anchors.normalize_pattern(
            path_pattern, stacklevel=4, param="path_pattern"
        )

    @staticmethod
    def _validate_path_pattern_tokens(path_pattern: str, available_events: set) -> None:
        """Guards against typos in path_pattern: a mistyped event name would
        otherwise silently produce zero matches (surfaced only as the generic
        PatternNoMatchError, indistinguishable from a legitimately-empty result)."""
        anchors.validate_pattern_tokens(
            path_pattern, available_events, param="path_pattern"
        )

    @staticmethod
    def _align_matrices(sms1, sms2):
        path_start = EventTypes().PATH_START.name
        path_end = EventTypes().PATH_END.name
        indices = [sm.index for sm in (sms1 + sms2)]
        index = reduce(lambda a, b: a.union(b), indices)
        index = (
            [path_start]
            + index.drop([path_start, path_end], errors="ignore").tolist()
            + [path_end]
        )
        aligned1, aligned2 = [], []
        for i in range(len(sms1)):
            cols = sms1[i].columns.union(sms2[i].columns)
            aligned1.append(sms1[i].reindex(index=index, columns=cols).fillna(0))
            aligned2.append(sms2[i].reindex(index=index, columns=cols).fillna(0))
        return aligned1, aligned2

    def _process_diff_matrix(self, max_steps, diff, path_col):
        stream1, stream2 = self.eventstream._split_two(diff, path_col=path_col)
        sms1 = StepMatrix(stream1).fit(max_steps=max_steps, path_col=path_col)
        sms2 = StepMatrix(stream2).fit(max_steps=max_steps, path_col=path_col)
        sms1, sms2 = self._align_matrices(list(sms1), list(sms2))
        sms = [sms1[i] - sms2[i] for i in range(len(sms1))]
        return sms, sms1, sms2

    def _regular(self, max_steps: int, path_col: str) -> pd.DataFrame:
        event_col = self.eventstream.schema.event_col
        index_col = self.eventstream.schema.index
        subindex_col = self.eventstream.schema.subindex
        path_start = EventTypes().PATH_START.name
        path_end = EventTypes().PATH_END.name

        df = self.eventstream.df
        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        index_col_q = engine.quote_ident(index_col)
        subindex_col_q = engine.quote_ident(subindex_col)

        # path_cols is validated (coarsest-first, strictly nested) at Eventstream
        # construction time, and fit() above restricts path_col to
        # schema.path_cols, so ordering by index_col is correct at any accepted
        # grain (see ADR-0004).
        query = f"""
            select step, {event_col_q}, count(*) as value
            from (
                select {path_col_q}, {event_col_q},
                    row_number() over (
                        partition by {path_col_q}
                        order by {index_col_q}, {subindex_col_q}
                    ) as step
                from df
            )
            where step <= {max_steps}
            group by step, {event_col_q}
            order by step, {event_col_q}
        """
        sm = engine.run(query, df=df).pivot_table(
            index=event_col, columns="step", values="value", observed=False
        )

        sm = sm.reindex(columns=range(max_steps + 1)).fillna(0)
        total_paths = int(sm[1].sum())
        sm.loc[path_start, 0] = total_paths
        sm.loc[path_start, 1:] = 0
        sm.loc[path_end, :] = pd.Series(total_paths, index=sm.columns) - sm.sum()

        event_order = (
            [path_start]
            + sm.index.drop([path_start, path_end], errors="ignore").tolist()
            + [path_end]
        )
        sm = sm.loc[event_order, :]
        sm /= total_paths
        return sm

    # ── pattern matrix ───────────────────────────────────────────────────────

    def _resolve_pattern(self, path_pattern: str, path_col: str):
        """Locate the pattern once, and narrow the stream to the paths it matched.

        One resolution answers both questions this needs: which paths to draw,
        and where each of the pattern's parts sits in them. They must come from
        the *same* match — resolving a prefix per block instead would anchor
        each block on a different occurrence than the pattern names (a suffix
        cannot move the anchor it is not part of).

        Steps are numbered within a path, so positions found before filtering
        stay valid after it.
        """
        stream = self.eventstream.add_start_end_events(path_col=path_col)
        match = anchors.resolve_anchors(
            stream.df, stream.schema, path_pattern, path_col=path_col
        )
        matching_ids = match.paths().tolist()

        if not matching_ids:
            raise PatternNoMatchError(path_pattern)

        return self.eventstream.filter_events(keep={path_col: matching_ids}), match

    def _stepped(self, stream, path_col):
        """The stream's frame with a 1-based `step` per path."""
        path_col_q = engine.quote_ident(path_col)
        index_col_q = engine.quote_ident(self.eventstream.schema.index)
        subindex_col_q = engine.quote_ident(self.eventstream.schema.subindex)
        return engine.run(
            f"""
            SELECT *, row_number() OVER (
                PARTITION BY {path_col_q} ORDER BY {index_col_q}, {subindex_col_q}
            ) AS step
            FROM df
            """,
            df=stream.df,
        )

    def _centred_block(self, df, centres, max_steps, steps_right, path_col):
        """One block, laid out around a per-path centre step.

        `centres` is a Series of centre steps indexed by path; a path missing
        from it has no centre and so contributes nothing.
        """
        event_col = self.eventstream.schema.event_col
        centred = (
            df.merge(centres.rename("center").to_frame(), how="left", on=[path_col])
            .assign(step_centered=lambda _df: _df["step"] - _df["center"])
            .drop("center", axis=1)
        )
        sm = (
            centred.groupby("step_centered")[event_col]
            .value_counts()
            .unstack(level=0)
            .fillna(0)
        )
        sm = sm.reindex(columns=range(-max_steps, steps_right + 1)).fillna(0)
        sm.columns.name = "step"
        return sm

    def _finalize_block(self, sm, start_anchored):
        """Synthesize the boundary rows, normalize to shares, order the rows.

        Outside the block's own columns a path is at neither an event nor
        nothing: it is before its start or past its end, which is what the
        `path_start` / `path_end` rows carry.
        """
        event_col = self.eventstream.schema.event_col
        path_start = EventTypes().PATH_START.name
        path_end = EventTypes().PATH_END.name

        if start_anchored:
            sm.loc[path_end] = sm.loc[path_end].cumsum()
        else:
            total_paths = sm[0].sum()
            totals = sm.drop(index=[path_start, path_end], errors="ignore").sum()
            sm.loc[path_start, :] = (
                pd.Series(total_paths, index=sm.columns[sm.columns < 0]) - totals
            )
            sm.loc[path_end, :] = (
                pd.Series(total_paths, index=sm.columns[sm.columns >= 0]) - totals
            )
            sm = sm.fillna(0)

        sm = sm / sm.sum()
        rows_order = (
            [path_start] + sm.index.drop([path_start, path_end]).tolist() + [path_end]
        )
        sm = sm.loc[rows_order]
        sm.index = pd.Index(sm.index.tolist(), name=event_col)
        return sm

    def _process_anchor_matrix(self, max_steps, diff, anchor, path_col):
        """One block, centred where a single anchor spec resolves.

        `path_pattern` answers "lay the pattern's parts out side by side"; this
        answers "put me at this position". The spec reaches what a pattern
        cannot say — which occurrence, and an offset in events or in time — and
        selection falls out of it: a path where the anchor does not resolve has
        no centre, so it is not in the matrix.
        """
        path_col = path_col or self.eventstream.schema.path_col
        path_end = EventTypes().PATH_END.name
        spec = self._parse_anchor(anchor)

        if diff is not None:
            stream1, stream2 = self.eventstream._split_two(diff, path_col=path_col)
            kwargs = dict(max_steps=max_steps, anchor=anchor, path_col=path_col)
            try:
                sms1 = stream1.step_sankey_data(**kwargs)
            except PatternNoMatchError:
                raise PatternNoMatchError(spec.pattern, group="the first diff group")
            try:
                sms2 = stream2.step_sankey_data(**kwargs)
            except PatternNoMatchError:
                raise PatternNoMatchError(spec.pattern, group="the second diff group")
            new_sms1, new_sms2 = self._align_matrices(list(sms1), list(sms2))
            sms = [new_sms1[i] - new_sms2[i] for i in range(len(new_sms1))]
            return sms, new_sms1, new_sms2

        event_col = self.eventstream.schema.event_col
        anchors.validate_pattern_tokens(
            spec.pattern,
            set(self.eventstream.df[event_col].unique().tolist()),
            param="anchor",
        )
        positions = anchors.resolve_positions(
            self.eventstream.add_start_end_events(path_col=path_col).df,
            self.eventstream.schema,
            spec,
            path_col=path_col,
        )
        if positions.empty:
            raise PatternNoMatchError(spec.pattern)

        # Narrow to the paths the anchor resolved for, as the pattern mode does:
        # a path with no centre contributes nothing, and leaving it in would keep
        # its events as all-zero rows. Steps are numbered within a path, so the
        # positions stay valid across the filter.
        centres = positions.set_index(path_col)["step"]
        stream = self.eventstream.filter_events(
            keep={path_col: centres.index.tolist()}
        ).add_start_end_events(path_col=path_col)
        # An anchor sitting on `path_end` has nothing to its right, exactly as a
        # pattern part ending there does.
        steps_right = 0 if self._anchor_token(spec) == path_end else max_steps
        sm = self._centred_block(
            self._stepped(stream, path_col), centres, max_steps, steps_right, path_col
        )
        return [self._finalize_block(sm, start_anchored=False)]

    @staticmethod
    def _parse_anchor(anchor):
        """Normalize the spec, refusing the one occurrence mode a matrix cannot
        render: `"all"` gives several positions per path, and a path counted
        once per occurrence is no longer the unit the shares are taken over."""
        spec = anchors.parse_spec(anchor, param="anchor")
        if spec.occurrence == "all":
            raise InvalidParameterError(
                "anchor",
                "occurrence='all'",
                ["first", "last"],
            )
        return spec

    @staticmethod
    def _anchor_token(spec) -> str:
        """The event name the spec anchors on."""
        tokens = anchors.literal_tokens(spec.pattern)
        ordinal = spec.ordinal()
        return tokens[ordinal]

    def _process_pattern_matrix(self, max_steps, diff, path_pattern, path_col):
        from retentioneering.exceptions import EmptyEventstreamError as _Empty

        path_col = path_col or self.eventstream.schema.path_col
        event_col = self.eventstream.schema.event_col
        path_start = EventTypes().PATH_START.name
        path_end = EventTypes().PATH_END.name

        try:
            stream, match = self._resolve_pattern(path_pattern, path_col)
            stream = stream.add_start_end_events(path_col=path_col)
        except PatternNoMatchError:
            raise
        except _Empty:
            raise PatternNoMatchError(path_pattern)

        # Split on the pattern's own structure rather than on the literal string
        # "->.*->": a gap may be restricted ("->[^X]*->"), and a string split
        # would read `A->[^X]*->B` as three strictly adjacent tokens.
        parts = anchors.split_parts(path_pattern)

        if diff is None:
            sms = []
            df = self._stepped(stream, path_col)

            for i, pattern_part in enumerate(parts):
                is_start_anchored = (i == 0) and (pattern_part[0] == path_start)

                if is_start_anchored:
                    sm = (
                        df.groupby("step")[event_col]
                        .value_counts()
                        .unstack(level=0)
                        .fillna(0)
                    )
                    steps = len(pattern_part) + max_steps
                    sm = sm[[col for col in sm.columns if col <= steps]]
                    sm.columns = pd.Index(range(len(sm.columns)), name="step")
                    if len(sm.columns) < max_steps + 1:
                        sm = sm.reindex(columns=range(max_steps + 1)).fillna(0)
                else:
                    # The block's centre is where its part *begins* — the part is
                    # laid out to the right of it (see steps_right below), so the
                    # part's first token, not the pattern's last one. Read off the
                    # single whole-pattern match, so every block anchors on the
                    # same one.
                    centres = match.at_part(i).set_index(path_col)["step"]
                    steps_right = (
                        0
                        if pattern_part[-1] == path_end
                        else len(pattern_part) + max_steps - 1
                    )
                    sm = self._centred_block(
                        df, centres, max_steps, steps_right, path_col
                    )

                sms.append(self._finalize_block(sm, is_start_anchored))

            return sms

        else:
            stream1, stream2 = self.eventstream._split_two(diff, path_col=path_col)
            kwargs = dict(
                max_steps=max_steps,
                path_pattern=path_pattern,
                path_col=path_col,
            )
            try:
                sms1 = stream1.step_sankey_data(**kwargs)
            except PatternNoMatchError:
                raise PatternNoMatchError(path_pattern, group="the first diff group")
            try:
                sms2 = stream2.step_sankey_data(**kwargs)
            except PatternNoMatchError:
                raise PatternNoMatchError(path_pattern, group="the second diff group")

            new_sms1, new_sms2 = self._align_matrices(list(sms1), list(sms2))
            sms = [new_sms1[i] - new_sms2[i] for i in range(len(new_sms1))]
            return sms, new_sms1, new_sms2
