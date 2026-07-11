import re
import warnings
from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING, Tuple

import duckdb
import pandas as pd

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

        if path_pattern is None:
            if diff is None:
                sm = self._regular(max_steps, path_col)
                return (sm,)
            else:
                sms, sms1, sms2 = self._process_diff_matrix(max_steps, diff, path_col)
                return tuple(sms), tuple(sms1), tuple(sms2)
        else:
            path_pattern = self._normalize_path_pattern(path_pattern)
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
        """Strip a redundant leading ".*->" and/or trailing "->.*".

        A pattern already matches anywhere in the path by default (a bare
        "X" and ".*->X->.*" select the same paths), so boundary wildcards
        add nothing -- except a trailing ".*" also throws off
        `_find_center_position`'s anchor bookkeeping, shifting the anchor's
        own column off of 0. Trimming both forms up front keeps every
        pattern on the bare-literal path, which centers correctly.
        """
        normalized = path_pattern
        stripped_leading = normalized.startswith(".*->")
        if stripped_leading:
            normalized = normalized[len(".*->") :]
        stripped_trailing = normalized.endswith("->.*")
        if stripped_trailing:
            normalized = normalized[: -len("->.*")]

        if (stripped_leading or stripped_trailing) and normalized:
            warnings.warn(
                f"path_pattern {path_pattern!r} has a redundant leading/trailing "
                f"'.*' -- a pattern already matches anywhere in the path by "
                f"default. Using {normalized!r} instead.",
                UserWarning,
                stacklevel=3,
            )
            return normalized
        return path_pattern

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
        sms = [sms2[i] - sms1[i] for i in range(len(sms1))]
        return sms, sms1, sms2

    def _regular(self, max_steps: int, path_col: str) -> pd.DataFrame:
        event_col = self.eventstream.schema.event_col
        index_col = self.eventstream.schema.index
        subindex_col = self.eventstream.schema.subindex
        path_start = EventTypes().PATH_START.name
        path_end = EventTypes().PATH_END.name

        df = self.eventstream.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string

        # path_cols is validated (coarsest-first, strictly nested) at Eventstream
        # construction time, and fit() above restricts path_col to
        # schema.path_cols, so ordering by index_col is correct at any accepted
        # grain (see ADR-0004).
        query = f"""
            select step, {event_col}, count(*) as value
            from (
                select {path_col}, {event_col},
                    row_number() over (
                        partition by {path_col}
                        order by {index_col}, {subindex_col}
                    ) as step
                from df
            )
            where step <= {max_steps}
            group by step, {event_col}
            order by step, {event_col}
        """
        sm = (
            duckdb.sql(query)
            .df()
            .pivot_table(
                index=event_col, columns="step", values="value", observed=False
            )
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

    # ── pattern matrix (copied from be-app) ──────────────────────────────────

    @staticmethod
    def _find_center_position(sequence, pattern):
        seq = sequence.split("->")
        if ".*" not in pattern:
            pat = pattern.split("->")
            pat_len = len(pat)
            for i in range(len(seq) - pat_len + 1):
                if seq[i : i + pat_len] == pat:
                    return i + 1
            return None
        else:
            # Split by every occurrence of ".*" wildcard (including leading/trailing).
            # e.g. ".*->path_end"           → ["", "path_end"]
            #      "path_start->.*"          → ["path_start", ""]
            #      ".*->basket->.*->path_end" → ["", "basket", "path_end"]
            pattern_parts = re.split(r"(?:^|->)\.\*(?:->|$)", pattern)

            def find_non_greedy_match(seq, parts):
                idx = 0
                matched_indices = []
                for part in parts:
                    sub_pat = part.split("->") if part else []
                    found = False
                    for i in range(idx, len(seq) - len(sub_pat) + 1):
                        if seq[i : i + len(sub_pat)] == sub_pat:
                            matched_indices.append(i)
                            idx = i + len(sub_pat)
                            found = True
                            break
                    if not found:
                        return None
                return matched_indices

            result = find_non_greedy_match(seq, pattern_parts)
            if result is not None:
                last_index = result[-1]
                return last_index + 1
            return None

    def _filter_paths_by_pattern(
        self, path_pattern: str, path_col: str
    ) -> "Eventstream":
        """Filter eventstream to paths matching the given path_pattern."""
        path_start = EventTypes().PATH_START.name
        path_end = EventTypes().PATH_END.name
        event_col = self.eventstream.schema.event_col
        index_col = self.eventstream.schema.index
        subindex_col = self.eventstream.schema.subindex
        df = self.eventstream.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string

        # Build path sequences prefixed/suffixed with path_start/path_end so
        # that patterns including these markers match correctly. path_cols is
        # validated (coarsest-first, strictly nested) at Eventstream
        # construction time, and fit() restricts path_col to schema.path_cols,
        # so ordering by index_col is correct at any accepted grain (see
        # ADR-0004).
        query = f"""
            SELECT {path_col}, list_aggregate(list({event_col}), 'string_agg', '->') AS path
            FROM (SELECT * FROM df ORDER BY {index_col}, {subindex_col})
            GROUP BY {path_col}
        """
        paths = duckdb.sql(query).df().set_index(path_col)["path"]
        paths_with_se = path_start + "->" + paths + "->" + path_end

        matching_ids = paths_with_se[
            paths_with_se.apply(
                lambda p: self._find_center_position(p, path_pattern) is not None
            )
        ].index.tolist()

        if not matching_ids:
            raise PatternNoMatchError(path_pattern)

        return self.eventstream.filter_events(keep={path_col: matching_ids})

    def _process_pattern_matrix(self, max_steps, diff, path_pattern, path_col):
        from retentioneering.exceptions import EmptyEventstreamError as _Empty

        path_col = path_col or self.eventstream.schema.path_col
        index_col = self.eventstream.schema.index
        subindex_col = self.eventstream.schema.subindex
        event_col = self.eventstream.schema.event_col
        path_start = EventTypes().PATH_START.name
        path_end = EventTypes().PATH_END.name

        try:
            stream = self._filter_paths_by_pattern(
                path_pattern, path_col
            ).add_start_end_events(path_col=path_col)
        except PatternNoMatchError:
            raise
        except _Empty:
            raise PatternNoMatchError(path_pattern)

        original_pattern = path_pattern
        pattern_tokens = path_pattern.split("->")
        skip_first_matrix = pattern_tokens[0] != path_start
        if skip_first_matrix:
            if pattern_tokens[0] == ".*":
                path_pattern = f"{path_start}->{path_pattern}"
            else:
                path_pattern = f"{path_start}->.*->{path_pattern}"

        if diff is None:
            sms = []
            df = stream.df
            query = f"""
                SELECT *, row_number() OVER (
                    PARTITION BY {path_col} ORDER BY {index_col}, {subindex_col}
                ) AS step
                FROM df
            """
            df = duckdb.sql(query).df()

            query = f"""
                SELECT {path_col}, list_aggregate(list({event_col}), 'string_agg', '->') AS path
                FROM (SELECT * FROM df ORDER BY {path_col}, {index_col}, {subindex_col})
                GROUP BY {path_col}
            """
            paths = duckdb.sql(query).df().set_index(path_col)["path"]

            current_pattern = []
            for i, pattern_part in enumerate(path_pattern.split("->.*->")):
                current_pattern.append(pattern_part)
                current_pattern_str = "->.*->".join(current_pattern)

                first_token = pattern_part.split("->")[0] if pattern_part else ""
                is_start_anchored = (i == 0) and (first_token == path_start)

                if is_start_anchored:
                    groupby_col = "step"
                    df_centered = df.copy()
                else:
                    centers = (
                        paths.map(
                            lambda x: self._find_center_position(x, current_pattern_str)
                        )
                        .loc[lambda s: s.notnull()]
                        .to_frame("center")
                    )

                    df_centered = (
                        df.merge(centers, how="left", on=[path_col])
                        .assign(step_centered=lambda _df: _df["step"] - _df["center"])
                        .drop("center", axis=1)
                    )
                    groupby_col = "step_centered"

                sm = (
                    df_centered.groupby(groupby_col)[event_col]
                    .value_counts()
                    .unstack(level=0)
                    .fillna(0)
                )

                if is_start_anchored:
                    steps = len(pattern_part.split("->")) + max_steps
                    sm = sm[[col for col in sm.columns if col <= steps]]
                    sm.columns = pd.Index(range(len(sm.columns)), name="step")
                    if len(sm.columns) < max_steps + 1:
                        sm = sm.reindex(columns=range(max_steps + 1)).fillna(0)
                else:
                    steps_left = max_steps
                    steps_right = (
                        0
                        if pattern_part.endswith(path_end)
                        else len(pattern_part.split("->")) + max_steps - 1
                    )
                    sm = sm.reindex(columns=range(-steps_left, steps_right + 1)).fillna(
                        0
                    )
                    sm.columns.name = "step"

                if not is_start_anchored:
                    total_paths = sm[0].sum()
                    totals = sm.drop(
                        index=[path_start, path_end], errors="ignore"
                    ).sum()
                    sm.loc[path_start, :] = (
                        pd.Series(total_paths, index=sm.columns[sm.columns < 0])
                        - totals
                    )
                    sm.loc[path_end, :] = (
                        pd.Series(total_paths, index=sm.columns[sm.columns >= 0])
                        - totals
                    )
                    sm = sm.fillna(0)
                else:
                    sm.loc[path_end] = sm.loc[path_end].cumsum()

                sm = sm / sm.sum()

                rows_order = (
                    [path_start]
                    + sm.index.drop([path_start, path_end]).tolist()
                    + [path_end]
                )
                sm = sm.loc[rows_order]
                sm.index = pd.Index(sm.index.tolist(), name=event_col)
                sms.append(sm)

            if skip_first_matrix:
                sms = sms[1:]

            return sms

        else:
            stream1, stream2 = self.eventstream._split_two(diff, path_col=path_col)
            # Use original_pattern so skip_first_matrix logic applies correctly in each sub-call
            kwargs = dict(
                max_steps=max_steps,
                path_pattern=original_pattern,
                path_col=path_col,
            )
            try:
                sms1 = stream1.step_sankey_data(**kwargs)
            except PatternNoMatchError:
                raise PatternNoMatchError(
                    original_pattern, group="the first diff group"
                )
            try:
                sms2 = stream2.step_sankey_data(**kwargs)
            except PatternNoMatchError:
                raise PatternNoMatchError(
                    original_pattern, group="the second diff group"
                )

            new_sms1, new_sms2 = self._align_matrices(list(sms1), list(sms2))
            sms = [new_sms2[i] - new_sms1[i] for i in range(len(new_sms1))]
            return sms, new_sms1, new_sms2
