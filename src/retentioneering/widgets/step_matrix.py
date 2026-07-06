import json
import pathlib

import anywidget
import traitlets

from retentioneering.widgets._esm import _get_esm
from retentioneering.widgets._utils import parse_diff as _parse_diff
from retentioneering.widgets._html_export import write_html

_STATIC = pathlib.Path(__file__).parent.parent / "static"
_UNSET = object()


class StepMatrixWidget(anywidget.AnyWidget):
    _esm = _get_esm()
    _css = _STATIC / "widget.css"

    widget_type = traitlets.Unicode("step_matrix").tag(sync=True)

    # ── recompute triggers ─────────────────────────────────────────────────────
    max_steps = traitlets.Int(10).tag(sync=True)
    diff = traitlets.Unicode("").tag(sync=True)
    path_col = traitlets.Unicode("").tag(sync=True)
    path_pattern = traitlets.Unicode("").tag(sync=True)

    # ── catalogues ─────────────────────────────────────────────────────────────
    event_list = traitlets.Unicode("[]").tag(sync=True)
    path_cols = traitlets.Unicode("[]").tag(sync=True)
    segment_levels = traitlets.Unicode("{}").tag(sync=True)

    # ── result ─────────────────────────────────────────────────────────────────
    result = traitlets.Unicode("{}").tag(sync=True)
    is_loading = traitlets.Bool(False).tag(sync=True)
    error = traitlets.Unicode("").tag(sync=True)

    # ── display ────────────────────────────────────────────────────────────────
    height = traitlets.Int(600).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)

    def __init__(
        self,
        eventstream,
        max_steps=_UNSET,
        diff=_UNSET,
        path_col=_UNSET,
        path_pattern=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False
        self.widget_type = "step_matrix"

        try:
            all_events = sorted(
                eventstream.df[eventstream.schema.event_col]
                .astype(str)
                .unique()
                .tolist()
            )
            self.event_list = json.dumps(all_events)
        except Exception:
            self.event_list = "[]"
        try:
            self.segment_levels = json.dumps(eventstream.get_segment_values())
        except Exception:
            self.segment_levels = "{}"
        self.path_cols = json.dumps(eventstream.schema.path_cols)

        self.max_steps = max_steps if max_steps is not _UNSET else 10
        _diff_val = diff if diff is not _UNSET else None
        self.diff = json.dumps(list(_diff_val)) if _diff_val else ""
        self.path_col = path_col if path_col is not _UNSET else ""
        self.path_pattern = path_pattern if path_pattern is not _UNSET else ""
        self.height = height if height is not _UNSET else 600
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True

        self._recompute()
        self._initialized = True

        self.observe(
            self._on_params_change,
            names=["max_steps", "diff", "path_col", "path_pattern"],
        )

    # ── widget-specific observer ───────────────────────────────────────────────

    def _on_params_change(self, _change):
        if not self._initialized:
            return
        self._recompute()

    # ── computation ────────────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            result = self._compute_raw(
                max_steps=self.max_steps,
                path_col=self.path_col or None,
                diff=_parse_diff(self.diff),
                path_pattern=self.path_pattern or None,
            )
            self.result = json.dumps(result)
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False

    def _compute_raw(
        self, max_steps, path_col=None, diff=None, path_pattern=None
    ) -> dict:
        raw = self._eventstream.step_sankey_data(
            max_steps=max_steps,
            diff=diff,
            path_col=path_col,
            path_pattern=path_pattern,
        )
        if diff is not None:
            diff_sms, sms1, sms2 = raw
            matrices = [_df_to_matrix(sm) for sm in diff_sms]
            g1 = [_df_to_matrix(sm) for sm in sms1]
            g2 = [_df_to_matrix(sm) for sm in sms2]
            for i, m in enumerate(matrices):
                m["group1"] = g1[i]
                m["group2"] = g2[i]
        else:
            matrices = [_df_to_matrix(sm) for sm in raw]
            for m in matrices:
                m["group1"] = None
                m["group2"] = None

        try:
            pid = path_col or self._eventstream.schema.path_col
            ec = self._eventstream.schema.event_col
            import duckdb

            df = self._eventstream._df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
            event_counts = (
                duckdb.sql(
                    f"SELECT {ec}, COUNT(DISTINCT {pid}) AS cnt FROM df GROUP BY {ec}"
                )
                .df()
                .set_index(ec)["cnt"]
                .to_dict()
            )
            event_counts = {str(k): int(v) for k, v in event_counts.items()}
            total_paths = int(
                duckdb.sql(f"SELECT COUNT(DISTINCT {pid}) FROM df").fetchone()[0]
            )
            for s in ("path_start", "path_end"):
                if s not in event_counts:
                    event_counts[s] = total_paths
        except Exception:
            event_counts = {}

        event_counts_g1: dict = {}
        event_counts_g2: dict = {}
        if diff is not None:
            try:
                import duckdb as _duckdb

                _pid = path_col or self._eventstream.schema.path_col
                _ec = self._eventstream.schema.event_col
                _s1, _s2 = self._eventstream._split_two(diff, path_col=path_col)
                for _stream, _target in [(_s1, "g1"), (_s2, "g2")]:
                    _d = _stream._df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL strings below
                    _c = (
                        _duckdb.sql(
                            f"SELECT {_ec}, COUNT(DISTINCT {_pid}) AS cnt FROM _d GROUP BY {_ec}"
                        )
                        .df()
                        .set_index(_ec)["cnt"]
                        .to_dict()
                    )
                    _c = {str(k): int(v) for k, v in _c.items()}
                    _tot = int(
                        _duckdb.sql(
                            f"SELECT COUNT(DISTINCT {_pid}) FROM _d"
                        ).fetchone()[0]
                    )
                    for _s in ("path_start", "path_end"):
                        if _s not in _c:
                            _c[_s] = _tot
                    if _target == "g1":
                        event_counts_g1 = _c
                    else:
                        event_counts_g2 = _c
            except Exception:
                pass

        return {
            "matrices": matrices,
            "event_counts": event_counts,
            "event_counts_g1": event_counts_g1,
            "event_counts_g2": event_counts_g2,
        }

    # ── HTML export ───────────────────────────────────────────────────────────

    def export_html(
        self,
        path: str,
        title: str = "Step Matrix",
        analysis: str | None = None,
        sidebar_open: bool | None = None,
    ) -> None:
        """
        Export the step matrix as a standalone interactive HTML file.

        Parameters
        ----------
        path:
            Destination file path.
        title:
            Title shown in the browser tab.
        analysis:
            Optional analysis text. Supports basic markdown and [event] links.
        sidebar_open:
            Whether the settings sidebar starts open in the exported file.
            Defaults to the widget's current ``sidebar_open`` value.
        """
        data = {
            "widget_type": "step_matrix",
            "result": json.loads(self.result or "{}"),
            "max_steps": self.max_steps,
            "diff": json.loads(self.diff) if self.diff else None,
            "path_col": self.path_col or "",
            "path_pattern": self.path_pattern or "",
            "path_cols": json.loads(self.path_cols or "[]"),
            "segment_levels": json.loads(self.segment_levels or "{}"),
            "event_list": json.loads(self.event_list or "[]"),
            "height": self.height,
            "sidebar_open": sidebar_open
            if sidebar_open is not None
            else self.sidebar_open,
        }
        write_html(path, title, "Step Matrix", data, analysis)


def _df_to_matrix(df) -> dict:
    return {
        "events": df.index.tolist(),
        "values": df.values.tolist(),
        "columns": [int(c) for c in df.columns.tolist()],
    }
