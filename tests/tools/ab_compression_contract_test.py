"""The contracts of a compression plan (review of #224): what ``get_ab_compression``
predicts is what ``compress_events`` produces, every form of a plan means the same,
a plan applies where it was learned, segments are well defined, an empty window has
a defined result, and the widget checks the caller's arguments as the getter does.

- **Parity.** At every step of the path, each compressed path is the plan's own
  reading of the original one (labels, merged runs read as one event), and the
  compressed stream's full edge table is the one the plan counted. Where the
  processors would read a path otherwise (events that share a timestamp),
  ``compress_events`` refuses instead.
- **One plan, many forms.** A frame, its rows, a mapping, a recipe and the MCP
  preprocessor give the same stream; a self-mapped event is a member of its label;
  an event in two rows or a label that is another event of the stream raises.
- **Scope.** The MCP step keeps the plan's grain (``path_col``); a segment's plan has
  no ready step and is refused on a stream of several segments.
- **Segments.** ``by`` must be constant per path, its levels must differ as text,
  and some compared path must have one.
- **Empty window.** No event in the analysed window gives an empty plan with
  ``status="no_events_in_window"``, through Python, the widget and MCP.
- **Widget arguments.** An invalid argument raises at construction as in the
  getter, whatever sentinel its traitlet uses, and a saved state still restores.
"""

from __future__ import annotations

import json
import warnings

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError, PreprocessingConfigError
from retentioneering.tools.ab_compression import plan_mapping

TOY = ("arm", "a", "b")


def _stream(paths: list[list[str]], extra: dict | None = None) -> Eventstream:
    """One user per path, the arms alternating, one minute between events."""
    rows = []
    start = pd.Timestamp("2026-01-01")
    for user, events in enumerate(paths):
        for k, event in enumerate(events):
            row = {
                "user_id": f"u{user:04d}",
                "event": event,
                "timestamp": start + pd.Timedelta(minutes=user * 100 + k),
                "arm": "a" if user % 2 == 0 else "b",
            }
            for name, values in (extra or {}).items():
                row[name] = values[user]
            rows.append(row)
    return Eventstream(pd.DataFrame(rows), {"segment_cols": ["arm", *(extra or {})]})


def _paths() -> list[list[str]]:
    templates = (
        [["home", "catalog", "product", "cart", "checkout", "pay"]] * 300
        + [["home", "search", "product", "cart"]] * 200
        + [["home", "catalog", "X", "product", "cart"]] * 8
        + [["home", "Y", "cart"]] * 8
        + [["home", "search_alt", "product", "cart"]] * 8
        + [["home", "catalog", "product", "home", "search", "product"]] * 30
    )
    order = np.random.default_rng(3).permutation(len(templates))
    return [list(templates[i]) for i in order]


@pytest.fixture(scope="module")
def stream() -> Eventstream:
    return _stream(_paths())


def _sequences(stream: Eventstream) -> dict:
    sc = stream.schema
    df = stream.df.sort_values([sc.path_col, sc.index, sc.subindex], kind="stable")
    return {
        p: g[sc.event_col].astype(str).tolist()
        for p, g in df.groupby(sc.path_col, observed=True, sort=True)
    }


def _reading(stream: Eventstream, plan) -> dict:
    """The plan's own reading of every path, written out independently."""
    mapping, merged, _ = plan_mapping(plan)
    out = {}
    for path, events in _sequences(stream).items():
        seq: list[str] = []
        for event in events:
            label = mapping.get(event, event)
            if label is None:
                continue
            if seq and seq[-1] == label and label in merged:
                continue
            seq.append(label)
        out[path] = seq
    return out


def _edges(sequences: dict) -> pd.Series:
    """Unique paths per direct transition, with path_start / path_end as the
    stream's own boundary events are."""
    pairs = {
        (path, a, b)
        for path, seq in sequences.items()
        for a, b in zip(seq[:-1], seq[1:])
    }
    frame = pd.DataFrame(list(pairs), columns=["path", "source", "target"])
    return frame.groupby(["source", "target"]).size().sort_index()


# ======================================================================= parity
class TestParity:
    @pytest.mark.parametrize("screen", ["transitions", "steps"])
    @pytest.mark.parametrize("max_events", [None, 4])
    def test_every_step_every_path(self, stream, screen, max_events) -> None:
        path = stream.get_ab_compression(
            TOY, screen, max_events=max_events, min_support=5
        ).attrs["path"]
        for step in path["step"]:
            plan = stream.get_ab_compression(
                TOY, screen, max_events=max_events, min_support=5, step=int(step)
            )
            compact = stream.compress_events(plan)
            expected = _reading(stream, plan)
            assert _sequences(compact) == expected, step
            # the full edge table, every edge and its paths
            assert _edges(_sequences(compact)).equals(_edges(expected)), step
            row = plan.attrs["path"].set_index("step").loc[int(step)]
            if screen == "transitions":
                screen_frame = compact.get_ab_transitions(TOY, min_support=5)
                assert len(screen_frame) == row["cells"]

    def test_markers_on_tied_events_apply_exactly_or_refuse(self) -> None:
        # the review's case: two A at one timestamp, each with a marker M. Where
        # the processors read the tie in another order than the plan, the step
        # raises rather than return a stream compressed otherwise.
        rows = [
            (f"u{u}", e, "2026-01-01 00:00", "a" if u % 2 else "b")
            for u in range(40)
            for e in "ABAB"
        ]
        df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"])
        tied = Eventstream(df, {"segment_cols": ["arm"]}).add_events(
            name="M", source_event="A"
        )
        plan = tied.get_ab_compression(TOY, max_events=2, keep=["M"], min_support=2)
        try:
            compact = tied.compress_events(plan)
        except PreprocessingConfigError as exc:
            assert "does not apply exactly" in str(exc)
        else:
            assert _sequences(compact) == _reading(tied, plan)


# ======================================================================= forms
class TestForms:
    def test_frame_rows_mapping_recipe_and_mcp_agree(self, stream) -> None:
        from retentioneering.mcp import tools
        from retentioneering.mcp._report_session import ReportSession

        plan = stream.get_ab_compression(TOY, max_events=5)
        mapping, _, _ = plan_mapping(plan)
        by_frame = stream.compress_events(plan)
        rows = plan[["event", "action", "label"]].to_dict("records")
        assert stream.compress_events(rows).fingerprint == by_frame.fingerprint
        assert stream.compress_events(mapping).fingerprint == by_frame.fingerprint
        rebuilt = Eventstream.from_recipe(
            stream.to_dataframe(), by_frame.recipe(), {"segment_cols": ["arm"]}
        )
        assert rebuilt.fingerprint == by_frame.fingerprint
        session = ReportSession(stream, {})
        result = tools.get_ab_compression(session, list(TOY), max_events=5)
        step = result["segments"][0]["preprocessor"]
        assert step["path_col"] == "user_id"
        through = tools._apply_preprocessors(stream, [step])
        assert through.fingerprint == by_frame.fingerprint

    def test_a_self_mapped_event_is_a_member_of_its_label(self, stream) -> None:
        by_dict = stream.compress_events({"search_alt": "search", "search": "search"})
        rows = [
            {"event": "search_alt", "action": "merge", "label": "search"},
            {"event": "search", "action": "merge", "label": "search"},
        ]
        assert by_dict.fingerprint == stream.compress_events(rows).fingerprint
        _, merged, _ = plan_mapping({"search_alt": "search", "search": "search"})
        assert merged == ["search"]

    def test_an_event_in_two_rows_raises(self, stream) -> None:
        rows = [
            {"event": "X", "action": "drop", "label": None},
            {"event": "X", "action": "other", "label": "other"},
        ]
        with pytest.raises(InvalidParameterError, match="more than one row"):
            stream.compress_events(rows)

    def test_a_label_that_is_another_changed_event_raises(self, stream) -> None:
        with pytest.raises(InvalidParameterError, match="also an event"):
            stream.compress_events({"X": "Y", "Y": None})

    def test_a_label_that_is_an_unnamed_event_of_the_stream_raises(
        self, stream
    ) -> None:
        # renaming X into cart would merge it with every cart silently
        with pytest.raises(PreprocessingConfigError, match="cart"):
            stream.compress_events({"X": "cart"})


# ======================================================================= scope
def _sessions() -> Eventstream:
    """40 users with two sessions each, the arm per session (the randomised
    unit)."""
    rows = []
    start = pd.Timestamp("2026-01-01")
    for user in range(40):
        for session in range(2):
            # the chain ends one session and opens the next: at the user grain
            # its two runs would be one
            events = ["home", "a1", "a2"] if session == 0 else ["a1", "a2", "cart"]
            for k, event in enumerate(events):
                rows.append(
                    {
                        "user_id": f"u{user}",
                        "session_id": f"u{user}_{session}",
                        "event": event,
                        "timestamp": start
                        + pd.Timedelta(hours=user * 10 + session * 3, minutes=k),
                        "arm": "a" if (user + session) % 2 == 0 else "b",
                    }
                )
    return Eventstream(
        pd.DataFrame(rows),
        {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm"]},
    )


class TestScope:
    def test_the_mcp_step_keeps_the_plans_grain(self) -> None:
        from retentioneering.mcp import tools
        from retentioneering.mcp._report_session import ReportSession

        sessions = _sessions()
        session = ReportSession(sessions, {})
        # step 1 of the path merges the chain a1 -> a2
        result = tools.get_ab_compression(
            session, list(TOY), path_col="session_id", step=1
        )
        assert "error" not in result, result
        step = result["segments"][0]["preprocessor"]
        assert step["path_col"] == "session_id"
        assert {r["label"] for r in step["plan"]} == {"a1+a2"}
        compact = tools._apply_preprocessors(sessions, [step])
        merged = compact.df[compact.df["event"] == "a1+a2"]
        # one run per session: 80, not one per user
        assert len(merged) == 80 == merged["session_id"].nunique()
        # the same rows without the grain collapse each user's two runs
        bare = tools._apply_preprocessors(
            sessions, [{"type": "compress_events", "plan": step["plan"]}]
        )
        assert int((bare.df["event"] == "a1+a2").sum()) == 40

    def test_a_segment_plan_has_no_ready_mcp_step(self) -> None:
        from retentioneering.mcp import tools
        from retentioneering.mcp._report_session import ReportSession

        paths = _paths()
        tier = ["gold" if i % 3 == 0 else "basic" for i in range(len(paths))]
        segmented = _stream(paths, {"tier": tier})
        result = tools.get_ab_compression(
            ReportSession(segmented, {}),
            list(TOY),
            by="tier",
            by_pre_exposure=True,
        )
        assert "error" not in result, result
        for entry in result["segments"]:
            assert entry["preprocessor"] is None
            assert entry["scope"] == {"column": "tier", "value": entry["segment"]}
            assert "not a ready step" in entry["apply"]
            assert isinstance(entry["plan"], list)


# ======================================================================= segments
class TestSegments:
    def test_a_segment_that_changes_within_a_path_is_refused(self) -> None:
        paths = _paths()[:200]
        df = _stream(paths).to_dataframe()
        df["tier"] = np.where(np.arange(len(df)) % 2 == 0, "x", "y")
        changing = Eventstream(df, {"segment_cols": ["arm", "tier"]})
        for declared in (True, None):
            with pytest.raises(InvalidParameterError, match="changes within"):
                changing.get_ab_compression(TOY, by="tier", by_pre_exposure=declared)

    def test_levels_that_print_alike_are_refused(self) -> None:
        paths = _paths()[:200]
        tier = [1 if i < 100 else "1" for i in range(len(paths))]
        alike = _stream(paths, {"tier": tier})
        with pytest.raises(InvalidParameterError, match="print the same"):
            alike.get_ab_compression(TOY, by="tier", by_pre_exposure=True)

    def test_no_compared_path_in_a_segment(self) -> None:
        paths = _paths()[:200]
        empty = _stream(paths, {"tier": [None] * len(paths)})
        with pytest.raises(InvalidParameterError, match="none of the 200"):
            empty.get_ab_compression(TOY, by="tier", by_pre_exposure=True)


# ======================================================================= empty window
def _late_assignment() -> tuple[Eventstream, pd.DataFrame]:
    df = pd.DataFrame(
        [
            ("u1", "A", "2026-01-01 00:00", "a"),
            ("u1", "B", "2026-01-01 00:01", "a"),
            ("u2", "A", "2026-01-01 00:00", "b"),
            ("u2", "B", "2026-01-01 00:01", "b"),
        ],
        columns=["user_id", "event", "timestamp", "arm"],
    )
    stream = Eventstream(df, {"segment_cols": ["arm"]})
    assignment = pd.DataFrame(
        {"arm": ["a", "b"], "assigned_at": pd.to_datetime(["2026-02-01"] * 2)},
        index=pd.Index(["u1", "u2"], name="user_id"),
    )
    return stream, assignment


class TestEmptyWindow:
    @pytest.mark.parametrize("screen", ["transitions", "steps"])
    def test_an_empty_plan_with_its_status(self, screen) -> None:
        stream, assignment = _late_assignment()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            plan = stream.get_ab_compression(TOY, screen, assignment=assignment)
        assert any("analysed window" in str(w.message) for w in caught)
        assert len(plan) == 0 and "step" in plan.columns
        assert plan.attrs["status"] == "no_events_in_window"
        assert (plan.attrs["group1_paths"], plan.attrs["group2_paths"]) == (1, 1)
        assert stream.compress_events(plan).fingerprint == stream.fingerprint

    def test_the_widget_and_mcp_say_so(self) -> None:
        from retentioneering.mcp import tools
        from retentioneering.mcp._report_session import ReportSession

        stream, assignment = _late_assignment()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            widget = stream.ab_compression(TOY, assignment=assignment)
        payload = json.loads(widget.result)
        assert widget.error == "" and payload["status"] == "no_events_in_window"
        assert any("analysed window" in w for w in payload["warnings"])
        result = tools.get_ab_compression(
            ReportSession(stream, {}, assignment=assignment), list(TOY)
        )
        assert result["status"] == "no_events_in_window"
        (entry,) = result["segments"]
        assert entry["preprocessor"] is None and entry["reading"].startswith("no plan")

    def test_a_plan_with_events_is_ok(self, stream) -> None:
        assert stream.get_ab_compression(TOY).attrs["status"] == "ok"


# ======================================================================= widget
class TestWidgetArguments:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_events": 0},
            {"max_events": False},
            {"step": -2},
            {"target_delta": 0},
            {"screen": "steps", "max_steps": 0},
            {"screen": "steps", "max_steps": False},
        ],
    )
    def test_invalid_arguments_raise_as_in_the_getter(self, stream, kwargs) -> None:
        with pytest.raises(InvalidParameterError) as getter:
            stream.get_ab_compression(TOY, **kwargs)
        with pytest.raises(InvalidParameterError) as drawn:
            stream.ab_compression(TOY, **kwargs)
        assert str(drawn.value) == str(getter.value)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_events": 4},
            {"step": 0},
            {"target_delta": 0.05},
            {"screen": "steps", "max_steps": 4},
        ],
    )
    def test_valid_arguments_are_the_getters_plan(self, stream, kwargs) -> None:
        widget = stream.ab_compression(TOY, **kwargs)
        plan = stream.get_ab_compression(TOY, **kwargs)
        payload = json.loads(widget.result)
        assert payload["segments"][0]["chosen_step"] == plan.attrs["chosen_step"]
        assert widget.error == ""

    def test_a_saved_state_restores_its_sentinels(self, stream, tmp_path) -> None:
        path = tmp_path / "state.json"
        first = stream.ab_compression(TOY, state_file=str(path))
        first.max_events = 4
        first.max_events = 0  # the sidebar clears the cap
        second = stream.ab_compression(TOY, state_file=str(path))
        assert second.max_events == 0 and second._params()["max_events"] is None
        assert second.error == ""

    def test_the_export_line_names_the_plans_arguments(self, stream) -> None:
        widget = stream.ab_compression(
            TOY, "steps", max_steps=4, max_events=4, keep=["cart"]
        )
        args = json.loads(widget.result)["call_args"]
        assert args == "screen='steps', max_steps=4, max_events=4, keep=['cart']"
        kwargs = eval(f"dict({args})")  # noqa: S307 - the line the widget prints
        plan = stream.ab_compression_data(TOY, **kwargs)
        assert (
            plan.attrs["chosen_step"]
            == stream.get_ab_compression(
                TOY, "steps", max_steps=4, max_events=4, keep=["cart"]
            ).attrs["chosen_step"]
        )
