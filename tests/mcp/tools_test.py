"""Tests for the free-function MCP tools in `retentioneering.mcp.tools`.

Exercises `tools.*` directly against a `ReportSession` — the contract a
non-MCP caller (e.g. the platform's Anthropic-SDK tool runner) relies on.
Complements `server_test.py`, which only covers the pure `_agent_logic`
helpers; nothing previously exercised `ReportSession`/tool-function wiring.
"""

import json

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.mcp import tools
from retentioneering.mcp._report_session import ReportSession


def get_stream() -> Eventstream:
    df = pd.DataFrame(
        [
            ["user_1", "view", "2020-01-01 00:00:00"],
            ["user_1", "noise", "2020-01-02 00:00:00"],
            ["user_1", "purchase", "2020-01-03 00:00:00"],
            ["user_2", "view", "2020-01-01 00:00:00"],
            ["user_2", "noise", "2020-01-02 00:00:00"],
            ["user_2", "purchase", "2020-01-03 00:00:00"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    return Eventstream(df)


def get_session(context: dict | None = None) -> ReportSession:
    return ReportSession(get_stream(), context or {})


class TestDescribe:
    def test__returns_schema_and_events(self) -> None:
        session = get_session()

        result = tools.describe(session, {})

        assert result["event_col"] == "event"
        assert result["path_col"] == "user_id"
        assert set(result["events"]) >= {"view", "noise", "purchase"}

    def test__includes_context_descriptions(self) -> None:
        session = get_session()

        result = tools.describe(session, {"events": {"purchase": "Completed purchase"}})

        assert result["event_descriptions"] == {"purchase": "Completed purchase"}


class TestUpdateAndResetBaseStream:
    def test__update_base_stream_filters_events(self) -> None:
        session = get_session()

        result = tools.update_base_stream(
            session, [{"type": "filter_events", "drop": {"event": ["noise"]}}]
        )

        assert result["status"] == "base stream updated"
        assert "noise" not in result["events"]
        assert set(session.active_stream.df["event"].astype(str)) == {
            "view",
            "purchase",
        }

    def test__update_base_stream_never_stacks(self) -> None:
        session = get_session()
        tools.update_base_stream(
            session, [{"type": "filter_events", "drop": {"event": ["noise"]}}]
        )

        # Calling again with a different filter always replays from the
        # ORIGINAL stream, not the currently active one.
        result = tools.update_base_stream(
            session, [{"type": "filter_events", "keep": {"event": ["view"]}}]
        )

        assert result["events"] == ["view"]

    def test__reset_base_stream_restores_original(self) -> None:
        session = get_session()
        tools.update_base_stream(
            session, [{"type": "filter_events", "drop": {"event": ["noise"]}}]
        )

        result = tools.reset_base_stream(session)

        assert result["status"] == "base stream reset to original"
        assert "noise" in result["events"]
        assert session.base_preprocessors == []


class TestLoadDataAndDataAgnosticMode:
    def test__tools_error_before_any_data_is_loaded(self) -> None:
        session = ReportSession(None, None)

        assert "error" in tools.describe(session, session.context)
        assert "error" in tools.update_base_stream(session, [])
        assert "error" in tools.reset_base_stream(session)
        assert "error" in tools.add_transition_graph(session, label="Flow")
        assert "error" in tools.add_step_matrix(session, label="Steps")
        assert "error" in tools.add_segment_overview(
            session, label="Segments", segment_col="platform"
        )
        assert "error" in tools.get_conversion_rate(session, "view", "purchase")

    def test__load_data_populates_an_empty_session(self, tmp_path) -> None:
        session = ReportSession(None, None)
        csv_path = tmp_path / "events.csv"
        get_stream().df.to_csv(csv_path, index=False)

        result = tools.load_data(session, str(csv_path))

        assert result["status"] == "data loaded"
        assert set(result["events"]) >= {"view", "noise", "purchase"}
        assert session.active_stream is not None
        assert "error" not in tools.describe(session, session.context)

    def test__load_data_sets_context(self, tmp_path) -> None:
        session = ReportSession(None, None)
        csv_path = tmp_path / "events.csv"
        get_stream().df.to_csv(csv_path, index=False)

        tools.load_data(session, str(csv_path), context={"description": "test biz"})

        assert session.context == {"description": "test biz"}

    def test__load_data_replaces_an_existing_stream_and_clears_tabs(self) -> None:
        session = get_session()
        tools.add_transition_graph(session, label="Overall Flow")
        assert session.pending_tabs

        result = tools.load_data(session, __file__)  # any unreadable-as-CSV path

        assert "error" in result
        # Failed load must not have touched the previous stream/tabs.
        assert session.pending_tabs

    def test__load_data_error_on_bad_path(self) -> None:
        session = ReportSession(None, None)

        result = tools.load_data(session, "/no/such/file.csv")

        assert "error" in result


class TestPlaybookAndDescribeTool:
    def test__playbook_empty_returns_index(self) -> None:
        result = tools.playbook("")

        assert "scenarios" in result
        assert isinstance(result["scenarios"], list)

    def test__playbook_known_scenario_returns_text(self) -> None:
        index = tools.playbook("")
        scenario = index["scenarios"][0]

        result = tools.playbook(scenario)

        assert isinstance(result, str)
        assert result

    def test__describe_tool_empty_returns_index(self) -> None:
        result = tools.describe_tool("")

        assert "preprocessors" in result
        assert "filter_paths" in result["preprocessors"]

    def test__describe_tool_known_preprocessor_returns_docs(self) -> None:
        result = tools.describe_tool("filter_paths")

        assert result["preprocessor"] == "filter_paths"
        assert result["docs"]

    def test__describe_tool_unknown_returns_error(self) -> None:
        result = tools.describe_tool("not_a_real_tool")

        assert "error" in result


class TestCheckAnalysis:
    def test__ok_when_no_unlinked_numbers(self) -> None:
        result = tools.check_analysis("Everything here is `2.2×` and exempt.")

        assert result["status"] == "ok"

    def test__needs_fixes_when_unlinked_percentage(self) -> None:
        result = tools.check_analysis("Conversion dropped 38% with no link at all.")

        assert result["status"] == "needs_fixes"
        assert result["issues"]

    def test__needs_fixes_when_one_link_shares_line_with_two_numbers(self) -> None:
        # A link near 38% must not also validate the unrelated, unlinked 999%
        # just because both sit within the 200-char proximity window.
        result = tools.check_analysis(
            "Real conversion is 38% [Flow:purchase], invented value is 999%."
        )

        assert result["status"] == "needs_fixes"
        assert any(issue["number"] == "999%" for issue in result["issues"])
        assert all(issue["number"] != "38%" for issue in result["issues"])


class TestGetConversionRate:
    def test__returns_rows_with_denominator_and_baseline(self) -> None:
        session = get_session()

        result = tools.get_conversion_rate(session, "view", "purchase")

        assert result["path_col"] == "user_id"
        assert result["rows"] == [
            {
                "start_event": "view",
                "end_event": "purchase",
                "paths_with_start": 2,
                "converted": 2,
                "conversion_rate": 1.0,
                "base_rate": 1.0,
                "lift": 1.0,
            }
        ]

    def test__is_json_serializable(self) -> None:
        # The server adapter json.dumps() whatever this returns, so numpy
        # scalars or a bare NaN here would surface as a broken tool response.
        session = get_session()

        result = tools.get_conversion_rate(session, "purchase", "view")

        payload = json.dumps(result, ensure_ascii=False, allow_nan=False)
        assert '"conversion_rate": 0.0' in payload

    def test__undefined_rate_serializes_as_null(self) -> None:
        session = get_session()

        result = tools.get_conversion_rate(session, "purchase->.*->noise", "view")

        assert result["rows"][0]["paths_with_start"] == 0
        assert result["rows"][0]["conversion_rate"] is None
        json.dumps(result, allow_nan=False)

    def test__end_event_list_fans_out(self) -> None:
        session = get_session()

        result = tools.get_conversion_rate(session, "view", ["purchase", "noise"])

        assert [row["end_event"] for row in result["rows"]] == ["purchase", "noise"]

    def test__within_and_local_preprocessors_are_applied(self) -> None:
        session = get_session()

        # noise sits between view and purchase, so purchase is 2 events away
        # until the preprocessor removes it.
        assert (
            tools.get_conversion_rate(session, "view", "purchase", within=1)["rows"][0][
                "converted"
            ]
            == 0
        )
        filtered = tools.get_conversion_rate(
            session,
            "view",
            "purchase",
            within=1,
            local_preprocessors=[
                {"type": "filter_events", "drop": {"event": ["noise"]}}
            ],
        )
        assert filtered["rows"][0]["converted"] == 2
        # Local preprocessing must not leak into the session's base stream.
        assert "noise" in set(session.active_stream.df["event"].astype(str))

    def test__unknown_event_returns_an_error_not_an_exception(self) -> None:
        session = get_session()

        result = tools.get_conversion_rate(session, "Purchse", "view")

        assert "error" in result
        assert "start_event" in result["error"]

    def test__is_documented_as_a_method_not_a_preprocessor(self) -> None:
        index = tools.describe_tool("")
        assert "get_conversion_rate" in index["analysis_tools"]

        docs = tools.describe_tool("get_conversion_rate")
        assert docs["method"] == "get_conversion_rate"
        assert "conversion_rate" in docs["docs"]


class TestAddWidgetsAndExportReport:
    def test__export_report_without_tabs_is_an_error(self) -> None:
        session = get_session()

        result = tools.export_report(session, title="Empty")

        assert "error" in result

    def test__add_transition_graph_registers_a_tab(self) -> None:
        session = get_session()

        result = tools.add_transition_graph(session, label="Overall Flow")

        assert result["label"] == "Overall Flow"
        assert result["tab_id"] == "tab-0"
        assert len(session.pending_tabs) == 1
        assert session.pending_tabs[0]["data"]["widget_type"] == "transition_graph"

    def test__export_report_packages_and_clears_pending_tabs(self) -> None:
        session = get_session()
        tools.add_transition_graph(session, label="Overall Flow")

        result = tools.export_report(
            session, title="My Report", analysis="Some `analysis` text."
        )

        assert result["title"] == "My Report"
        assert result["analysis"] == "Some `analysis` text."
        assert len(result["tabs"]) == 1
        assert result["tabs"][0]["label"] == "Overall Flow"
        assert session.pending_tabs == []
