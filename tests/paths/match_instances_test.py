"""Regression tests for the PR207 audit: execution safety and analytical units."""

import itertools
import json
import re

import pandas as pd
import pytest

from retentioneering import Eventstream
from retentioneering.exceptions import (
    InvalidParameterError,
    PatternMatchLimitError,
    PreprocessingConfigError,
)
from retentioneering.paths import non_overlapping


def stream(events, sessions=None, times=None):
    return Eventstream(
        pd.DataFrame(
            {
                "user_id": [1] * len(events),
                "session_id": sessions or ["s"] * len(events),
                "event": events,
                "timestamp": times
                if times is not None
                else pd.date_range("2026-01-01", periods=len(events), freq="s"),
            }
        ),
        schema={"path_cols": ["user_id", "session_id"]},
    )


def mark(es, pattern="(B)+", at="end", **kwargs):
    return es.add_events(
        "episode",
        anchor={"pattern": pattern, "at": at, "occurrence": "non_overlapping"},
        path_col="session_id",
        **kwargs,
    )


def events(es):
    return es.to_dataframe().event.tolist()


@pytest.mark.parametrize("middle", [None, float("nan")])
@pytest.mark.parametrize("token", [".", "[^X]"])
def test_null_is_not_a_positional_wildcard(middle, token):
    es = stream(["A", middle, "B", "X"])
    pattern = f"A->{token}->B"
    assert es.find_matches(pattern, path_col="session_id").empty
    for mode in ["all", "non_overlapping"]:
        result = es.add_events(
            "m",
            anchor={"pattern": pattern, "at": "start", "occurrence": mode},
            path_col="session_id",
        )
        assert "m" not in events(result)


@pytest.mark.parametrize("gap", [".*", "[^X]*", "[X]*", ".{1,3}"])
def test_gaps_allow_missing_and_boundary_rows_like_sql(gap):
    es = stream(["A", None, "B", "X"], ["s1", "s1", "s2", "s2"])
    if gap != ".{1,3}":
        es = es.add_start_end_events(path_col="session_id")
    pattern = f"A->{gap}->B"
    for mode in ["all", "non_overlapping"]:
        result = es.add_events(
            "m",
            anchor={"pattern": pattern, "at": "start", "occurrence": mode},
            path_col="user_id",
        )
        assert events(result).count("m") == 1


def test_full_match_end_is_distinct_from_numeric_capture_for_plus():
    es = stream(["A", "B", "B", "B", "C", "B"])
    table = es.find_matches("(B)+", path_col="session_id")
    assert table[["start_index", "end_index"]].values.tolist() == [[2, 4], [6, 6]]
    complete = mark(es).to_dataframe().query("event == 'episode'")["index"].tolist()
    ordinal = (
        mark(es, at=-1).to_dataframe().query("event == 'episode'")["index"].tolist()
    )
    assert complete == [4, 6]
    assert ordinal == [2, 6]


def test_non_overlapping_requires_explicit_at_and_unambiguous_grain():
    es = stream(list("ABBB"))
    with pytest.raises(PreprocessingConfigError, match="at"):
        es.add_events(
            "m",
            anchor={"pattern": "(B)+", "occurrence": "non_overlapping"},
            path_col="session_id",
        )
    with pytest.raises(PreprocessingConfigError, match="path_col"):
        es.add_events(
            "m",
            anchor={"pattern": "(B)+", "at": "start", "occurrence": "non_overlapping"},
        )


@pytest.mark.parametrize("boundaries", [False, True])
def test_after_terminal_event_is_after_it_with_same_timestamp_ties(boundaries):
    es = stream(list("ABBB"), times=[pd.Timestamp("2026-01-01")] * 4)
    if boundaries:
        es = es.add_start_end_events(path_col="session_id")
    result = mark(es, placement="after")
    assert events(result) == ["A", "B", "B", "B", "episode"]
    df = result.df.sort_values(["index", "subindex"])
    assert df[df.event.eq("episode")]["index"].tolist() == [4]
    if boundaries:
        assert df.event.tolist()[-2:] == ["episode", "path_end"]


def test_out_of_path_offset_fails_instead_of_clamping():
    es = stream(list("ABBB"))
    for offset in [1, "1s", -8, "-8s"]:
        with pytest.raises(
            (InvalidParameterError, PreprocessingConfigError), match="offset"
        ):
            es.add_events(
                "m",
                anchor={
                    "pattern": "(B)+",
                    "at": "end",
                    "occurrence": "non_overlapping",
                    "offset": offset,
                },
                path_col="session_id",
            )


def test_marker_replacement_is_explicit_and_preserves_raw_events_and_recipe():
    es = stream(list("ABBBCB"))
    once = mark(es, placement="after")
    with pytest.raises(PreprocessingConfigError, match="already exists"):
        mark(once, placement="after")
    twice = mark(once, placement="after", on_existing="replace")
    pd.testing.assert_frame_equal(once.df, twice.df)
    recipe = json.loads(json.dumps(twice.recipe()))
    replay = Eventstream.from_recipe(
        es.to_dataframe(), recipe, schema={"path_cols": ["user_id", "session_id"]}
    )
    pd.testing.assert_frame_equal(twice.df, replay.df)
    assert (
        events(mark(once, placement="after", on_existing="allow")).count("episode") == 4
    )
    with pytest.raises(PreprocessingConfigError, match="non-synthetic"):
        mark(stream(["episode", "B"]), on_existing="replace")


def test_match_conversion_has_its_own_denominator_and_explicit_attribution():
    es = stream(list("BXBY") + list("BX"), ["s1"] * 4 + ["s2"] * 2)
    kwargs = dict(path_col="session_id", from_boundary="end", attribution="any_later")
    table = es.get_match_conversion("(B)+", "Y", **kwargs)
    assert table.converted.tolist() == [True, True, False]
    assert table.match_id.tolist() == [1, 2, 1]
    assert table.unit.tolist() == ["match"] * 3
    assert table.target_index.iloc[:2].tolist() == [4, 4]
    assert table.converted.mean() == 2 / 3
    spec = {"pattern": "(B)+", "at": "end", "occurrence": "non_overlapping"}
    with pytest.raises(InvalidParameterError, match="unit"):
        es.get_conversion_rate(spec, "Y", path_col="session_id")
    paths = es.get_conversion_rate(spec, "Y", path_col="session_id", unit="path")
    assert paths.paths_with_start.iloc[0] == 2
    assert paths.conversion_rate.iloc[0] == 1 / 2
    assert paths.attrs["unit"] == "path"
    assert es.get_match_conversion(
        "(B)+", "Y", within=1, **kwargs
    ).converted.tolist() == [False, True, False]
    assert es.get_match_conversion(
        "(B)+", "Y", within="1s", **kwargs
    ).converted.tolist() == [False, True, False]


def test_conversion_start_and_end_boundaries_are_explicit():
    es = stream(list("BBBY"))
    kwargs = dict(path_col="session_id", attribution="any_later", within=1)
    assert not es.get_match_conversion(
        "(B)+", "Y", from_boundary="start", **kwargs
    ).converted.iloc[0]
    assert es.get_match_conversion(
        "(B)+", "Y", from_boundary="end", **kwargs
    ).converted.iloc[0]


def test_no_match_has_empty_schema_and_no_conversion_denominator():
    es = stream(list("BY"))
    result = es.get_match_conversion(
        "(B){3}",
        "Y",
        path_col="session_id",
        from_boundary="end",
        attribution="any_later",
    )
    assert result.empty and "converted" in result and "match_id" in result
    assert pd.isna(result.converted.mean())


@pytest.mark.parametrize("within", ["0s", "-1s", pd.Timedelta("NaT")])
def test_match_conversion_rejects_invalid_duration_instead_of_false_outcomes(within):
    with pytest.raises(InvalidParameterError, match="within"):
        stream(list("BY")).get_match_conversion(
            "B",
            "Y",
            path_col="session_id",
            from_boundary="end",
            attribution="any_later",
            within=within,
        )


def test_marker_insertion_reports_exhausted_float_rank():
    import numpy as np

    es = stream(["B", "existing"])
    df = es.df.copy()
    df.loc[1, "index"] = 1
    df["subindex"] = [2.0, np.nextafter(2.0, 3.0)]
    es = Eventstream(
        df, schema={"path_cols": ["user_id", "session_id"]}, preprocess=False
    )
    with pytest.raises(PreprocessingConfigError, match="representable subindex"):
        mark(es, placement="after")


def test_nested_repetition_failure_is_bounded_not_exponential():
    codes = {"A": 1, "C": 2, "path_start": 3, "path_end": 4}
    program = non_overlapping.compile_pattern("(A->.*){2,}->C", codes)
    assert list(program.finditer([2] + [1] * 1000)) == []
    with pytest.raises(PatternMatchLimitError, match="no partial result"):
        list(program.finditer([2] + [1] * 100, max_states=10))
    with pytest.raises(PatternMatchLimitError):
        non_overlapping.compile_pattern("A->.{1,999999}->C", codes)
    with pytest.raises(PatternMatchLimitError):
        stream(["B"]).find_matches("(B){1000000000}", path_col="session_id")


def test_after_uses_exact_boundary_row_not_first_row_sharing_its_index():
    es = stream(list("AB")).add_start_end_events(path_col="session_id")
    marked = es.add_events(
        "after_boundary",
        anchor={"pattern": "path_end", "at": "end", "occurrence": "non_overlapping"},
        path_col="session_id",
        placement="after",
    )
    assert marked.df.event.tolist() == [
        "path_start",
        "A",
        "B",
        "path_end",
        "after_boundary",
    ]


@pytest.mark.parametrize(
    "placement,expected",
    [
        ("before", ["episode", "hint", "episode", "B"]),
        ("after", ["hint", "episode", "B", "episode"]),
    ],
)
def test_two_matches_at_same_source_index_have_individual_placement(
    placement, expected
):
    es = stream(["B"]).add_events("hint", source_event="B")
    assert events(mark(es, pattern="[B|hint]", placement=placement)) == expected


def test_ambiguous_internal_order_fails_instead_of_arbitrary_match_selection():
    es = (
        stream(["B"])
        .add_events("hint", source_event="B")
        .add_events("hint", source_event="B")
    )
    with pytest.raises(InvalidParameterError, match="event order"):
        es.find_matches("(B)+", path_col="session_id")


def test_greedy_group_crosses_target_unless_gap_excludes_it():
    es = stream(["pv", "pv", "order", "pv", "pv", "pv", "order"])
    greedy = es.find_matches("(pv->.*){2,}->order", path_col="session_id")
    bounded = es.find_matches("(pv->[^order]*){2,}->order", path_col="session_id")
    assert greedy[["start_index", "end_index"]].values.tolist() == [[1, 7]]
    assert bounded[["start_index", "end_index"]].values.tolist() == [[1, 3], [4, 7]]


def test_custom_event_column_and_non_default_schema():
    es = Eventstream(
        pd.DataFrame(
            {
                "visitor": [1, 1, 1],
                "action": ["a", "b", "c"],
                "screen": ["B", "B", "C"],
                "when": pd.date_range("2026-01-01", periods=3, freq="s"),
            }
        ),
        schema={
            "path_cols": ["visitor"],
            "event_col": "action",
            "timestamp_col": "when",
            "index": "idx",
            "subindex": "rank",
        },
    )
    matches = es.find_matches("(B)+", path_col="visitor", event_col="screen")
    assert matches[["start_index", "end_index"]].values.tolist() == [[1, 2]]
    marked = es.add_events(
        "screen_end",
        anchor={
            "pattern": "(B)+",
            "event_col": "screen",
            "at": "end",
            "occurrence": "non_overlapping",
        },
        placement="after",
    )
    assert marked.to_dataframe().action.tolist() == ["a", "b", "screen_end", "c"]


def test_mcp_reports_full_denominator_when_display_is_truncated():
    from retentioneering.mcp import tools
    from retentioneering.mcp._report_session import ReportSession

    session = ReportSession(stream(list("BXBYBX")), {})
    result = tools.get_match_conversion(
        session, "B", "Y", "session_id", "end", "any_later", limit=1
    )
    assert (
        result["matches"] == 3 and result["returned_rows"] == 1 and result["truncated"]
    )
    assert result["converted"] == 2 and result["conversion_rate"] == 2 / 3
    assert result["unit"] == "match"
    json.dumps(result, allow_nan=False)
    assert tools.find_matches(session, "B", "session_id", limit=1)["matches"] == 3
    assert {"find_matches", "get_match_conversion"}.issubset(
        tools.describe_tool()["analysis_tools"]
    )


@pytest.mark.parametrize(
    "pattern,regex",
    [
        ("A->.*->B", r"(A).*?(B)"),
        ("A->.{0,2}->B", r"(A).{0,2}?(B)"),
        ("A->[^C]*->B", r"(A)[^C]*?(B)"),
        ("(B)+", r"(B)(?:B)*"),
        ("(B){2,}", r"(B)(?:B)*(B)"),
        ("(A->B){2,}", r"(A)(B)(?:AB)*(A)(B)"),
        ("(A->.*){2,}->C", r"(A).*?(?:A.*?)*(A).*?(C)"),
        ("(A->[^C]*){2,}->C", r"(A)[^C]*?(?:A[^C]*?)*(A)[^C]*?(C)"),
    ],
)
def test_ordered_matcher_against_independent_regex_on_small_exhaustive_paths(
    pattern, regex
):
    codes = {"A": 1, "B": 2, "C": 3, "path_start": 4, "path_end": 5}
    program = non_overlapping.compile_pattern(pattern, codes)
    oracle = re.compile(regex)
    for length in range(7):
        for letters in itertools.product("ABC", repeat=length):
            text = "".join(letters)
            expected = [
                tuple(m.start(i) for i in range(1, oracle.groups + 1))
                for m in oracle.finditer(text)
            ]
            actual = [
                m.positions for m in program.finditer([codes[x] for x in letters])
            ]
            assert actual == expected, (pattern, text, actual, expected)
