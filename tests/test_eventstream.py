import pandas as pd
import pytest

from retentioneering import Eventstream


@pytest.fixture
def simple_df():
    return pd.DataFrame(
        {
            "user_id": ["u1", "u1", "u1", "u2", "u2"],
            "event": ["home", "catalog", "cart", "home", "checkout"],
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 10:00",
                    "2024-01-01 10:05",
                    "2024-01-01 10:10",
                    "2024-01-01 11:00",
                    "2024-01-01 11:15",
                ]
            ),
        }
    )


def test_eventstream_creates(simple_df):
    es = Eventstream(simple_df)
    assert not es.is_empty()
    assert len(es.df) == 5


def test_schema_defaults(simple_df):
    es = Eventstream(simple_df)
    assert es.schema.path_col == "user_id"
    assert es.schema.event_col == "event"


def test_schema_custom_cols(simple_df):
    df = simple_df.rename(columns={"user_id": "session", "event": "action"})
    es = Eventstream(df, {"path_cols": ["session"], "event_cols": ["action"]})
    assert es.schema.path_col == "session"
    assert es.schema.event_col == "action"


def test_schema_unknown_key_raises_with_suggestion(simple_df):
    from retentioneering.exceptions import SchemaConfigError

    with pytest.raises(SchemaConfigError, match="timestamp_col"):
        Eventstream(simple_df, {"timestamp": "timestamp"})


def test_schema_auto_classifies_undeclared_columns_as_custom_cols(simple_df):
    df = simple_df.copy()
    df["returned"] = [True, True, True, False, False]
    es = Eventstream(df)
    assert es.schema.custom_cols == ["returned"]
    assert "returned" in es.df.columns


def test_schema_auto_custom_cols_excludes_declared_columns(simple_df):
    df = simple_df.copy()
    df["country"] = ["US", "US", "US", "DE", "DE"]
    es = Eventstream(df, {"segment_cols": ["country"]})
    assert es.schema.custom_cols == []
    assert es.schema.segment_cols == ["country"]


def test_schema_explicit_empty_custom_cols_drops_undeclared_columns(simple_df):
    df = simple_df.copy()
    df["returned"] = [True, True, True, False, False]
    es = Eventstream(df, {"custom_cols": []})
    assert es.schema.custom_cols == []
    assert "returned" not in es.df.columns


def test_schema_explicit_custom_cols_keeps_only_listed_columns(simple_df):
    df = simple_df.copy()
    df["returned"] = [True, True, True, False, False]
    df["source"] = ["ads", "ads", "ads", "organic", "organic"]
    es = Eventstream(df, {"custom_cols": ["returned"]})
    assert es.schema.custom_cols == ["returned"]
    assert "returned" in es.df.columns
    assert "source" not in es.df.columns


def test_schema_explicit_custom_cols_missing_from_df_raises(simple_df):
    from retentioneering.exceptions import SchemaConfigError

    with pytest.raises(SchemaConfigError, match="returned"):
        Eventstream(simple_df, {"custom_cols": ["returned"]})


def test_index_created(simple_df):
    es = Eventstream(simple_df)
    assert "index" in es.df.columns
    u1_indices = es.df[es.df["user_id"] == "u1"]["index"].tolist()
    assert u1_indices == [1, 2, 3]


def test_path_cols_nesting_valid():
    # path_cols must be ordered coarsest-first: every session belongs to
    # exactly one user, so ["user_id", "session_id"] is valid.
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u1", "u1", "u2"],
            "session_id": ["s1", "s1", "s2", "s3"],
            "event": ["a", "b", "c", "d"],
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01 10:00",
                    "2024-01-01 10:01",
                    "2024-01-01 10:02",
                    "2024-01-01 10:03",
                ]
            ),
        }
    )
    es = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
    assert not es.is_empty()


def test_path_cols_nesting_violation_raises():
    from retentioneering.exceptions import SchemaConfigError

    # session_id claimed as the coarser (first) column, but user u1 spans two
    # different session_id values -> session_id doesn't nest user_id.
    df = pd.DataFrame(
        {
            "user_id": ["u1", "u1"],
            "session_id": ["s1", "s2"],
            "event": ["a", "b"],
            "timestamp": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 10:01"]),
        }
    )
    with pytest.raises(SchemaConfigError):
        Eventstream(df, {"path_cols": ["session_id", "user_id"]})


def test_path_col_none_raises():
    from retentioneering.exceptions import SchemaConfigError

    df = pd.DataFrame(
        {
            "user_id": ["u1", None],
            "event": ["a", "b"],
            "timestamp": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 10:01"]),
        }
    )
    with pytest.raises(SchemaConfigError):
        Eventstream(df)


def test_path_col_preserves_numeric_dtype():
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 2],
            "event": ["a", "b", "c"],
            "timestamp": pd.to_datetime(
                ["2024-01-01 10:00", "2024-01-01 10:01", "2024-01-01 10:02"]
            ),
        }
    )
    es = Eventstream(df)
    assert es.df["user_id"].dtype == "int64"


def test_add_start_end_events(simple_df):
    es = Eventstream(simple_df)
    with_se = es.add_start_end_events()
    event_types = set(with_se.df["event_type"].unique())
    assert "path_start" in event_types
    assert "path_end" in event_types


def test_transition_graph_data_proba_out(simple_df):
    es = Eventstream(simple_df)
    tm = es.transition_graph_data(edge_weight="proba_out")
    assert "path_start" in tm.index
    assert "path_end" in tm.columns
    # Row sums (excluding NaN/zero rows) should be <= 1
    row_sums = tm.sum(axis=1)
    for s in row_sums:
        assert s <= 1.01, f"Row sum {s} exceeds 1"


def test_transition_graph_data_count(simple_df):
    es = Eventstream(simple_df)
    tm = es.transition_graph_data(edge_weight="count")
    # Should be integer-like
    assert tm.values.dtype in (int, "int64", "float64")
    assert tm.sum().sum() > 0


def test_transition_graph_data_unique_paths(simple_df):
    es = Eventstream(simple_df)
    tm = es.transition_graph_data(edge_weight="unique_paths")
    assert tm.sum().sum() > 0


def test_transition_graph_data_proba_in(simple_df):
    es = Eventstream(simple_df)
    tm = es.transition_graph_data(edge_weight="proba_in")
    col_sums = tm.sum(axis=0)
    for s in col_sums:
        assert s <= 1.01, f"Col sum {s} exceeds 1"


def test_to_dataframe_excludes_start_end(simple_df):
    es = Eventstream(simple_df)
    with_se = es.add_start_end_events()
    df = with_se.to_dataframe(exclude_start_end=True)
    assert "path_start" not in df["event"].values
    assert "path_end" not in df["event"].values


def test_from_csv(tmp_path):
    csv = tmp_path / "data.csv"
    csv.write_text(
        "user_id,event,timestamp\nu1,A,2024-01-01 10:00\nu1,B,2024-01-01 10:05\n"
    )
    es = Eventstream(str(csv))
    assert len(es.df) == 2
