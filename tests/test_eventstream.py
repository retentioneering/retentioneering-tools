import pandas as pd
import pytest

from retentioneering import Eventstream, EventstreamSchema


@pytest.fixture
def simple_df():
    return pd.DataFrame({
        "user_id": ["u1", "u1", "u1", "u2", "u2"],
        "event":   ["home", "catalog", "cart", "home", "checkout"],
        "timestamp": pd.to_datetime([
            "2024-01-01 10:00",
            "2024-01-01 10:05",
            "2024-01-01 10:10",
            "2024-01-01 11:00",
            "2024-01-01 11:15",
        ]),
    })


def test_eventstream_creates(simple_df):
    es = Eventstream(simple_df)
    assert not es.empty()
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


def test_index_created(simple_df):
    es = Eventstream(simple_df)
    assert "index" in es.df.columns
    u1_indices = es.df[es.df["user_id"] == "u1"]["index"].tolist()
    assert u1_indices == [1, 2, 3]


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
    csv.write_text("user_id,event,timestamp\nu1,A,2024-01-01 10:00\nu1,B,2024-01-01 10:05\n")
    es = Eventstream(str(csv))
    assert len(es.df) == 2
