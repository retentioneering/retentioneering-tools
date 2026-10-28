"""Documentation examples; optional engine checks: uv run --with chdb --with sqlglot pytest tests/docs/observation_windows_test.py."""

from pathlib import Path
import re

import pandas as pd
import pytest

from retentioneering import Eventstream, engine

ROOT = Path(__file__).resolve().parents[2]


def micros(value):
    return pd.Timestamp(value, tz="UTC").value // 1000


def fixture_rows():
    return [
        ("cross", "2026-08-31 23:50:00"),
        ("cross", "2026-09-01 00:00:00"),  # A included, same session
        ("cross", "2026-09-01 00:30:00"),  # equality: same session
        ("cross", "2026-09-01 00:30:00"),  # tie, distinct real event
        ("cross", "2026-09-01 01:00:00.000001"),  # > timeout
        ("cross", "2026-09-30 23:50:00"),
        ("cross", "2026-10-01 00:00:00"),  # B excluded, same session
        ("exact", "2026-09-30 12:00:00"),
        ("exact", "2026-10-14 12:00:00"),  # return exactly at deadline
        ("late", "2026-09-30 12:00:00"),
        ("late", "2026-10-14 12:00:00.000001"),
        ("silent", "2026-09-30 12:00:00"),
        ("outside", "2026-08-01 00:00:00"),  # L included but no core
        ("outside", "2026-10-16 00:00:00"),  # R excluded
    ]


def run_sql(dialect, compact=True, end="2026-10-16 00:00:00", extra=()):
    sql = (ROOT / f"docs/sql/{dialect}_observation_window.sql").read_text()
    sql = sql.replace("2026-10-16 00:00:00", end)
    rows = fixture_rows() + list(extra)
    source = " UNION ALL ".join(
        f"SELECT '{user}' AS user_id, 'visit' AS event, '{i:04}' AS event_id, "
        f"{micros(ts)} AS ts_us"
        for i, (user, ts) in enumerate(rows)
    )
    if dialect == "clickhouse":
        values = ",".join(
            f"('{user}', 'visit', '{i:04}', {micros(ts)})"
            for i, (user, ts) in enumerate(rows)
        )
        source = (
            "SELECT * FROM values('user_id String, event String, event_id String, ts_us Int64', "
            + values
            + ")"
        )
    sql = re.sub(
        r"source AS \(.*?\),\nextended AS",
        f"source AS ({source}),\nextended AS",
        sql,
        flags=re.S,
    )
    if not compact:
        sql = sql.replace(
            "SELECT * FROM enriched WHERE is_core", "SELECT * FROM enriched"
        )
    if dialect == "clickhouse":
        chdb = pytest.importorskip("chdb")
        return chdb.query(
            sql,
            "DataFrame",
            options={"max_query_size": 2000000, "max_parser_depth": 10000},
        )
    sqlglot = pytest.importorskip("sqlglot")
    # A local semantic check, NOT a BigQuery execution/dry run.
    translated = sqlglot.transpile(sql, read="bigquery", write="duckdb")[0]
    return engine.run(translated)


@pytest.mark.parametrize("dialect", ["bigquery", "clickhouse"])
def test_sql_boundaries_context_and_outcomes(dialect):
    df = run_sql(dialect)
    full = run_sql(dialect, compact=False)
    assert set(df.event_id) == set(full.loc[full.is_core.astype(bool), "event_id"])
    assert "outside" not in set(full.user_id)
    assert len(df) == 8  # no multiplication by the summary joins
    cross = df[df.user_id == "cross"]
    assert cross.observed_session_index.tolist() == [1, 1, 1, 2, 3]
    assert cross.session_event_count.tolist() == [4, 4, 4, 1, 2]
    assert cross.iloc[0].session_start_us == micros("2026-08-31 23:50:00")
    assert cross.iloc[-1].session_end_us == micros("2026-10-01 00:00:00")
    assert cross.iloc[0].next_session_start_us == micros("2026-09-01 01:00:00.000001")
    assert pd.isna(cross.iloc[0].previous_session_end_us)
    outcomes = df.groupby("user_id").outcome.first().to_dict()
    assert outcomes == {
        "cross": "returned",
        "exact": "returned",
        "late": "inactive_14d",
        "silent": "inactive_14d",
    }
    assert pd.isna(df[df.user_id == "silent"].iloc[0].next_event_us)
    assert not df.session_left_censored.any()
    assert not df.session_right_censored.any()


@pytest.mark.parametrize("dialect", ["bigquery", "clickhouse"])
def test_sql_short_followup_and_exclusive_watermark(dialect):
    # Deadline itself is excluded from observation: no evidence of return/absence.
    df = run_sql(dialect, end="2026-10-14 12:00:00")
    assert df[df.user_id == "exact"].iloc[0].outcome == "right_censored"
    assert df[df.user_id == "silent"].iloc[0].outcome == "right_censored"
    # Enough coverage to include the exact-deadline event.
    df = run_sql(dialect, end="2026-10-14 12:00:00.000001")
    assert df[df.user_id == "exact"].iloc[0].outcome == "returned"
    assert df[df.user_id == "silent"].iloc[0].outcome == "inactive_14d"


@pytest.mark.parametrize("dialect", ["bigquery", "clickhouse"])
def test_sql_session_clipped_at_both_scan_edges(dialect):
    # A continuous chain spans L, A, B and R; padding cannot complete it.
    extra = [
        ("chain", str(t))
        for t in pd.date_range("2026-08-01", "2026-10-16", freq="30min")
    ]
    df = run_sql(dialect, extra=extra)
    chain = df[df.user_id == "chain"]
    assert chain.observed_session_index.nunique() == 1
    assert chain.session_left_censored.all()
    assert chain.session_right_censored.all()
    assert chain.session_end_us.iloc[0] == micros("2026-10-15 23:30:00")


def test_eventstream_documented_import_and_core_metrics():
    frame = pd.DataFrame(
        {
            "user_id": ["u", "u", "u"],
            "event": ["before", "a", "b"],
            "event_id": ["0", "1", "2"],
            "timestamp": pd.to_datetime(
                ["2026-08-31 23:50", "2026-09-01 00:00", "2026-09-01 00:10"]
            ),
            "observed_session_index": [1, 1, 1],
            "is_core": [False, True, True],
            "outcome": ["right_censored"] * 3,
        }
    )
    guide = (ROOT / "docs/guide/observation-windows.md").read_text()
    code = re.search(r"```python\n(.*?)```", guide, re.S).group(1)
    ns = {"export_df": frame}
    exec(code, ns)
    core = ns["core"]
    assert len(core.to_dataframe()) == 2
    assert core.to_dataframe().session_id.nunique() == 1
    assert core.get_metrics([{"metric": "duration"}]).iloc[0, 0] == 600
    assert ns["stream"].get_metrics([{"metric": "duration"}]).iloc[0, 0] == 1200


def test_no_churn_marker_does_not_prove_retention():
    frame = pd.DataFrame(
        {
            "user_id": ["early", "recent"],
            "event": ["visit", "visit"],
            "timestamp": pd.to_datetime(["2026-09-01", "2026-09-20"]),
        }
    )
    result = (
        Eventstream(frame)
        .add_events("churned", churn={"inactivity_days": 14})
        .to_dataframe()
    )
    marker = result[result.event == "churned"]
    assert marker.user_id.tolist() == ["early"]
    assert marker.timestamp.iloc[0] == pd.Timestamp("2026-09-01")
    # recent has zero follow-up, despite having no marker.
    assert result[result.user_id == "recent"].timestamp.nunique() == 1


def test_dst_calendar_day_is_not_elapsed_day():
    start = pd.Timestamp("2026-10-25", tz="Europe/Belgrade").tz_convert("UTC")
    end = pd.Timestamp("2026-10-26", tz="Europe/Belgrade").tz_convert("UTC")
    assert end - start == pd.Timedelta(hours=25)
