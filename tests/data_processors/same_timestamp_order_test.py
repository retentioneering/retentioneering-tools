"""`collapse_events` and `split_sessions` on a stream with many same-timestamp
ties, read in shuffled row orders by DuckDB on one thread and on four: every
run must return the same frame, row for row and column for column.

The permutation tests next to each processor's own tests cover every row order
of a few events; this one covers a block of ties too large for a single read
order, where DuckDB's parallel window evaluation decides how tied rows meet.
"""

import hashlib

import numpy as np
import pandas as pd
import pytest

from retentioneering import engine
from retentioneering.data_processors.collapse_events import CollapseEvents
from retentioneering.data_processors.split_sessions import SplitSessions
from retentioneering.eventstream.eventstream import Eventstream

PROCESSORS = {
    "loops": lambda: CollapseEvents(loops=True, agg={"value": "last"}),
    "group_col": lambda: CollapseEvents(group_col="screen"),
    "event_groups": lambda: CollapseEvents(event_groups=["A", "B"], name="AB"),
    "bounds": lambda: CollapseEvents(
        bounds={"start_event": "S", "end_event": "E"}, name="SE"
    ),
    "timeout": lambda: SplitSessions(timeout="90s"),
    "separator": lambda: SplitSessions(separator="S"),
}


@pytest.fixture(scope="module")
def tied_stream() -> Eventstream:
    """300 users, 40 events each, on four distinct timestamps a minute apart:
    every event shares its timestamp with about nine others."""
    rng = np.random.default_rng(20261031)
    users, length = 300, 40
    events = rng.choice(["A", "B", "C", "S", "E"], size=users * length)
    df = pd.DataFrame(
        {
            "user_id": np.repeat([f"u{i:03d}" for i in range(users)], length),
            "event": events,
            "timestamp": pd.Timestamp("2026-01-01")
            + pd.to_timedelta(
                np.sort(rng.integers(0, 4, size=(users, length)), axis=1).ravel(),
                unit="min",
            ),
            "screen": np.where(np.isin(events, ["A", "B"]), "catalog", "checkout"),
            "value": rng.integers(0, 1000, size=users * length),
        }
    )
    return Eventstream(df, {"custom_cols": ["screen", "value"]})


@pytest.fixture
def threads():
    conn = engine._root()
    (before,) = conn.execute("SELECT current_setting('threads')").fetchone()

    def set_threads(n: int) -> None:
        conn.execute(f"SET threads = {int(n)}")

    yield set_threads
    conn.execute(f"SET threads = {int(before)}")


def fingerprint(frame: pd.DataFrame) -> str:
    """Every column, row by row, in the order the processor returned them."""
    hashed = pd.util.hash_pandas_object(
        frame.astype(str).reset_index(drop=True), index=False
    )
    return hashlib.sha256(hashed.to_numpy().tobytes()).hexdigest()


@pytest.mark.parametrize("mode", list(PROCESSORS))
def test_shuffled_reads_on_one_and_four_threads_agree(tied_stream, threads, mode):
    frame, schema = tied_stream._df, tied_stream.schema
    seen = set()
    for n_threads in (1, 4):
        threads(n_threads)
        for seed in range(3):
            shuffled = frame.sample(frac=1, random_state=seed).reset_index(drop=True)
            out, _ = PROCESSORS[mode]().apply(shuffled, schema)
            seen.add(fingerprint(out))
    assert len(seen) == 1
