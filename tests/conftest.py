import os
from typing import Any

os.environ["RETENTIONEERING_NO_TRACK"] = "1"

import numpy as np
import pandas as pd
import pytest

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture(autouse=True)
def _fresh_id_match_warnings():
    """The id-match warning shows once per table in a process; each test starts
    with none shown, so a test that expects it does not depend on test order."""
    from retentioneering.tools._diff import _ID_MATCH_WARNED

    _ID_MATCH_WARNED.clear()
    yield


@pytest.fixture()
def base_dir() -> str:
    return BASE_DIR


@pytest.fixture()
def fx_read_csv(base_dir: str) -> callable([str, str, Any]):
    def read_csv(filename: str, sep: str = "\t") -> pd.read_csv:
        return pd.read_csv(os.path.join(base_dir, filename), sep=sep)

    return read_csv


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="also run tests marked slow (Lumeo-scale timings, coverage simulations)",
    )


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers", "slow: long simulations and timings; run with --run-slow"
    )


def pytest_collection_modifyitems(config, items) -> None:
    if config.getoption("--run-slow") or os.environ.get("RETENTIONEERING_RUN_SLOW"):
        return
    skip = pytest.mark.skip(reason="slow: pass --run-slow to run it")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip)


def _markov_stream(n_events: int = 80, n_users: int = 3000, seed: int = 0):
    """Paths of a Markov chain over ``n_events`` event types in which almost every
    transition occurs (81 sources x 81 targets for 80 types); the treatment arm
    moves ten rows of the chain."""
    from retentioneering.eventstream.eventstream import Eventstream

    rng = np.random.default_rng(seed)
    events = np.array([f"event_{i:02d}" for i in range(n_events)])
    chain = rng.dirichlet(np.full(n_events, 2.0), size=n_events)
    frames = []
    for arm, n in (("treatment", n_users + 100), ("control", n_users - 100)):
        moved = chain.copy()
        if arm == "treatment":
            moved[:10] = rng.dirichlet(np.full(n_events, 2.0), size=10)
        cumulative = moved.cumsum(axis=1)
        length = rng.integers(20, 120, size=n)
        current = rng.integers(0, n_events, size=n)
        steps = []
        for _ in range(int(length.max())):
            steps.append(current)
            u = rng.random(n)[:, None]
            current = np.minimum((cumulative[current] < u).sum(axis=1), n_events - 1)
        path = np.stack(steps, axis=1)
        keep = np.arange(path.shape[1])[None, :] < length[:, None]
        user, step = np.nonzero(keep)
        frames.append(
            pd.DataFrame(
                {
                    "user_id": [f"{arm}{u}" for u in user],
                    "event": events[path[keep]],
                    "timestamp": pd.Timestamp("2026-01-01")
                    + pd.to_timedelta(user * 10_000 + step * 30, unit="s"),
                    "exp_group": arm,
                }
            )
        )
    return Eventstream(
        pd.concat(frames, ignore_index=True), {"segment_cols": ["exp_group"]}
    )


@pytest.fixture(scope="session")
def ab_markov80():
    """An A/B stream with 80 event types in which almost every transition occurs:
    an 81 x 81 transition matrix (6,500+ edges), the size the A/B matrices' payload
    and static export are checked at."""
    return _markov_stream()
