"""Shared building blocks of the A/B case generators.

Every generator draws user-level outcomes first (arm, type, conversion) from the
planted probabilities, then renders sessions and events consistent with them, so
each planted effect has an exact expectation. Everything is vectorised NumPy;
each generator draws from independent child streams of one ``SeedSequence``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from retentioneering.exceptions import InvalidParameterError

DAY_S = 86_400

# Share of sessions starting in each hour of the day (sums to 1).
HOUR_PROFILE = np.array(
    [
        0.010, 0.006, 0.004, 0.003, 0.003, 0.005, 0.012, 0.025, 0.038, 0.045,
        0.048, 0.050, 0.052, 0.050, 0.048, 0.047, 0.050, 0.055, 0.065, 0.075,
        0.082, 0.078, 0.060, 0.030,
    ]
)  # fmt: skip
HOUR_PROFILE = HOUR_PROFILE / HOUR_PROFILE.sum()


def check_n_users(n_users) -> int:
    if (
        isinstance(n_users, bool)
        or not isinstance(n_users, (int, np.integer))
        or n_users < 1
    ):
        raise InvalidParameterError("n_users", n_users, ["a positive int"])
    return int(n_users)


def streams(seed: int, n: int) -> list[np.random.Generator]:
    """``n`` independent generators: tuning one stage never reshuffles another."""
    return [np.random.default_rng(s) for s in np.random.SeedSequence(seed).spawn(n)]


def arrival_seconds(
    rng: np.random.Generator,
    n: int,
    n_days: int,
    weekday_factor=(1, 1, 1, 1, 1, 1, 1),
) -> np.ndarray:
    """Arrival times (seconds since a Monday start) over ``n_days`` days.

    The day is drawn with weights ``weekday_factor[day % 7]``; the time of day
    follows ``HOUR_PROFILE``, rounded to whole seconds.
    """
    days = np.arange(n_days)
    weights = np.asarray(weekday_factor, dtype=float)[days % 7]
    day = rng.choice(n_days, size=n, p=weights / weights.sum())
    hour = rng.choice(24, size=n, p=HOUR_PROFILE)
    second = rng.integers(0, 3600, n)
    return (day * DAY_S + hour * 3600 + second).astype(np.int64)


def sessions(
    rng: np.random.Generator,
    first_s: np.ndarray,
    rate_per_day: np.ndarray,
    end_s: int,
    gap_s: int = 2 * 3600,
    tail_s: int = 2 * 3600,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """First session at ``first_s``, returns as a Poisson process until the end.

    Returns ``(sess_user, sess_start, sess_no)`` sorted by user and time; ``sess_user``
    indexes ``first_s``. Return sessions closer than ``gap_s`` to the previous one are
    dropped, and none starts in the last ``tail_s`` seconds of the window.
    """
    n = len(first_s)
    horizon = np.maximum(end_s - tail_s - first_s, 0) / DAY_S
    n_returns = rng.poisson(rate_per_day * horizon)
    ret_user = np.repeat(np.arange(n), n_returns)
    ret_start = (
        first_s[ret_user]
        + gap_s / 2
        + rng.random(len(ret_user))
        * (np.repeat(horizon, n_returns) * DAY_S - gap_s / 2)
    )
    ret_start = (ret_start // 60 * 60).astype(np.int64)
    sess_user = np.concatenate([np.arange(n), ret_user])
    sess_start = np.concatenate([first_s, ret_start]).astype(np.int64)
    order = np.lexsort((sess_start, sess_user))
    sess_user, sess_start = sess_user[order], sess_start[order]
    same_user = np.r_[False, sess_user[1:] == sess_user[:-1]]
    too_close = same_user & (np.r_[0, np.diff(sess_start)] < gap_s)
    sess_user, sess_start = sess_user[~too_close], sess_start[~too_close]
    return sess_user, sess_start, session_numbers(sess_user)


def session_numbers(sess_user: np.ndarray) -> np.ndarray:
    """0-based number of each session within its user (input sorted by user)."""
    idx = np.arange(len(sess_user))
    first = np.r_[True, sess_user[1:] != sess_user[:-1]]
    return idx - np.maximum.accumulate(np.where(first, idx, 0))


def chain_cdf(chains: list[dict], n_states: int) -> np.ndarray:
    """Cumulative transition matrices ``(kind, state, next_state)``.

    ``chains[k]`` is ``{state: {next_state: weight}}``; a state without a row exits
    to state ``n_states - 1``.
    """
    cdf = np.zeros((len(chains), n_states, n_states))
    for k, chain in enumerate(chains):
        for state in range(n_states):
            row = np.zeros(n_states)
            for nxt, weight in chain.get(state, {n_states - 1: 1}).items():
                row[nxt] = weight
            cdf[k, state] = np.cumsum(row / row.sum())
        cdf[k, :, -1] = 1.0
    return cdf


def walk(
    rng: np.random.Generator,
    cdf: np.ndarray,
    kind: np.ndarray,
    start: np.ndarray,
    max_len: int,
    n_events: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Run one Markov chain per row from ``start`` until a terminal state.

    States ``< n_events`` are events and are written to the grid; any state
    ``>= n_events`` ends the walk. Returns ``(grid, end_state)``: ``grid`` is
    ``(rows, max_len)`` int16 with -1 padding (the start state is not written);
    ``end_state`` is the terminal state reached (the last column forces an exit to
    ``cdf.shape[1] - 1`` unless it reached another terminal state).
    """
    n = len(kind)
    exit_state = cdf.shape[1] - 1
    grid = np.full((n, max_len), -1, dtype=np.int16)
    state = start.astype(np.int64).copy()
    end_state = np.full(n, exit_state, dtype=np.int64)
    active = np.arange(n)
    for step in range(max_len):
        if len(active) == 0:
            break
        u = rng.random(len(active))
        nxt = (u[:, None] >= cdf[kind[active], state[active]]).sum(1)
        nxt = np.minimum(nxt, exit_state)
        if step == max_len - 1:
            nxt = np.where(nxt >= n_events, nxt, exit_state)
        stop = nxt >= n_events
        end_state[active[stop]] = nxt[stop]
        cont = ~stop
        grid[active[cont], step] = nxt[cont]
        state[active[cont]] = nxt[cont]
        active = active[cont]
    return grid, end_state


def flatten(
    grid: np.ndarray,
    sess_start: np.ndarray,
    rng: np.random.Generator,
    min_gap_s: int = 8,
    max_gap_s: int = 120,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Turn a session grid (``-1`` = no event) into events.

    Returns ``(ev_sess, ev_code, ev_t)`` in session order, with gaps of ``min_gap_s``
    to ``max_gap_s`` seconds (multiples of 4) between consecutive events of a session;
    the first event comes one gap after the session start. Every session must hold
    at least one event.
    """
    ev_sess, ev_col = np.nonzero(grid >= 0)
    ev_code = grid[ev_sess, ev_col].astype(np.int64)
    gaps = 4 * rng.integers(min_gap_s // 4, max_gap_s // 4 + 1, len(ev_code))
    cum_gap = np.cumsum(gaps.astype(np.int64))
    first = np.r_[0, np.flatnonzero(np.diff(ev_sess)) + 1]
    offset = np.repeat(
        cum_gap[first] - gaps[first], np.diff(np.r_[first, len(ev_sess)])
    )
    ev_t = sess_start[ev_sess] + cum_gap - offset
    return ev_sess, ev_code, ev_t


def session_ids(user_id: np.ndarray, sess_no: np.ndarray) -> pd.Series:
    """``"<user_id>_<n>"`` with ``n`` counted from 1."""
    return pd.Series(user_id.astype(str)) + "_" + pd.Series((sess_no + 1).astype(str))


def money(
    rng: np.random.Generator, n: int, log_mu: float, log_sigma: float
) -> np.ndarray:
    """Lognormal amounts rounded to cents."""
    return np.round(np.exp(rng.normal(log_mu, log_sigma, n)), 2)
