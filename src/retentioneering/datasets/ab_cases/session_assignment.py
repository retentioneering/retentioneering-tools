"""Synthetic A/B case "session assignment": the right split of the wrong unit.

About 8,000 users over four weeks of a furniture web shop and a 50/50 test of a
redesigned product card in the catalog. Stories are planted in the data; the
analysis is the exercise, and the solutions are at the end of this docstring.

The shop and the test
---------------------
The experiment service assigned an arm at the start of every session
(``assigned_at`` = the session start), 50/50, from Monday 2026-06-01; users
arrive on any day of the four weeks and come back at their own pace (most come
once or twice; a few trade buyers, interior designers shopping for clients, have
more than a hundred sessions); the data end on 2026-06-29 00:00. The product
team's question is per user: does the new card make users add more to the cart?

Every session opens with ``session_start`` and browses ``home``, ``catalog``,
``search`` and ``product_view``; it may continue with ``add_to_cart``,
``checkout_start`` and ``order_success``.

Where the treatment sits: in the catalog. A treatment session's catalog tiles
open a quick look (``card_quick_look``, treatment only) before the product page;
exposure starts at the session's first ``catalog``. ``exp_group`` is the arm of
the session an event belongs to.

Fixed at assignment: nothing per user (the arm is per session).

The assignment table is the service's log: one row per session, indexed by
``session_id``, with ``arm`` and ``assigned_at``.

Schema
------
``path_cols=["user_id", "session_id"]``, ``segment_cols=["exp_group"]``, custom
column ``order_value`` (set on ``order_success`` rows only). User ids start at
400001. ``with_truth=True`` adds ``planted_effect`` (custom column, float): the
user's planted change of the per-session add probability under the new card.

Solutions (spoilers)
--------------------
1. Users are in both arms: about 60 % of users have more than one session and
   most of them saw both cards. ``get_sample_ratio("exp_group")`` at the user grain
   reports them as ``multi_valued_paths`` and a mismatch, although the session
   split is a clean 50/50 (``get_sample_ratio`` with the table and
   ``path_col="session_id"``: no mismatch).
2. ``get_ab_test`` at the user grain refuses (``DiffConfigError``: a user in both
   groups), with the stream's segment or with the session table.
3. At the session grain (``path_col="session_id"``) the scorecard runs, and the
   add rate per session reads as a clear increase, but its interval is too
   narrow: it treats every session as an independent unit while the effect of the
   card differs by user (planted per-user effects on the per-session add
   probability: normal, mean +5 pp, standard deviation 25 pp, before the
   probability is clipped to [0.01, 0.95], which lifts the realised mean to about
   +7 pp) and the heavy users contribute many sessions each. A user-clustered
   standard error of the same delta is about 1.5 to 1.8 times the reported
   ``delta_se``.
4. The first session of every user is a clean user-level experiment: cut each path
   before its second ``session_start`` (``truncate_paths("path_start",
   [{"pattern": "session_start->.*->session_start", "at": 1, "offset": -1},
   "path_end"])``) and the arm is constant per user; the add rate there increases
   by about 7 pp (orders follow the adds; the checkout is the same in both arms).

Correct conclusion: fix the assignment (assign per user, then rerun). Until then,
report the first-session read with its caveat (it measures one session per
user; later sessions mixed both cards), never the session-grain interval.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from retentioneering.datasets.ab_cases import _sim

NAME = "session_assignment"
DESCRIPTION = (
    "Furniture web shop, 50/50 test of a product card redesign run by the web "
    "experiment service (8,000 users, 4 weeks)."
)
DEFAULT_N_USERS = 8_000
DEFAULT_SEED = 2026
FIRST_USER_ID = 400_001
SCHEMA = {
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["exp_group"],
    "custom_cols": ["order_value"],
}
TRUTH_COLS = ("planted_effect",)

START = pd.Timestamp("2026-06-01")  # a Monday
N_DAYS = 28

EVENTS = (
    "session_start",
    "home",
    "catalog",
    "search",
    "card_quick_look",
    "product_view",
    "add_to_cart",
    "checkout_start",
    "order_success",
)
(
    _START,
    _HOME,
    _CATALOG,
    _SEARCH,
    _QUICK_LOOK,
    _PRODUCT,
    _ADD,
    _CHECKOUT,
    _ORDER,
) = range(len(EVENTS))
_N_EVENTS = len(EVENTS)
_N_STATES = _N_EVENTS + 1
_EXIT = _N_EVENTS

_WEEKDAY = (1.00, 1.00, 0.98, 1.00, 0.95, 1.05, 1.10)
_BASE_ADD = (1.5, 7.0)  # Beta parameters of a user's per-session add probability
_EFFECT_MEAN, _EFFECT_SD = 0.05, 0.25
_RATE_SHAPE, _RATE_MEAN = 0.15, 0.30  # gamma return rate per day: heavy users
_CHECKOUT_P, _ORDER_P = 0.45, 0.80  # after an add, the same in both arms
_ORDER_LOG_MU, _ORDER_LOG_SIGMA = 5.0, 0.7

_BROWSE = (
    {  # control
        _HOME: {_CATALOG: 70, _SEARCH: 20, _EXIT: 10},
        _CATALOG: {_PRODUCT: 62, _CATALOG: 14, _SEARCH: 4, _EXIT: 20},
        _SEARCH: {_PRODUCT: 66, _SEARCH: 10, _CATALOG: 6, _EXIT: 18},
        _PRODUCT: {_PRODUCT: 20, _CATALOG: 30, _SEARCH: 8, _EXIT: 42},
    },
    {  # treatment: catalog tiles open a quick look first
        _HOME: {_CATALOG: 70, _SEARCH: 20, _EXIT: 10},
        _CATALOG: {_QUICK_LOOK: 62, _CATALOG: 14, _SEARCH: 4, _EXIT: 20},
        _QUICK_LOOK: {_PRODUCT: 85, _CATALOG: 15},
        _SEARCH: {_PRODUCT: 66, _SEARCH: 10, _CATALOG: 6, _EXIT: 18},
        _PRODUCT: {_PRODUCT: 20, _CATALOG: 30, _SEARCH: 8, _EXIT: 42},
    },
)


def generate(n_users: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(events, assignment)``; ``events`` includes the truth column."""
    n = _sim.check_n_users(n_users)
    rng_users, rng_sessions, rng_browse, rng_funnel, rng_time, rng_orders = _sim.streams(
        seed, 6
    )

    # ── users: base propensity, planted effect, pace ─────────────────────────
    base = rng_users.beta(*_BASE_ADD, n)
    effect = rng_users.normal(_EFFECT_MEAN, _EFFECT_SD, n)
    first_s = _sim.arrival_seconds(rng_users, n, N_DAYS, _WEEKDAY)
    rate = rng_users.gamma(_RATE_SHAPE, _RATE_MEAN / _RATE_SHAPE, n)

    # ── sessions, each assigned 50/50 ────────────────────────────────────────
    sess_user, sess_start, sess_no = _sim.sessions(
        rng_sessions, first_s, rate, N_DAYS * _sim.DAY_S, gap_s=3600
    )
    n_sess = len(sess_user)
    treated = rng_sessions.random(n_sess) < 0.5
    p_add = np.clip(base[sess_user] + treated * effect[sess_user], 0.01, 0.95)
    add = rng_sessions.random(n_sess) < p_add

    # ── browsing ─────────────────────────────────────────────────────────────
    cdf = _sim.chain_cdf(list(_BROWSE), _N_STATES)
    chain, _ = _sim.walk(
        rng_browse, cdf, treated.astype(np.int64), np.full(n_sess, _HOME), 16, _N_EVENTS
    )
    chain_len = (chain >= 0).sum(1)
    last_chain = chain[np.arange(n_sess), np.maximum(chain_len - 1, 0)]
    head = np.full((n_sess, 2), _START, dtype=np.int16)
    head[:, 1] = _HOME
    tail = np.full((n_sess, 1), -1, dtype=np.int16)
    tail[add & ((chain_len == 0) | (last_chain != _PRODUCT)), 0] = _PRODUCT

    post = np.full((n_sess, 3), -1, dtype=np.int16)
    a = np.flatnonzero(add)
    post[a, 0] = _ADD
    checkout = rng_funnel.random(len(a)) < _CHECKOUT_P
    post[a[checkout], 1] = _CHECKOUT
    ordered = checkout & (rng_funnel.random(len(a)) < _ORDER_P)
    post[a[ordered], 2] = _ORDER

    grid = np.concatenate([head, chain, tail, post], axis=1)
    ev_sess, ev_code, ev_t = _sim.flatten(grid, sess_start, rng_time)

    # ── frame ────────────────────────────────────────────────────────────────
    ev_user = sess_user[ev_sess]
    user_id = FIRST_USER_ID + np.arange(n, dtype=np.int64)
    is_order = ev_code == _ORDER
    value = np.full(len(ev_code), np.nan)
    value[is_order] = _sim.money(
        rng_orders, int(is_order.sum()), _ORDER_LOG_MU, _ORDER_LOG_SIGMA
    )
    arm = np.where(treated, "treatment", "control").astype(object)
    sess_id = _sim.session_ids(user_id[sess_user], sess_no)
    events = pd.DataFrame(
        {
            "user_id": user_id[ev_user],
            "session_id": sess_id.to_numpy()[ev_sess],
            "event": np.asarray(EVENTS, dtype=object)[ev_code],
            "timestamp": START + pd.to_timedelta(ev_t, unit="s"),
            "exp_group": arm[ev_sess],
            "order_value": value,
            "planted_effect": np.round(effect, 4)[ev_user],
        }
    )
    assignment = pd.DataFrame(
        {
            "arm": arm,
            "assigned_at": START + pd.to_timedelta(sess_start, unit="s"),
        },
        index=pd.Index(sess_id.to_numpy(), name="session_id"),
    )
    return events, assignment
