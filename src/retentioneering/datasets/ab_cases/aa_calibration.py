"""Synthetic A/B case "A/A calibration": two identical arms, one pipeline check.

About 10,000 users over four weeks of an online furniture and decor store, split
50/50 into two arms that run the same build: an A/A test, run to check that the
experiment pipeline finds nothing when there is nothing. Stories are planted in
the data; the analysis is the exercise, and the solutions are at the end of this
docstring.

The store and the test
----------------------
Users are assigned at their first visit (``assigned_at``), 50/50, on any day of
the four weeks from Monday 2026-07-06; the data end on 2026-08-03 00:00. Traffic
and buying follow the week: more visits at the weekend, and a weekend session is
more likely to end in an order. Sessions start at ``home`` and browse
``catalog``, ``search``, ``product_view`` and ``reviews_view``; a session may end
in ``add_to_cart``, ``cart``, ``checkout_start``, ``payment`` and
``order_success``. Order amounts are heavy-tailed.

Where the treatment sits: nowhere. The arms are labelled ``"treatment"`` and
``"control"`` so that the usual ``diff=("exp_group", "treatment", "control")``
works, and both see the same store.

Fixed at assignment (pre-exposure, declare ``strata_pre_exposure=True``):
``platform`` (``"app"`` / ``"web"``, the signup platform). The assignment table
carries it too.

Schema
------
``path_cols=["user_id", "session_id"]``, ``segment_cols=["exp_group",
"platform"]``, custom column ``order_value`` (set on ``order_success`` rows
only). User ids start at 500001. ``with_truth=True`` adds the planted ``whale``
(bool, custom column).

Solutions (spoilers)
--------------------
1. Nothing differs between the arms by design, so every ITT read is a true null:
   conversion, orders per user, and every daily or weekly row (``period="D"``,
   ``period="W"``) are inconclusive (the first day, with few orders, may read
   ``insufficient_data``); the sample ratio is fine. The day-of-week pattern is in
   the levels (a weekend session is about 1.6 times as likely to end in an order),
   not in the deltas: read a test in whole weeks.
2. Six whale accounts of one reseller, created in one batch on the first
   Wednesday, landed in the treatment arm together (a batch shares one hash
   bucket). Each placed one bulk order worth 25,000 to 35,000 (ordinary orders
   are tens to a few hundred, rarely a few thousand). They are the six largest
   paths by revenue. Six whales, not three: whatever their size, k outliers in
   one arm cap Welch's t near the square root of k, so three could never look
   significant on their own.
3. Revenue per user (``column_sum`` of ``order_value``) reads as an increase under
   the default Welch test, and so does average order value under the bootstrap.
   Both are the whales. Switching revenue to ``test="bootstrap"`` does not help:
   the bootstrap resamples the same six paths, and its p-value comes from a
   standard error they dominate. A robust metric does: winsorise the order value
   (a custom column capped at its 99th percentile, about 600) and both revenue
   rows are inconclusive.

Correct conclusion: the 0/1 metrics behave as a null should, but the assignment
has a defect: accounts created together are assigned together, so the reseller,
not the user, is the unit those six accounts share. Randomise (or analyse) at the
reseller level, and declare a robust revenue metric (winsorised or capped) before
any revenue result of a real test can be trusted. One A/A run is a smoke test,
not a calibration: measuring the false-positive rate takes many re-randomised
splits (an A/A replay, which the library does not offer yet).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from retentioneering.datasets.ab_cases import _sim

NAME = "aa_calibration"
DESCRIPTION = (
    "Furniture and decor store, an A/A test (both arms run the same build) of the "
    "experiment pipeline (10,000 users, 4 weeks, revenue)."
)
DEFAULT_N_USERS = 10_000
DEFAULT_SEED = 2026
FIRST_USER_ID = 500_001
SCHEMA = {
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["exp_group", "platform"],
    "custom_cols": ["order_value"],
}
TRUTH_COLS = ("whale",)

START = pd.Timestamp("2026-07-06")  # a Monday
N_DAYS = 28

EVENTS = (
    "home",
    "catalog",
    "search",
    "product_view",
    "reviews_view",
    "add_to_cart",
    "cart",
    "checkout_start",
    "payment",
    "order_success",
)
(
    _HOME,
    _CATALOG,
    _SEARCH,
    _PRODUCT,
    _REVIEWS,
    _ADD,
    _CART,
    _CHECKOUT,
    _PAYMENT,
    _ORDER,
) = range(len(EVENTS))
_N_EVENTS = len(EVENTS)
_N_STATES = _N_EVENTS + 1
_EXIT = _N_EVENTS

_PLATFORMS = ("app", "web")
_PLATFORM_P = (0.55, 0.45)
_WEEKDAY_TRAFFIC = (1.00, 0.95, 0.95, 1.00, 1.05, 1.45, 1.55)
_WEEKDAY_ORDER = (1.0, 1.0, 1.0, 1.0, 1.05, 1.6, 1.7)  # order propensity by weekday
_ORDER_P = (1.5, 30.0)  # Beta parameters of a user's per-session order probability
_RETURN_RATE = 0.10
_CART_ONLY_P = 0.10
_ORDER_LOG_MU, _ORDER_LOG_SIGMA = 4.2, 0.9
_N_WHALES = 6
_WHALE_DAY = 2  # the first Wednesday
_WHALE_ORDER = (25_000.0, 35_000.0)

_BROWSE = {
    _HOME: {_CATALOG: 60, _SEARCH: 25, _PRODUCT: 5, _EXIT: 10},
    _CATALOG: {_PRODUCT: 60, _CATALOG: 14, _SEARCH: 6, _EXIT: 20},
    _SEARCH: {_PRODUCT: 64, _SEARCH: 12, _CATALOG: 6, _EXIT: 18},
    _PRODUCT: {_PRODUCT: 22, _CATALOG: 24, _SEARCH: 8, _REVIEWS: 10, _EXIT: 36},
    _REVIEWS: {_PRODUCT: 55, _EXIT: 45},
}


def generate(n_users: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(events, assignment)``; ``events`` includes the truth column."""
    n = _sim.check_n_users(n_users)
    rng_users, rng_sessions, rng_browse, rng_funnel, rng_time, rng_orders = (
        _sim.streams(seed, 6)
    )

    # ── users; the whales are the last ids, one batch, all in one arm ────────
    n_whales = min(_N_WHALES, n // 100)
    treated = rng_users.random(n) < 0.5
    platform = rng_users.choice(len(_PLATFORMS), size=n, p=_PLATFORM_P)
    assigned_s = _sim.arrival_seconds(rng_users, n, N_DAYS, _WEEKDAY_TRAFFIC)
    order_p = rng_users.beta(*_ORDER_P, n)
    rate = _RETURN_RATE * rng_users.gamma(2.0, 0.5, n)
    # a first session late on the last day would run past the end of the data
    assigned_s = np.minimum(assigned_s, N_DAYS * _sim.DAY_S - 2 * 3600)
    whale = np.zeros(n, dtype=bool)
    if n_whales:
        whale[n - n_whales :] = True
        treated[whale] = True
        platform[whale] = 1
        batch_s = _WHALE_DAY * _sim.DAY_S + 10 * 3600
        assigned_s[whale] = batch_s + 60 * np.arange(n_whales)
        order_p[whale] = 0.0
        rate[whale] = 0.0

    # ── sessions; orders depend on the user and the weekday, never the arm ──
    sess_user, sess_start, sess_no = _sim.sessions(
        rng_sessions, assigned_s, rate, N_DAYS * _sim.DAY_S
    )
    n_sess = len(sess_user)
    weekday = (sess_start // _sim.DAY_S) % 7
    p_order = np.minimum(
        order_p[sess_user] * np.asarray(_WEEKDAY_ORDER)[weekday] * 2.2, 0.9
    )
    order_sess = rng_sessions.random(n_sess) < p_order
    s_whale = whale[sess_user]
    order_sess[s_whale] = True  # the whale's single session is the bulk order
    add_sess = order_sess | (rng_sessions.random(n_sess) < _CART_ONLY_P)

    cdf = _sim.chain_cdf([_BROWSE], _N_STATES)
    chain, _ = _sim.walk(
        rng_browse,
        cdf,
        np.zeros(n_sess, dtype=np.int64),
        np.full(n_sess, _HOME),
        16,
        _N_EVENTS,
    )
    chain_len = (chain >= 0).sum(1)
    last_chain = chain[np.arange(n_sess), np.maximum(chain_len - 1, 0)]
    head = np.full((n_sess, 1), _HOME, dtype=np.int16)
    tail = np.full((n_sess, 1), -1, dtype=np.int16)
    tail[add_sess & ((chain_len == 0) | (last_chain != _PRODUCT)), 0] = _PRODUCT

    post = np.full((n_sess, 5), -1, dtype=np.int16)
    a = np.flatnonzero(add_sess)
    post[a, 0] = _ADD
    to_cart = order_sess[a] | (rng_funnel.random(len(a)) < 0.6)
    post[a[to_cart], 1] = _CART
    checkout = order_sess[a] | (to_cart & (rng_funnel.random(len(a)) < 0.3))
    post[a[checkout], 2] = _CHECKOUT
    o = a[order_sess[a]]
    post[o, 3] = _PAYMENT
    post[o, 4] = _ORDER

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
    whale_rows = np.flatnonzero(is_order & whale[ev_user])
    value[whale_rows] = np.round(rng_orders.uniform(*_WHALE_ORDER, len(whale_rows)), -1)
    arm = np.where(treated, "treatment", "control")
    platform_name = np.asarray(_PLATFORMS, dtype=object)[platform]
    events = pd.DataFrame(
        {
            "user_id": user_id[ev_user],
            "session_id": _sim.session_ids(user_id[ev_user], sess_no[ev_sess]),
            "event": np.asarray(EVENTS, dtype=object)[ev_code],
            "timestamp": START + pd.to_timedelta(ev_t, unit="s"),
            "exp_group": arm[ev_user],
            "platform": platform_name[ev_user],
            "order_value": value,
            "whale": whale[ev_user],
        }
    )
    assignment = pd.DataFrame(
        {
            "arm": arm,
            "assigned_at": START + pd.to_timedelta(assigned_s, unit="s"),
            "platform": platform_name,
        },
        index=pd.Index(user_id, name="user_id"),
    )
    return events, assignment
