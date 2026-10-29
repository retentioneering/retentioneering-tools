"""Synthetic A/B case "SRM platform bug": a checkout redesign with a broken log.

About 10,000 assigned users over five weeks of a shopping app (iOS, Android and
web) and a 50/50 test of a redesigned checkout. Stories are planted in the data;
the analysis is the exercise, and the solutions are at the end of this docstring.

The store and the test
----------------------
Users are assigned at their first visit (``assigned_at``), 50/50, on any day of the
first four weeks from Monday 2026-05-04; the data end on 2026-06-08 00:00, five
weeks after the start. Sessions start at ``home`` and browse ``catalog``,
``search`` and ``product_view``; a session may end in ``add_to_cart``, ``cart`` and
``checkout_start``.

Where the treatment sits: at ``checkout_start``. Control continues with the old
form (``checkout_address``, ``checkout_delivery``), treatment with the redesign
(``checkout_express``, treatment only); both end in ``payment`` and
``order_success``. Exposure starts at a user's first ``checkout_start``.

Fixed at assignment (pre-exposure, declare ``strata_pre_exposure=True``):
``platform`` (``"ios"``, ``"android"``, ``"web"``, where the user signed up). The
assignment table carries it too.

The assignment table is the analytics exposure log, the table an analyst gets:
one row per user the log saw, with ``arm``, ``assigned_at`` and ``platform``. It
comes from the same event pipeline as the stream.

Schema
------
``path_cols=["user_id", "session_id"]``, ``segment_cols=["exp_group",
"platform"]``, custom column ``order_value`` (set on ``order_success`` rows only).
User ids start at 300001; ids missing from the table were assigned but never
logged. ``with_truth=True`` adds the planted ``intent`` (``"high"`` / ``"low"``,
custom column).

Solutions (spoilers)
--------------------
1. The checkout redesign does nothing: every user's purchase probability is the
   same in both arms (high-intent users 50 %, low-intent users 2 %).
2. A logging bug: the Android treatment build released on day 9 (2026-05-12)
   sends its first event batch only after some time in the app, so users who
   leave quickly on their first visit are never logged, neither their events nor
   their exposure. It drops about 85 % of the low-intent and 5 % of the
   high-intent Android treatment users assigned from day 9 on. Users assigned
   earlier were already logged.
3. The sample ratio of the table is far off (``get_sample_ratio`` mismatch, p far
   below 0.001); ``by="platform"`` puts the whole mismatch on Android, and the
   table cut at day 9 is balanced before and unbalanced after (the treatment's
   share of each later day is about 0.4).
4. The naive read (stream or table) shows a conversion lift of about +3 pp
   that "survives" the test: the missing users are the ones who would not have
   bought. Per platform the lift sits on Android only; iOS and web, where nothing
   was lost, are inconclusive, and so are the users assigned before day 9.

Correct conclusion: the test is invalid. Do not read any metric of it; fix the
logging (or recover the exposure log from the assignment service) and rerun.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from retentioneering.datasets.ab_cases import _sim

NAME = "srm_platform_bug"
DESCRIPTION = (
    "Shopping app on iOS, Android and web, 50/50 test of a checkout redesign "
    "(10,000 users, 5 weeks)."
)
DEFAULT_N_USERS = 10_000
DEFAULT_SEED = 2026
FIRST_USER_ID = 300_001
SCHEMA = {
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["exp_group", "platform"],
    "custom_cols": ["order_value"],
}
TRUTH_COLS = ("intent",)

START = pd.Timestamp("2026-05-04")  # a Monday
N_DAYS = 35
ENROL_DAYS = 28
BUG_DAY = 8  # day index: the ninth day, 2026-05-12

EVENTS = (
    "home",
    "catalog",
    "search",
    "product_view",
    "add_to_cart",
    "cart",
    "checkout_start",
    "checkout_address",
    "checkout_delivery",
    "checkout_express",
    "payment",
    "order_success",
)
(
    _HOME,
    _CATALOG,
    _SEARCH,
    _PRODUCT,
    _ADD,
    _CART,
    _CHECKOUT,
    _ADDRESS,
    _DELIVERY,
    _EXPRESS,
    _PAYMENT,
    _ORDER,
) = range(len(EVENTS))
_N_EVENTS = len(EVENTS)
_N_STATES = _N_EVENTS + 1
_EXIT = _N_EVENTS

_PLATFORMS = ("ios", "android", "web")
_PLATFORM_P = (0.30, 0.45, 0.25)
_ANDROID = 1
_WEEKDAY = (1.00, 0.97, 0.97, 1.00, 0.95, 1.12, 1.20)
_HIGH_INTENT_P = 0.30
_CONV = {"high": 0.50, "low": 0.02}
_DROP = {"high": 0.05, "low": 0.85}
_RETURN_RATE = {"high": 0.15, "low": 0.04}
_CART_ONLY_P = {"high": 0.25, "low": 0.08}  # sessions with an add and no order
_ORDER_LOG_MU, _ORDER_LOG_SIGMA = 3.9, 0.55

_BROWSE = {
    _HOME: {_CATALOG: 62, _SEARCH: 25, _PRODUCT: 3, _EXIT: 10},
    _CATALOG: {_PRODUCT: 60, _CATALOG: 14, _SEARCH: 6, _EXIT: 20},
    _SEARCH: {_PRODUCT: 64, _SEARCH: 12, _CATALOG: 6, _EXIT: 18},
    _PRODUCT: {_PRODUCT: 24, _CATALOG: 26, _SEARCH: 12, _EXIT: 38},
}


def generate(n_users: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(events, assignment)``; ``events`` includes the truth column."""
    n = _sim.check_n_users(n_users)
    rng_users, rng_sessions, rng_browse, rng_funnel, rng_time, rng_orders = (
        _sim.streams(seed, 6)
    )

    # ── users, outcomes and the logging bug ──────────────────────────────────
    high = rng_users.random(n) < _HIGH_INTENT_P
    treated = rng_users.random(n) < 0.5
    platform = rng_users.choice(len(_PLATFORMS), size=n, p=_PLATFORM_P)
    assigned_s = _sim.arrival_seconds(rng_users, n, ENROL_DAYS, _WEEKDAY)
    converts = rng_users.random(n) < np.where(high, _CONV["high"], _CONV["low"])
    rate = np.where(high, _RETURN_RATE["high"], _RETURN_RATE["low"]) * rng_users.gamma(
        2.0, 0.5, n
    )
    affected = treated & (platform == _ANDROID) & (assigned_s >= BUG_DAY * _sim.DAY_S)
    lost = affected & (
        rng_users.random(n) < np.where(high, _DROP["high"], _DROP["low"])
    )
    logged = np.flatnonzero(~lost)

    # ── sessions of the logged users ─────────────────────────────────────────
    sess_user, sess_start, sess_no = _sim.sessions(
        rng_sessions, assigned_s[logged], rate[logged], N_DAYS * _sim.DAY_S
    )
    sess_user = logged[sess_user]
    n_sess = len(sess_user)
    user_first = np.full(n, -1)
    user_first[sess_user[sess_no == 0]] = np.flatnonzero(sess_no == 0)
    n_user_sess = np.bincount(sess_user, minlength=n)
    pick = np.floor(rng_sessions.random(n) * np.maximum(n_user_sess, 1)).astype(
        np.int64
    )
    order_sess = np.zeros(n_sess, dtype=bool)
    buyers = np.flatnonzero(converts & ~lost)
    order_sess[user_first[buyers] + pick[buyers]] = True
    s_high = high[sess_user]
    cart_only = ~order_sess & (
        rng_sessions.random(n_sess)
        < np.where(s_high, _CART_ONLY_P["high"], _CART_ONLY_P["low"])
    )
    add_sess = order_sess | cart_only

    # ── browsing ─────────────────────────────────────────────────────────────
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

    # ── funnel ───────────────────────────────────────────────────────────────
    s_treated = treated[sess_user]
    post = np.full((n_sess, 7), -1, dtype=np.int16)
    a = np.flatnonzero(add_sess)
    post[a, 0] = _ADD
    to_cart = order_sess[a] | (rng_funnel.random(len(a)) < 0.6)
    post[a[to_cart], 1] = _CART
    checkout = order_sess[a] | (to_cart & (rng_funnel.random(len(a)) < 0.35))
    c = a[checkout]
    post[c, 2] = _CHECKOUT
    trt = s_treated[c]
    post[c[trt], 3] = _EXPRESS
    post[c[~trt], 3] = _ADDRESS
    post[c[~trt], 4] = _DELIVERY
    o = a[order_sess[a]]
    post[o, 5] = _PAYMENT
    post[o, 6] = _ORDER

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
            "intent": np.where(high, "high", "low").astype(object)[ev_user],
        }
    )
    assignment = pd.DataFrame(
        {
            "arm": arm[logged],
            "assigned_at": START + pd.to_timedelta(assigned_s[logged], unit="s"),
            "platform": platform_name[logged],
        },
        index=pd.Index(user_id[logged], name="user_id"),
    )
    return events, assignment
