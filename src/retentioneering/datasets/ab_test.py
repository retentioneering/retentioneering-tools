"""Bundled A/B test datasets, and the default one, "Northwind Home".

:func:`load_ab_test` and :func:`load_ab_test_assignment` take a dataset ``name``
(:data:`AB_TESTS`, or :func:`list_ab_tests`, maps every name to a one-line
description). Each dataset is a deterministic generator (no data file) with
stories planted in it: an effect hidden in a pre-exposure segment, a broken test
that must be rejected, a population or horizon that changes the answer. Each
generator's docstring describes the design and ends with a "Solutions (spoilers)"
section. ``northwind_home``, the default, is generated in this module; the others
live in ``retentioneering.datasets.ab_cases.<name>``. User ids do not overlap
between datasets (``northwind_home`` 100001+, ``lumeo_smart_cart`` 200001+,
``srm_platform_bug`` 300001+, ``session_assignment`` 400001+, ``aa_calibration``
500001+, ``trial_conversion`` 600001+, ``push_campaign`` 700001+, for fewer than
100,000 users), so an assignment table never matches another dataset's stream.

Northwind Home
==============

A deterministic generator producing about 10,000 users over eight weeks of an
online home-goods store with a randomised 50/50 test of a ``quick_add`` feature: a
one-tap "add to basket" button on the product page. The dataset is built so that
the tutorial's analysis finds the planted structure and nothing else; every planted
story has an acceptance test in ``tests/datasets/ab_test_test.py``.

The store and the test
----------------------
Users are assigned at their first app open (``assigned_at``), 50/50, from Monday
2026-03-02 for six weeks (``cohort_week`` ``"W1"`` .. ``"W6"``); the generator's
window ends on 2026-04-27 00:00, exactly eight weeks after the start, and the last
event (2026-04-26 22:04) is the end of data that ``horizon`` and since-assignment
bins use. About 3 % of assigned users
never produce a tracked event ("silent" users): they are in the assignment table of
:func:`load_ab_test_assignment` but not in the events. Every session opens with
browsing (``home``, ``catalog``, ``search``, ``search_filter``, ``product_view``,
``collections_view``, ``wishlist_add``, ``reviews_view``, ``support_chat``) and may end in
an add. In control the add is ``add_to_cart``; in treatment 70 % of adds are
``quick_add`` (the feature, treatment only) and the rest ``add_to_cart``. After the add
come ``cart``, ``checkout_start``, ``payment`` and ``order_success``.

Where the treatment sits: ``quick_add`` follows a ``product_view`` almost every
time, so users see the button from their first ``product_view``, the strict
exposure point. The button changes nothing before the first add: the browsing
before a user's first add is generated identically in both arms, so anything
measured on the window before the first add is a pre-exposure attribute (the
tutorial's choice of exposure marker).

Fixed at assignment (pre-exposure, declare ``strata_pre_exposure=True``):
``platform`` (``"mobile"`` / ``"desktop"``) and ``cohort_week``. Both are constant
per user.

Schema
------
``path_cols=["user_id", "session_id"]``, ``segment_cols=["exp_group", "platform",
"cohort_week"]``, custom columns ``order_value`` (nullable float, set on
``order_success`` rows only) and ``order_items`` (nullable int, set on
``order_success`` rows only). Every segment is constant per user.

Examples
--------
>>> from retentioneering import datasets
>>> stream = datasets.load_ab_test()
>>> assignment = datasets.load_ab_test_assignment()
>>> datasets.list_ab_tests()["srm_platform_bug"]  # doctest: +SKIP

Solutions (spoilers)
--------------------
The planted stories of Northwind Home:

1. The top-line conversion effect (share of assigned users with ``order_success``) is
   inconclusive: |delta| below half the MDE, p > 0.2; the sample ratio of the
   assignment table is fine (p > 0.05).
2. Every overall funnel step of ``["product_view", "cart", "checkout_start",
   "payment", "order_success"]`` is grey after Holm. The treatment build shows a
   "checkout now" shortcut after every treatment add that reaches the cart
   (``quick_add`` or ``add_to_cart``), which starts checkouts that never pay, so the
   *conditional*
   (step-to-step) rate ``checkout_start -> payment`` has a naive two-proportion
   p < 0.05 (lower in treatment) although nothing changed for users who meant to pay:
   its denominator is a post-exposure population.
3. Three pre-exposure user types with near-exclusive marker events: ``scanner``
   (18 %; three or more ``search_filter`` before the first add; the feature raises
   their conversion by +12.5 pp at the default seed; the acceptance test allows +4 to
   +15 pp), ``curator`` (18 %; ``collections_view`` and ``wishlist_add``; -6.7 pp,
   allowed -12 to -4 pp) and ``other`` (64 %; no planted effect, -0.4 pp, allowed
   within 2 pp). Blind
   K-means on ``has_event_bulk`` flags plus ``active_days`` of the window before the
   first add (``truncate_paths("path_start", [{"pattern": "add_to_cart", "offset": -1},
   {"pattern": "quick_add", "offset": -1}, "path_end"])``, K = 3, ``n_init=4``,
   ``random_state=42``, minmax) recovers them (ARI >= 0.5) and both signs survive Holm.
4. Treatment curators who used ``quick_add`` reopen the cart: the loop
   ``cart -> cart -> cart`` exists in treatment only (the ``cart -> cart`` self-edge is
   0 in control), and looping sessions rarely reach checkout.
5. Novelty since assignment: the treatment's checkout propensity is multiplied by 1.35
   in days 0-13 after assignment, by 0.9 in days 14-27 and by 0.7 from day 28 on, so
   the conversion effect in days 0-13 minus the effect in days 28-41 is positive with
   a confidence interval that excludes 0 at 10,000 users.
6. ``order_value`` is lognormal and does not depend on the arm (planted AOV shift 0):
   AOV is inconclusive.
7. The sign of the effect flips by pre-exposure path length: users with fewer than
   10 events before their first add (``metric_bins`` with ``edges=[10]`` on
   ``length`` of the pre-exposure window; a median split gives the same picture)
   convert less in treatment, users with longer windows convert more; both bins are
   significant after Holm.

Correct conclusion (the tutorial's readout): over the whole window the effect on
conversion is inconclusive; within each user's first 28 days it is positive, and
it fades after the first two weeks (novelty). The effect differs between
pre-exposure user types (scanners gain, curators lose, the curators' carts turn
into a reopen loop): ship or not is a product call on these facts, and the next
experiment targets the loop.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from retentioneering.datasets.ab_cases import (
    aa_calibration,
    lumeo_smart_cart,
    push_campaign,
    session_assignment,
    srm_platform_bug,
    trial_conversion,
)
from retentioneering.exceptions import InvalidParameterError

if TYPE_CHECKING:
    from retentioneering.eventstream import Eventstream

__all__ = [
    "AB_TESTS",
    "DEFAULT_AB_TEST",
    "list_ab_tests",
    "load_ab_test",
    "load_ab_test_assignment",
    "EVENTS",
    "DEFAULT_N_USERS",
    "DEFAULT_SEED",
]

DEFAULT_N_USERS = 10_000
DEFAULT_SEED = 2026
DEFAULT_AB_TEST = "northwind_home"

_CASES = {
    case.NAME: case
    for case in (
        lumeo_smart_cart,
        srm_platform_bug,
        session_assignment,
        aa_calibration,
        trial_conversion,
        push_campaign,
    )
}
#: Bundled A/B datasets, name -> one-line description; the first is the default.
AB_TESTS = MappingProxyType(
    {
        DEFAULT_AB_TEST: (
            "Home-goods store, 50/50 test of a one-tap quick_add button "
            "(10,000 users, 8 weeks); the tutorial's dataset."
        ),
        **{name: case.DESCRIPTION for name, case in _CASES.items()},
    }
)

EVENTS = (
    "home",
    "catalog",
    "search",
    "search_filter",
    "product_view",
    "collections_view",
    "wishlist_add",
    "reviews_view",
    "quick_add",
    "add_to_cart",
    "cart",
    "checkout_start",
    "payment",
    "order_success",
    "support_chat",
)

_SCHEMA = {
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["exp_group", "platform", "cohort_week"],
    "custom_cols": ["order_value", "order_items"],
}

# ---------------------------------------------------------------------------
# Generator parameters
# ---------------------------------------------------------------------------

START = pd.Timestamp("2026-03-02")  # a Monday
N_DAYS = 56  # eight weeks; the data end on a week boundary
_DAY_S = 86_400
_END_S = N_DAYS * _DAY_S

_ENTRY_WEEK_P = (0.36, 0.28, 0.13, 0.10, 0.07, 0.06)  # assignment week -> cohort_week
_SILENT_P = 0.03
_TYPES = ("other", "scanner", "curator")
_TYPE_P = (0.64, 0.18, 0.18)
_PLATFORMS = ("mobile", "desktop")
_PLATFORM_P = (0.58, 0.42)
_RETURN_RATE = 0.075  # mean return sessions per day (gamma-distributed per user)
_SESSION_GAP_S = 2 * 3600  # minimum distance between two session starts

# event codes
(
    _HOME,
    _CATALOG,
    _SEARCH,
    _FILTER,
    _PRODUCT,
    _COLLECTIONS,
    _WISHLIST,
    _REVIEWS,
    _QUICK_ADD,
    _ADD,
    _CART,
    _CHECKOUT,
    _PAYMENT,
    _ORDER,
    _SUPPORT,
) = range(len(EVENTS))
_EXIT = len(EVENTS)  # absorbing chain states
_TO_ADD = _EXIT + 1
_N_STATES = _EXIT + 2
_MAX_CHAIN = 24

# Browsing Markov chains, one per user type: {state: {next_state: weight}}.
_CHAIN = {
    "other": {
        _HOME: {_CATALOG: 70, _SEARCH: 6, _PRODUCT: 10, _SUPPORT: 2, _EXIT: 12},
        _CATALOG: {_PRODUCT: 60, _CATALOG: 12, _SEARCH: 3, _HOME: 5, _EXIT: 20},
        _SEARCH: {_PRODUCT: 62, _SEARCH: 8, _FILTER: 1, _CATALOG: 11, _EXIT: 18},
        _FILTER: {_PRODUCT: 70, _EXIT: 30},
        _PRODUCT: {
            _PRODUCT: 18,
            _CATALOG: 23,
            _SEARCH: 2,
            _REVIEWS: 7,
            _SUPPORT: 1,
            _TO_ADD: 26,
            _EXIT: 30,
        },
        _COLLECTIONS: {_PRODUCT: 60, _EXIT: 40},
        _WISHLIST: {_PRODUCT: 40, _EXIT: 60},
        _REVIEWS: {_PRODUCT: 55, _TO_ADD: 10, _EXIT: 35},
        _SUPPORT: {_HOME: 30, _EXIT: 70},
    },
    "scanner": {
        _HOME: {_SEARCH: 88, _CATALOG: 2, _EXIT: 10},
        _CATALOG: {_PRODUCT: 50, _SEARCH: 30, _EXIT: 20},
        _SEARCH: {_FILTER: 60, _PRODUCT: 30, _EXIT: 10},
        _FILTER: {_FILTER: 25, _PRODUCT: 60, _SEARCH: 10, _EXIT: 5},
        _PRODUCT: {
            _FILTER: 14,
            _SEARCH: 16,
            _PRODUCT: 15,
            _CATALOG: 1,
            _REVIEWS: 2,
            _SUPPORT: 1,
            _TO_ADD: 28,
            _EXIT: 31,
        },
        _COLLECTIONS: {_PRODUCT: 60, _EXIT: 40},
        _WISHLIST: {_PRODUCT: 40, _EXIT: 60},
        _REVIEWS: {_PRODUCT: 55, _TO_ADD: 10, _EXIT: 35},
        _SUPPORT: {_HOME: 30, _EXIT: 70},
    },
    "curator": {
        _HOME: {_COLLECTIONS: 60, _CATALOG: 20, _EXIT: 20},
        _CATALOG: {_PRODUCT: 60, _COLLECTIONS: 15, _EXIT: 25},
        _SEARCH: {_PRODUCT: 70, _EXIT: 30},
        _FILTER: {_PRODUCT: 70, _EXIT: 30},
        _PRODUCT: {
            _WISHLIST: 18,
            _REVIEWS: 16,
            _COLLECTIONS: 12,
            _PRODUCT: 10,
            _SUPPORT: 1,
            _TO_ADD: 23,
            _EXIT: 26,
        },
        _COLLECTIONS: {_PRODUCT: 62, _COLLECTIONS: 13, _EXIT: 25},
        _WISHLIST: {_PRODUCT: 35, _COLLECTIONS: 30, _EXIT: 35},
        _REVIEWS: {_PRODUCT: 50, _WISHLIST: 20, _TO_ADD: 5, _EXIT: 25},
        _SUPPORT: {_HOME: 30, _EXIT: 70},
    },
}

# Funnel after the add (per add session).
_CHECKOUT_P = 0.48  # P(checkout_start | cart), before the treatment multipliers
_MOBILE_CHECKOUT_MULT = 0.92
_PAYMENT_P = 0.97  # P(payment | checkout_start)
_ORDER_P = 0.82  # P(order_success | payment)
_CART_P = 0.70  # P(cart | add), the same in both arms
_IDLE_CHECKOUT_P = 0.05  # treatment: checkout_start without intent to pay
_QUICK_ADD_SHARE = 0.70  # share of treatment adds made with quick_add
# Treatment effects on the checkout propensity.
_SCANNER_MULT = 1.05
_CURATOR_MULT = 0.60
_CURATOR_LOOP_P = 0.80  # P(cart loop | treatment curator, quick_add, cart)
_CURATOR_LOOP_MULT = 0.50  # checkout multiplier in a looping session
_LENGTH_EDGE = 10  # pre-exposure length: < edge is "short"
_LENGTH_MULT = {"short": 0.70, "long": 1.72}
_NOVELTY = ((14, 1.35), (28, 0.90), (np.inf, 0.70))  # (days since assignment <, mult)
# order value: lognormal per item bundle, independent of the arm
_ORDER_LOG_MU = 3.75
_ORDER_LOG_SIGMA = 0.55


def _transition_cdf() -> np.ndarray:
    """Cumulative transition matrices, shape (type, state, next_state)."""
    cdf = np.zeros((len(_TYPES), _N_STATES, _N_STATES))
    for t, name in enumerate(_TYPES):
        for state in range(_N_STATES):
            row = np.zeros(_N_STATES)
            weights = _CHAIN[name].get(state, {_EXIT: 1})
            for nxt, w in weights.items():
                row[nxt] = w
            cdf[t, state] = np.cumsum(row / row.sum())
        cdf[t, :, -1] = 1.0
    return cdf


def _novelty_mult(days: np.ndarray) -> np.ndarray:
    out = np.empty(len(days))
    lower = -np.inf
    for upper, mult in _NOVELTY:
        out[(days >= lower) & (days < upper)] = mult
        lower = upper
    return out


def _generate(n_users: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(events, users)`` in compact form.

    ``events`` holds integer codes sorted by user and time: ``user``, ``session`` (number
    within the user), ``event`` (index into ``EVENTS``), ``t`` (seconds since ``START``),
    ``order_cents`` and ``order_items`` (0 outside ``order_success`` rows). ``users`` has
    one row per assigned user: ``user``, ``treated``, ``assigned_s``, ``platform``,
    ``cohort`` and ``user_type`` (indices into the module tuples).
    """
    if (
        isinstance(n_users, bool)
        or not isinstance(n_users, (int, np.integer))
        or n_users < 1
    ):
        raise InvalidParameterError("n_users", n_users, ["a positive int"])
    n_users = int(n_users)
    # independent streams, so that tuning one stage does not reshuffle the others
    rng_users, rng_sessions, rng_browse, rng_funnel, rng_time, rng_orders = (
        np.random.default_rng(child) for child in np.random.SeedSequence(seed).spawn(6)
    )

    # ── users ────────────────────────────────────────────────────────────────
    utype = rng_users.choice(len(_TYPES), size=n_users, p=_TYPE_P)
    treated = rng_users.random(n_users) < 0.5
    platform = rng_users.choice(len(_PLATFORMS), size=n_users, p=_PLATFORM_P)
    cohort = rng_users.choice(len(_ENTRY_WEEK_P), size=n_users, p=_ENTRY_WEEK_P)
    assigned_s = (
        cohort * 7 * _DAY_S + rng_users.integers(0, 7 * _DAY_S, n_users)
    ).astype(np.int64)
    silent = rng_users.random(n_users) < _SILENT_P
    rate = rng_users.gamma(2.0, _RETURN_RATE / 2.0, n_users)

    # ── sessions: the first at assignment, returns as a Poisson process ──────
    visitors = np.flatnonzero(~silent)
    horizon = (_END_S - _SESSION_GAP_S - assigned_s[visitors]) / _DAY_S
    n_returns = rng_sessions.poisson(rate[visitors] * np.maximum(horizon, 0))
    ret_user = np.repeat(visitors, n_returns)
    ret_start = (
        assigned_s[ret_user]
        + _SESSION_GAP_S / 2
        + rng_sessions.random(len(ret_user))
        * (np.repeat(horizon, n_returns) * _DAY_S - _SESSION_GAP_S / 2)
    )
    sess_user = np.concatenate([visitors, ret_user])
    ret_start = (ret_start // 60 * 60).astype(np.int64)  # whole minutes
    sess_start = np.concatenate([assigned_s[visitors], ret_start])
    order = np.lexsort((sess_start, sess_user))
    sess_user, sess_start = sess_user[order], sess_start[order]
    same_user = np.r_[False, sess_user[1:] == sess_user[:-1]]
    too_close = same_user & (np.r_[0, np.diff(sess_start)] < _SESSION_GAP_S)
    sess_user, sess_start = sess_user[~too_close], sess_start[~too_close]
    n_sess = len(sess_user)
    first_of_user = np.r_[True, sess_user[1:] != sess_user[:-1]]
    sess_no = np.arange(n_sess) - np.maximum.accumulate(
        np.where(first_of_user, np.arange(n_sess), 0)
    )
    s_type = utype[sess_user]

    # ── browsing: a type-specific prefix with the marker events, then a chain ─
    prefix = np.full((n_sess, 8), -1, dtype=np.int16)
    is_other, is_scanner, is_curator = (s_type == 0), (s_type == 1), (s_type == 2)
    prefix[is_other, 0] = rng_browse.choice(
        [_HOME, _CATALOG, _SEARCH], size=is_other.sum(), p=[0.6, 0.37, 0.03]
    )
    # scanners: search then >= 3 search_filter (first session always, later 85 %)
    sc = np.flatnonzero(is_scanner)
    sc_marked = sc[(sess_no[sc] == 0) | (rng_browse.random(len(sc)) < 0.85)]
    sc_plain = np.setdiff1d(sc, sc_marked)
    prefix[sc_marked, 0] = _HOME
    prefix[sc_marked, 1] = _SEARCH
    n_filter = 3 + np.minimum(rng_browse.geometric(0.5, len(sc_marked)) - 1, 3)
    for k in range(6):
        prefix[sc_marked[n_filter > k], 2 + k] = _FILTER
    prefix[sc_plain, 0] = _SEARCH
    # curators: collections_view -> product_view -> wishlist_add (first always, later 85 %)
    cu = np.flatnonzero(is_curator)
    cu_marked = cu[(sess_no[cu] == 0) | (rng_browse.random(len(cu)) < 0.85)]
    cu_plain = np.setdiff1d(cu, cu_marked)
    prefix[cu_marked, 0] = _HOME
    prefix[cu_marked, 1] = _COLLECTIONS
    prefix[cu_marked, 2] = _PRODUCT
    prefix[cu_marked, 3] = _WISHLIST
    prefix[cu_plain, 0] = _HOME
    last_prefix = prefix[np.arange(n_sess), (prefix >= 0).sum(1) - 1]

    cdf = _transition_cdf()
    chain = np.full((n_sess, _MAX_CHAIN), -1, dtype=np.int16)
    state = last_prefix.astype(np.int64)
    active = np.arange(n_sess)
    reached_add = np.zeros(n_sess, dtype=bool)
    for step in range(_MAX_CHAIN):
        if len(active) == 0:
            break
        u = rng_browse.random(len(active))
        nxt = (u[:, None] >= cdf[s_type[active], state[active]]).sum(1)
        if step == _MAX_CHAIN - 1:
            nxt = np.where(nxt == _TO_ADD, _TO_ADD, _EXIT)
        reached_add[active[nxt == _TO_ADD]] = True
        cont = nxt < _EXIT
        chain[active[cont], step] = nxt[cont]
        state[active[cont]] = nxt[cont]
        active = active[cont]
    browse_len = (prefix >= 0).sum(1) + (chain >= 0).sum(1)

    # ── pre-exposure length: events before the user's first add ──────────────
    cum_len = np.cumsum(browse_len)
    user_first_sess = np.flatnonzero(first_of_user)
    len_before_user = np.repeat(
        np.r_[0, cum_len[user_first_sess[1:] - 1]],
        np.diff(np.r_[user_first_sess, n_sess]),
    )
    add_sess = np.flatnonzero(reached_add)
    add_users, first_add_pos = np.unique(sess_user[add_sess], return_index=True)
    pre_len = np.zeros(n_users, dtype=np.int64)
    # non-adders: their whole path
    total_len = np.zeros(n_users, dtype=np.int64)
    np.add.at(total_len, sess_user, browse_len)
    pre_len[:] = total_len
    first_add_sess = add_sess[first_add_pos]
    pre_len[add_users] = cum_len[first_add_sess] - len_before_user[first_add_sess]
    is_long = pre_len >= _LENGTH_EDGE

    # ── after the add: arm-dependent funnel ─────────────────────────────────
    n_add = len(add_sess)
    a_user = sess_user[add_sess]
    a_treated = treated[a_user]
    a_type = utype[a_user]
    quick = a_treated & (rng_funnel.random(n_add) < _QUICK_ADD_SHARE)
    days = (sess_start[add_sess] - assigned_s[a_user]) / _DAY_S
    cart = rng_funnel.random(n_add) < _CART_P
    p_checkout = np.full(n_add, _CHECKOUT_P)
    p_checkout[platform[a_user] == 0] *= _MOBILE_CHECKOUT_MULT
    mult = np.ones(n_add)
    mult[a_type == 1] *= _SCANNER_MULT
    mult[a_type == 2] *= _CURATOR_MULT
    looped = cart & quick & (a_type == 2) & (rng_funnel.random(n_add) < _CURATOR_LOOP_P)
    mult[looped] *= _CURATOR_LOOP_MULT
    mult *= np.where(is_long[a_user], _LENGTH_MULT["long"], _LENGTH_MULT["short"])
    mult *= _novelty_mult(days)
    p_checkout = np.where(a_treated, np.minimum(p_checkout * mult, 0.97), p_checkout)
    checkout = cart & (rng_funnel.random(n_add) < p_checkout)
    # treatment's "checkout now" shortcut, after any treatment add that reaches the
    # cart: starts that never pay
    idle_checkout = (
        a_treated & cart & ~checkout & (rng_funnel.random(n_add) < _IDLE_CHECKOUT_P)
    )
    extra_loop = looped & (rng_funnel.random(n_add) < 0.4)
    payment = checkout & (rng_funnel.random(n_add) < _PAYMENT_P)
    ordered = payment & (rng_funnel.random(n_add) < _ORDER_P)
    support = (checkout & ~ordered) & (rng_funnel.random(n_add) < 0.15)
    checkout = checkout | idle_checkout

    post = np.full((n_sess, 9), -1, dtype=np.int16)
    post[add_sess, 0] = np.where(quick, _QUICK_ADD, _ADD)
    post[add_sess[cart], 1] = _CART
    post[add_sess[looped], 2] = _CART
    post[add_sess[looped], 3] = _CART
    post[add_sess[extra_loop], 4] = _CART
    post[add_sess[checkout], 5] = _CHECKOUT
    post[add_sess[payment], 6] = _PAYMENT
    post[add_sess[ordered], 7] = _ORDER
    post[add_sess[support], 8] = _SUPPORT

    # ── flatten to events ────────────────────────────────────────────────────
    grid = np.concatenate([prefix, chain, post], axis=1)
    ev_sess, ev_col = np.nonzero(grid >= 0)
    ev_code = grid[ev_sess, ev_col]
    gaps = 4 * rng_time.integers(2, 31, len(ev_code)).astype(np.int64)  # 8..120 s
    cum_gap = np.cumsum(gaps)
    sess_first_ev = np.r_[0, np.flatnonzero(np.diff(ev_sess)) + 1]
    offset = np.repeat(
        cum_gap[sess_first_ev] - gaps[sess_first_ev],
        np.diff(np.r_[sess_first_ev, len(ev_sess)]),
    )
    ev_t = sess_start[ev_sess] + cum_gap - offset

    is_order = ev_code == _ORDER
    # one candidate basket per add session, drawn whatever the arm: no AOV shift
    basket_items = 1 + np.minimum(rng_orders.poisson(0.7, n_add), 5)
    basket_cents = np.round(
        100
        * np.exp(rng_orders.normal(_ORDER_LOG_MU, _ORDER_LOG_SIGMA, n_add))
        * basket_items**0.6
    )
    items = basket_items[ordered]
    cents = basket_cents[ordered]
    order_cents = np.zeros(len(ev_code), dtype=np.int64)
    order_cents[is_order] = cents.astype(np.int64)
    order_items = np.zeros(len(ev_code), dtype=np.int16)
    order_items[is_order] = items

    events = pd.DataFrame(
        {
            "user": sess_user[ev_sess].astype(np.int32),
            "session": sess_no[ev_sess].astype(np.int16),
            "event": ev_code.astype(np.int8),
            "t": ev_t.astype(np.int64),
            "order_cents": order_cents,
            "order_items": order_items,
        }
    )
    users = pd.DataFrame(
        {
            "user": np.arange(n_users, dtype=np.int32),
            "treated": treated,
            "assigned_s": assigned_s,
            "platform": platform.astype(np.int8),
            "cohort": cohort.astype(np.int8),
            "user_type": utype.astype(np.int8),
        }
    )
    return events, users


def _user_ids(users: pd.DataFrame) -> np.ndarray:
    return 100_001 + users["user"].to_numpy(dtype=np.int64)


def _assemble(
    events: pd.DataFrame, users: pd.DataFrame, with_truth: bool = False
) -> pd.DataFrame:
    """Turn the compact ``(events, users)`` pair into the public events frame."""
    u = events["user"].to_numpy()
    uid = _user_ids(users)
    out = pd.DataFrame(
        {
            "user_id": uid[u],
            "session_id": pd.Series(uid[u].astype(str))
            + "_"
            + pd.Series((events["session"].to_numpy() + 1).astype(str)),
            "event": np.asarray(EVENTS, dtype=object)[events["event"].to_numpy()],
            "timestamp": START + pd.to_timedelta(events["t"].to_numpy(), unit="s"),
            "exp_group": np.where(users["treated"].to_numpy(), "treatment", "control")[
                u
            ],
            "platform": np.asarray(_PLATFORMS, dtype=object)[
                users["platform"].to_numpy()
            ][u],
            "cohort_week": np.array([f"W{c + 1}" for c in range(len(_ENTRY_WEEK_P))])[
                users["cohort"].to_numpy()
            ][u],
        }
    )
    items = events["order_items"].to_numpy()
    has_order = items > 0
    out["order_value"] = np.where(
        has_order, events["order_cents"].to_numpy() / 100.0, np.nan
    )
    out["order_items"] = pd.arrays.IntegerArray(items.astype(np.int64), ~has_order)
    if with_truth:
        out["user_type"] = np.asarray(_TYPES, dtype=object)[
            users["user_type"].to_numpy()
        ][u]
    return out


def _assignment(users: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "arm": np.where(users["treated"].to_numpy(), "treatment", "control"),
            "assigned_at": START
            + pd.to_timedelta(users["assigned_s"].to_numpy(), unit="s"),
        },
        index=pd.Index(_user_ids(users), name="user_id"),
    )
    return frame


# ---------------------------------------------------------------------------
# Cached fixture (tests): a lossless, compact encoding of ``_generate``'s output
# ---------------------------------------------------------------------------

_FIXTURE_FILES = ("events", "sessions", "users")


def _write_fixture(
    directory, n_users: int = DEFAULT_N_USERS, seed: int = DEFAULT_SEED
) -> dict:
    """Write the compact output of ``_generate(n_users, seed)`` as three parquet files.

    Events keep only the event code, the seconds since the previous event of the
    session and the order fields; sessions keep the user, the first event time and
    the number of events. Returns ``{file name: size in bytes}``.
    """
    import pathlib

    import pyarrow as pa
    import pyarrow.parquet as pq

    events, users = _generate(n_users, seed)
    directory = pathlib.Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    key = events["user"].to_numpy().astype(np.int64) * 1_000 + events["session"]
    new_session = np.r_[True, key.to_numpy()[1:] != key.to_numpy()[:-1]]
    t = events["t"].to_numpy()
    dt = np.where(new_session, 0, np.r_[0, np.diff(t)])
    starts = np.flatnonzero(new_session)
    tables = {
        "events": pa.table(
            {
                "event": events["event"].to_numpy(),
                "dt": dt.astype(np.uint8),
                "order_cents": events["order_cents"].to_numpy().astype(np.int32),
                "order_items": events["order_items"].to_numpy().astype(np.int8),
            }
        ),
        "sessions": pa.table(
            {
                "user": events["user"].to_numpy()[starts],
                "first_t": t[starts].astype(np.int64),
                "n_events": np.diff(np.r_[starts, len(t)]).astype(np.int16),
            }
        ),
        "users": pa.Table.from_pandas(users, preserve_index=False),
    }
    meta = {b"n_users": str(n_users).encode(), b"seed": str(seed).encode()}
    sizes = {}
    for name in _FIXTURE_FILES:
        table = tables[name].replace_schema_metadata(meta)
        wide = [
            field.name
            for field in table.schema
            if pa.types.is_integer(field.type) and field.type.bit_width >= 32
        ]
        path = directory / f"ab_test_{name}.parquet"
        pq.write_table(
            table,
            path,
            compression="zstd",
            compression_level=19,
            use_dictionary=[col for col in table.column_names if col not in wide],
            column_encoding={col: "DELTA_BINARY_PACKED" for col in wide},
        )
        sizes[path.name] = path.stat().st_size
    return sizes


def _read_fixture(directory) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read the files of ``_write_fixture`` back into ``_generate``'s ``(events, users)``."""
    import pathlib

    import pyarrow.parquet as pq

    directory = pathlib.Path(directory)
    ev, sess, users = (
        pq.read_table(directory / f"ab_test_{name}.parquet").to_pandas()
        for name in _FIXTURE_FILES
    )
    n_events = sess["n_events"].to_numpy().astype(np.int64)
    ev_sess = np.repeat(np.arange(len(sess)), n_events)
    sess_user = sess["user"].to_numpy()
    first_of_user = np.r_[True, sess_user[1:] != sess_user[:-1]]
    idx = np.arange(len(sess))
    sess_no = idx - np.maximum.accumulate(np.where(first_of_user, idx, 0))
    dt = ev["dt"].to_numpy().astype(np.int64)
    within = np.cumsum(dt) - np.repeat(
        np.cumsum(dt)[np.r_[0, np.cumsum(n_events)[:-1]]], n_events
    )
    t = sess["first_t"].to_numpy()[ev_sess] + within
    events = pd.DataFrame(
        {
            "user": sess_user[ev_sess].astype(np.int32),
            "session": sess_no[ev_sess].astype(np.int16),
            "event": ev["event"].to_numpy().astype(np.int8),
            "t": t.astype(np.int64),
            "order_cents": ev["order_cents"].to_numpy().astype(np.int64),
            "order_items": ev["order_items"].to_numpy().astype(np.int16),
        }
    )
    return events, users


def list_ab_tests() -> dict[str, str]:
    """Return the bundled A/B datasets: name -> one-line description.

    The first entry is the default of :func:`load_ab_test`. The descriptions say what
    each experiment is, not what it hides; the planted stories and the correct
    conclusion are in the "Solutions (spoilers)" section of each generator's
    docstring (``retentioneering.datasets.ab_test`` for ``northwind_home``,
    ``retentioneering.datasets.ab_cases.<name>`` for the others).

    Returns
    -------
    dict
        ``{name: description}``, in this order: ``"northwind_home"``,
        ``"lumeo_smart_cart"``, ``"srm_platform_bug"``, ``"session_assignment"``,
        ``"aa_calibration"``, ``"trial_conversion"``, ``"push_campaign"``.

    Examples
    --------
    >>> from retentioneering import datasets
    >>> for name, description in datasets.list_ab_tests().items():  # doctest: +SKIP
    ...     print(name, "-", description)
    """
    return dict(AB_TESTS)


def _resolve(name, n_users, seed):
    """Validate the loader arguments; return ``(name, n_users, seed)``."""
    if name is None:
        name = DEFAULT_AB_TEST
    if not isinstance(name, str) or name not in AB_TESTS:
        raise InvalidParameterError("name", name, list(AB_TESTS))
    if name == DEFAULT_AB_TEST:
        defaults = (DEFAULT_N_USERS, DEFAULT_SEED)
    else:
        defaults = (_CASES[name].DEFAULT_N_USERS, _CASES[name].DEFAULT_SEED)
    n_users = defaults[0] if n_users is None else n_users
    seed = defaults[1] if seed is None else seed
    if (
        isinstance(n_users, bool)
        or not isinstance(n_users, (int, np.integer))
        or n_users < 1
    ):
        raise InvalidParameterError("n_users", n_users, ["a positive int"])
    if isinstance(seed, bool) or not isinstance(seed, (int, np.integer)) or seed < 0:
        raise InvalidParameterError("seed", seed, ["a non-negative int"])
    return name, int(n_users), int(seed)


def load_ab_test(
    name: str | None = DEFAULT_AB_TEST,
    *,
    as_dataframe: bool = False,
    with_truth: bool = False,
    n_users: int | None = None,
    seed: int | None = None,
) -> "Eventstream | pd.DataFrame":
    """Generate a bundled A/B test dataset (default: "Northwind Home").

    Every dataset is a synthetic randomised experiment generated deterministically
    (no data file) with stories planted in it. :func:`list_ab_tests` lists the names
    with a one-line description; each generator's docstring describes the design
    (where the treatment sits, which segments are fixed at assignment) and ends with
    the solutions.

    Parameters
    ----------
    name : str, default "northwind_home"
        The dataset; ``None`` means the default.

        - ``"northwind_home"`` — home-goods store, 50/50 test of a one-tap
          ``quick_add`` button (about 10,000 users, eight weeks); the tutorial's
          dataset.
        - ``"lumeo_smart_cart"`` — home and living store, a smart cart drawer after
          ``add_to_cart`` (15,000 users, twelve weeks, sessions as a second path
          column).
        - ``"srm_platform_bug"`` — a checkout redesign on three platforms (10,000
          users, five weeks).
        - ``"session_assignment"`` — a product card redesign assigned by the web
          experiment service (8,000 users, four weeks).
        - ``"aa_calibration"`` — an A/A test that calibrates the pipeline (10,000
          users, four weeks, revenue).
        - ``"trial_conversion"`` — a 14-day trial of a subscription app and a paywall
          change (10,000 users, about six weeks).
        - ``"push_campaign"`` — a push campaign assigned from a marketing table
          (20,000 users, two weeks).

        Any other value raises ``InvalidParameterError`` listing the names.
    as_dataframe : bool, default False
        Return the raw events frame instead of an ``Eventstream``.
    with_truth : bool, default False
        Add the dataset's planted truth column (custom column, never a segment), for
        checking an analysis at the end; the analysis must not use it.
        ``northwind_home``: ``user_type`` (``scanner`` / ``curator`` / ``other``);
        the other datasets name theirs in their docstring.
    n_users : int, optional
        Number of assigned users; default the dataset's documented size. Users who
        never produce an event (where a dataset has them) are in the assignment
        table only.
    seed : int, optional
        Generator seed; the default reproduces the documented numbers.

    Returns
    -------
    Eventstream or pandas.DataFrame
        One row per event, sorted by path and time. Every dataset has
        ``user_id`` (int, the randomised unit unless the dataset says otherwise),
        ``session_id`` (str ``"<user_id>_<n>"``, the second path column),
        ``event``, ``timestamp`` and the arm segment ``exp_group``
        (``"treatment"`` / ``"control"``); the dataset's docstring lists its other
        segments and custom columns. For ``northwind_home``:

        - ``user_id`` — int, constant per user; path column.
        - ``session_id`` — str ``"<user_id>_<n>"``; second path column.
        - ``event`` — one of ``EVENTS``.
        - ``timestamp`` — event time, second precision, within
          ``[2026-03-02, 2026-04-27)``.
        - ``exp_group`` — ``"treatment"`` or ``"control"`` (segment).
        - ``platform`` — ``"mobile"`` or ``"desktop"`` (segment).
        - ``cohort_week`` — assignment week ``"W1"`` .. ``"W6"`` (segment).
        - ``order_value`` — float, order amount on ``order_success`` rows, NaN elsewhere.
        - ``order_items`` — nullable int, items on ``order_success`` rows, NA elsewhere.
        - ``user_type`` — only with ``with_truth=True``: the planted type.

        The ``Eventstream`` has ``path_cols=["user_id", "session_id"]``, the
        dataset's segment columns and its custom columns (plus the truth column
        with ``with_truth=True``).

    Raises
    ------
    InvalidParameterError
        Unknown ``name`` (the message lists the names); ``n_users`` not a
        positive int; ``seed`` not a non-negative int.

    Notes
    -----
    Each dataset generates in about a second at its default size. The generation is
    deterministic for a given ``(name, n_users, seed)`` and NumPy version; tests use
    a cached fixture of the default ``northwind_home`` call regenerated by
    ``scripts/refresh_ab_fixture.py``.

    Examples
    --------
    >>> from retentioneering import datasets
    >>> stream = datasets.load_ab_test()                      # northwind_home
    >>> df = datasets.load_ab_test(as_dataframe=True, with_truth=True)
    >>> lumeo = datasets.load_ab_test("lumeo_smart_cart")
    >>> assignment = datasets.load_ab_test_assignment("lumeo_smart_cart")
    """
    name, n_users, seed = _resolve(name, n_users, seed)
    if name == DEFAULT_AB_TEST:
        events, users = _generate(n_users, seed)
        df = _assemble(events, users, with_truth=with_truth)
        schema = {key: list(value) for key, value in _SCHEMA.items()}
        truth = ["user_type"]
    else:
        case = _CASES[name]
        df, _ = case.generate(n_users, seed)
        schema = {key: list(value) for key, value in case.SCHEMA.items()}
        truth = list(case.TRUTH_COLS)
        if not with_truth:
            df = df.drop(columns=truth)
    if as_dataframe:
        return df

    from retentioneering.eventstream.eventstream import Eventstream

    if with_truth:
        schema["custom_cols"].extend(truth)
    return Eventstream(df, schema)


def load_ab_test_assignment(
    name: str | None = DEFAULT_AB_TEST,
    *,
    n_users: int | None = None,
    seed: int | None = None,
) -> pd.DataFrame:
    """Return the assignment table that goes with :func:`load_ab_test`.

    Parameters
    ----------
    name : str, default "northwind_home"
        The dataset (see :func:`load_ab_test`); ``None`` means the default; an unknown
        name raises ``InvalidParameterError`` listing the names.
    n_users : int, optional
        Must match the ``n_users`` passed to :func:`load_ab_test`.
    seed : int, optional
        Must match the ``seed`` passed to :func:`load_ab_test`.

    Returns
    -------
    pandas.DataFrame
        Indexed by the assigned unit (``user_id``; ``session_id`` for
        ``session_assignment``, which assigned sessions); columns ``arm``
        (``"treatment"`` / ``"control"``) and ``assigned_at`` (Timestamp: the moment
        of assignment), plus columns fixed at assignment where the dataset's
        docstring says so (``platform``). For ``northwind_home``: ``assigned_at``
        is the user's first app open (the user's tracked events, if any, all come
        later), and the table includes silent users, who were assigned but
        produced no tracked event. Where a dataset's table differs from the true
        assignment (a logging bug, a marketing table), its docstring says how.

    Raises
    ------
    InvalidParameterError
        As in :func:`load_ab_test`.

    Examples
    --------
    >>> from retentioneering import datasets
    >>> assignment = datasets.load_ab_test_assignment()
    >>> push = datasets.load_ab_test_assignment("push_campaign")
    """
    name, n_users, seed = _resolve(name, n_users, seed)
    if name == DEFAULT_AB_TEST:
        _, users = _generate(n_users, seed)
        return _assignment(users)
    return _CASES[name].generate(n_users, seed)[1]
