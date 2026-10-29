"""Synthetic A/B case "Lumeo smart cart": a cart drawer on a home and living store.

About 15,000 users over twelve weeks of Lumeo, a home and living web store with
iOS and Android apps, and a randomised 50/50 test of the "smart cart drawer".
Stories are planted in the data; the analysis is the exercise, and the solutions
are at the end of this docstring.

The store and the test
----------------------
Users are assigned at their first visit (``assigned_at``), 50/50, on any day of the
first ten weeks from Monday 2026-03-02; the data end on 2026-05-25 00:00, twelve
weeks after the start. Every assigned user has at least one session, and every
event comes after its user's assignment. Sessions start at ``home`` and browse
``catalog``, ``search``, ``product_view``, ``reviews_view``, ``collections`` (room
collections) and ``wishlist_add``; a session may end in ``add_to_cart``.

Where the treatment sits: right after ``add_to_cart``. In control the add shows a
toast (``cart_toast_shown``); in treatment a drawer slides in (``smart_cart_open``,
treatment only) with the buttons "Go to cart" (``smart_cart_go_to_cart``) and
"Continue shopping" (``smart_cart_continue``). Exposure starts at a user's first
``add_to_cart``: everything before it is generated identically in both arms. After
the cart come ``cart``, ``checkout_start``, ``payment`` and ``order_success``.

Fixed at assignment (pre-exposure, declare ``strata_pre_exposure=True``):
``platform`` (``"ios"``, ``"android"``, ``"web"``, the signup platform) and
``cohort_week`` (assignment week ``"W01"`` .. ``"W10"``). The assignment table
carries ``platform`` too.

Schema
------
``path_cols=["user_id", "session_id"]`` (sessions are the second path column),
``segment_cols=["exp_group", "platform", "cohort_week"]``, custom column
``order_value`` (GBP, set on ``order_success`` rows only). User ids start at
200001. ``with_truth=True`` adds the planted ``user_type`` (custom column).

Solutions (spoilers)
--------------------
1. The top line is a true null: the expected conversion (share of assigned users
   with ``order_success``) is the same in both arms (planted delta 0.0 pp), so
   ``get_ab_test`` reads it inconclusive; the sample ratio is fine.
2. A small pre-exposure segment, the "collection builders" (9 % of users, planted
   ``user_type="collection_builder"``), shop by room: their first session opens
   ``collections`` and adds to the wishlist before any ``add_to_cart``, so
   ``has_all_events(["collections", "wishlist_add"])`` on the stream truncated
   before the first ``add_to_cart`` recovers them (other users almost never do
   both). The drawer bridges them from collecting to buying: control builders
   convert at 15 %, treatment builders at about 27 % (+12 pp; the drawer
   helps a builder who does not loop by +18 pp). The rest of the users pay for it
   with -1.2 pp, which is what makes the top line null; that row is usually
   inconclusive (true but small).
3. A drawer-reopen loop exists in treatment only: ``smart_cart_open ->
   smart_cart_open`` (the drawer's recommendation thumbnails look tappable and
   are not). About 38 % of treatment builders loop (50 % on the apps, 15 % on
   the web; a few other treatment users reopen the drawer once by accident). Loopers
   convert at 17 %, far below the builders who do not loop (33 %): the loop is
   where the drawer's gain leaks. This comparison is post-exposure and
   descriptive (loopers are selected by the treatment); the ITT effect is the
   builder stratum of story 2.
4. ``order_value`` is lognormal and does not depend on the arm.

Correct conclusion: do not ship on the top line, and do not call it "no effect".
The drawer helps collection builders (a pre-exposure segment) and costs the rest a
little; fix the non-tappable thumbnails behind the reopen loop, then retest,
pre-declaring the builder stratum.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from retentioneering.datasets.ab_cases import _sim

NAME = "lumeo_smart_cart"
DESCRIPTION = (
    "Home and living store, 50/50 test of a smart cart drawer after add_to_cart "
    "(15,000 users, 12 weeks, sessions as a second path column)."
)
DEFAULT_N_USERS = 15_000
DEFAULT_SEED = 2026
FIRST_USER_ID = 200_001
SCHEMA = {
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["exp_group", "platform", "cohort_week"],
    "custom_cols": ["order_value"],
}
TRUTH_COLS = ("user_type",)

START = pd.Timestamp("2026-03-02")  # a Monday
N_DAYS = 84
ENROL_DAYS = 70

EVENTS = (
    "home",
    "catalog",
    "search",
    "product_view",
    "reviews_view",
    "collections",
    "wishlist_add",
    "add_to_cart",
    "cart_toast_shown",
    "smart_cart_open",
    "smart_cart_go_to_cart",
    "smart_cart_continue",
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
    _COLLECTIONS,
    _WISHLIST,
    _ADD,
    _TOAST,
    _DRAWER,
    _GO_CART,
    _CONTINUE,
    _CART,
    _CHECKOUT,
    _PAYMENT,
    _ORDER,
) = range(len(EVENTS))
_N_EVENTS = len(EVENTS)
_EXIT = _N_EVENTS
_N_STATES = _N_EVENTS + 1

_TYPES = ("other", "collection_builder")
_BUILDER_P = 0.09
_PLATFORMS = ("ios", "android", "web")
_PLATFORM_P = (0.35, 0.30, 0.35)
_WEEKDAY = (1.00, 0.95, 0.95, 1.00, 0.92, 1.10, 1.25)
# planted conversion probabilities (share of users with an order)
_CONV_CONTROL = {"other": 0.30, "collection_builder": 0.15}
_CONV_SMOOTH_BUILDER = 0.33  # treatment builder who does not loop
_CONV_LOOPER = 0.17
_LOOPER_P = {"ios": 0.50, "android": 0.50, "web": 0.15}
_RETURN_RATE = {"other": 0.07, "collection_builder": 0.16}  # sessions per day
_ABANDON_ADD_P = {"other": 0.10, "collection_builder": 0.22}  # add without order
_REPEAT_ORDER_P = 0.12
_ACCIDENTAL_REOPEN_P = 0.02  # treatment non-builder add sessions
_ORDER_LOG_MU, _ORDER_LOG_SIGMA = 4.1, 0.6

_BROWSE = {
    "other": {
        _HOME: {_CATALOG: 60, _SEARCH: 22, _PRODUCT: 6, _COLLECTIONS: 2, _EXIT: 10},
        _CATALOG: {_PRODUCT: 62, _CATALOG: 12, _SEARCH: 6, _EXIT: 20},
        _SEARCH: {_PRODUCT: 66, _SEARCH: 10, _CATALOG: 6, _EXIT: 18},
        _PRODUCT: {_PRODUCT: 22, _CATALOG: 24, _SEARCH: 8, _REVIEWS: 12, _WISHLIST: 2, _EXIT: 32},
        _REVIEWS: {_PRODUCT: 55, _EXIT: 45},
        _COLLECTIONS: {_PRODUCT: 60, _EXIT: 40},
        _WISHLIST: {_PRODUCT: 40, _EXIT: 60},
    },
    "collection_builder": {
        _HOME: {_COLLECTIONS: 70, _CATALOG: 20, _EXIT: 10},
        _CATALOG: {_PRODUCT: 55, _COLLECTIONS: 25, _EXIT: 20},
        _SEARCH: {_PRODUCT: 70, _EXIT: 30},
        _PRODUCT: {_WISHLIST: 26, _COLLECTIONS: 20, _PRODUCT: 16, _REVIEWS: 6, _EXIT: 32},
        _REVIEWS: {_PRODUCT: 55, _EXIT: 45},
        _COLLECTIONS: {_PRODUCT: 64, _COLLECTIONS: 14, _EXIT: 22},
        _WISHLIST: {_PRODUCT: 30, _COLLECTIONS: 40, _EXIT: 30},
    },
}  # fmt: skip


def generate(n_users: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(events, assignment)``; ``events`` includes the truth column."""
    n = _sim.check_n_users(n_users)
    rng_users, rng_sessions, rng_browse, rng_funnel, rng_time, rng_orders = _sim.streams(
        seed, 6
    )

    # ── users and their planted outcomes ─────────────────────────────────────
    builder = rng_users.random(n) < _BUILDER_P
    treated = rng_users.random(n) < 0.5
    platform = rng_users.choice(len(_PLATFORMS), size=n, p=_PLATFORM_P)
    assigned_s = _sim.arrival_seconds(rng_users, n, ENROL_DAYS, _WEEKDAY)
    looper_p = np.asarray([_LOOPER_P[p] for p in _PLATFORMS])[platform]
    looper = treated & builder & (rng_users.random(n) < looper_p)
    p_control = np.where(
        builder, _CONV_CONTROL["collection_builder"], _CONV_CONTROL["other"]
    )
    # the rest of the users pay for the builders' gain: the expected overall delta is 0
    builder_gain = (
        (1 - _mean_looper_p()) * _CONV_SMOOTH_BUILDER
        + _mean_looper_p() * _CONV_LOOPER
        - _CONV_CONTROL["collection_builder"]
    )
    other_loss = _BUILDER_P * builder_gain / (1 - _BUILDER_P)
    p_treated = np.where(
        builder,
        np.where(looper, _CONV_LOOPER, _CONV_SMOOTH_BUILDER),
        _CONV_CONTROL["other"] - other_loss,
    )
    converts = rng_users.random(n) < np.where(treated, p_treated, p_control)
    rate = np.where(
        builder, _RETURN_RATE["collection_builder"], _RETURN_RATE["other"]
    ) * rng_users.gamma(2.0, 0.5, n)

    # ── sessions ─────────────────────────────────────────────────────────────
    sess_user, sess_start, sess_no = _sim.sessions(
        rng_sessions, assigned_s, rate, N_DAYS * _sim.DAY_S
    )
    n_sess = len(sess_user)
    s_builder = builder[sess_user]
    user_first = np.flatnonzero(sess_no == 0)
    n_user_sess = np.diff(np.r_[user_first, n_sess])
    # the order session of a converter: a random session, the last for builders
    pick = np.where(
        builder,
        n_user_sess - 1,
        np.floor(rng_sessions.random(n) * n_user_sess).astype(np.int64),
    )
    order_sess = np.zeros(n_sess, dtype=bool)
    order_sess[(user_first + pick)[converts]] = True
    repeat = (
        ~order_sess
        & converts[sess_user]
        & (sess_no > pick[sess_user])
        & (rng_sessions.random(n_sess) < _REPEAT_ORDER_P)
    )
    order_sess |= repeat
    abandon_p = np.where(
        s_builder, _ABANDON_ADD_P["collection_builder"], _ABANDON_ADD_P["other"]
    )
    add_sess = order_sess | (rng_sessions.random(n_sess) < abandon_p)

    # ── browsing: builders' first session opens a collection and wishlists ───
    prefix = np.full((n_sess, 4), -1, dtype=np.int16)
    prefix[:, 0] = _HOME
    marked = s_builder & ((sess_no == 0) | (rng_browse.random(n_sess) < 0.7))
    prefix[marked, 1] = _COLLECTIONS
    prefix[marked, 2] = _PRODUCT
    prefix[marked, 3] = _WISHLIST
    last = prefix[np.arange(n_sess), (prefix >= 0).sum(1) - 1]
    cdf = _sim.chain_cdf([_BROWSE[t] for t in _TYPES], _N_STATES)
    chain, _ = _sim.walk(
        rng_browse, cdf, s_builder.astype(np.int64), last, 18, _N_EVENTS
    )
    # an add session ends its browsing on a product page
    chain_len = (chain >= 0).sum(1)
    last_chain = chain[np.arange(n_sess), np.maximum(chain_len - 1, 0)]
    on_product = np.where(chain_len > 0, last_chain == _PRODUCT, last == _PRODUCT)
    needs_product = add_sess & ~on_product
    tail = np.full((n_sess, 1), -1, dtype=np.int16)
    tail[needs_product, 0] = _PRODUCT

    # ── after the add ────────────────────────────────────────────────────────
    s_treated = treated[sess_user]
    s_looper = looper[sess_user]
    post = np.full((n_sess, 11), -1, dtype=np.int16)
    a = np.flatnonzero(add_sess)
    post[a, 0] = _ADD
    ctrl = a[~s_treated[a]]
    post[ctrl, 1] = _TOAST
    trt = a[s_treated[a]]
    post[trt, 1] = _DRAWER
    loops = s_looper[trt] & (rng_funnel.random(len(trt)) < 0.85)
    accident = ~s_builder[trt] & (rng_funnel.random(len(trt)) < _ACCIDENTAL_REOPEN_P)
    n_reopen = np.where(loops, 1 + rng_funnel.integers(1, 4, len(trt)), 0)
    n_reopen[accident & ~loops] = 1
    for k in range(4):
        post[trt[n_reopen > k], 2 + k] = _DRAWER
    ordered = order_sess[a]
    to_cart = ordered | (rng_funnel.random(len(a)) < 0.45)
    t_mask = s_treated[a]
    post[a[t_mask & to_cart], 6] = _GO_CART
    post[a[t_mask & ~to_cart & (rng_funnel.random(len(a)) < 0.6)], 6] = _CONTINUE
    post[a[to_cart], 7] = _CART
    checkout = ordered | (to_cart & (rng_funnel.random(len(a)) < 0.25))
    post[a[checkout], 8] = _CHECKOUT
    post[a[ordered], 9] = _PAYMENT
    post[a[ordered], 10] = _ORDER

    grid = np.concatenate([prefix, chain, tail, post], axis=1)
    ev_sess, ev_code, ev_t = _sim.flatten(grid, sess_start, rng_time)

    # ── frame ────────────────────────────────────────────────────────────────
    ev_user = sess_user[ev_sess]
    user_id = FIRST_USER_ID + np.arange(n, dtype=np.int64)
    is_order = ev_code == _ORDER
    value = np.full(len(ev_code), np.nan)
    value[is_order] = _sim.money(
        rng_orders, int(is_order.sum()), _ORDER_LOG_MU, _ORDER_LOG_SIGMA
    )
    cohort = np.char.add("W", np.char.zfill((assigned_s // (7 * _sim.DAY_S) + 1).astype(str), 2))
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
            "cohort_week": cohort.astype(object)[ev_user],
            "order_value": value,
            "user_type": np.asarray(_TYPES, dtype=object)[builder.astype(int)][ev_user],
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


def _mean_looper_p() -> float:
    return float(np.dot(_PLATFORM_P, [_LOOPER_P[p] for p in _PLATFORMS]))
