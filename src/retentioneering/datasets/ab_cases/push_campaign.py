"""Synthetic A/B case "push campaign": a test assigned outside the product.

About 20,000 users of a fashion shopping app, picked by the marketing team for a
push campaign with a 50/50 holdout. Stories are planted in the data; the analysis
is the exercise, and the solutions are at the end of this docstring.

The app and the test
--------------------
The marketing tool drew the audience from its customer table and assigned every
user an arm: treatment users got a push with a promotion, control users got
nothing. It sent in two waves, Tuesday 2026-09-15 and Thursday 2026-09-17 at
10:00 (``assigned_at``, half of the users each). The event stream is the app's
own log from Tuesday 2026-09-08 to 2026-10-01 00:00, two weeks after the second
wave; it holds the users of the table who opened the app in that time and nobody
else. ``exp_group`` on the stream comes from the table.

App sessions start with ``app_open`` and browse ``home``, ``catalog``,
``product_view``; they may continue with ``add_to_cart``, ``checkout_start`` and
``purchase``.

Where the treatment sits: outside the app, in the push. A session opened from the
push starts with ``push_open`` (treatment only) and shows the promotion
(``promo_view``, treatment only) before the catalog. Exposure is the push itself,
at ``assigned_at``; opening the app is already an outcome.

Fixed at assignment (pre-exposure, declare ``strata_pre_exposure=True``):
``platform`` (``"ios"`` / ``"android"``, from the marketing table). The assignment
table carries it too.

The assignment table is the marketing tool's export: one row per assigned user,
``arm``, ``assigned_at`` and ``platform``.

Schema
------
``path_cols=["user_id", "session_id"]``, ``segment_cols=["exp_group",
"platform"]``, custom column ``order_value`` (set on ``purchase`` rows only). User
ids start at 700001. ``with_truth=True`` adds the planted ``compliance`` (custom
column): ``"always"`` (opens the app in the two weeks whatever the arm),
``"push_only"`` (opens only when pushed) or ``"never"``.

Solutions (spoilers)
--------------------
1. Most of the table never opens the app after the push: 24 % of users open it
   anyway, 12 % open it only when pushed, 64 % never do; about 70 % of the
   assigned users have no event after ``assigned_at``. About 5 % of the table
   were active in the week before the push, so they have events before
   ``assigned_at`` (most of them keep using the app, some do not).
2. ITT over the table (``assignment=``) is the experiment: purchase conversion
   rises by about +1.4 pp (users who open anyway buy at 22 % in both arms, the
   users the push brings in buy at 12 %), and the sample ratio of the table is
   fine.
3. The stream-only read (no ``assignment``) compares the users who showed up,
   and the push changed who shows up: treatment has 50 % more users on the
   stream (``get_sample_ratio`` on the stream reports a mismatch, which here is
   the push working, not a bug), the extra users buy less, and the users active
   only before the push count too. Its conversion delta is negative, the
   opposite sign of the ITT effect.

Correct conclusion: analyse ITT over the marketing table (silent users count as
not converted, events before ``assigned_at`` are dropped): the push lifts
purchases by about 1.4 pp of the audience. The stream-only population is selected
by the treatment and must not be compared.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from retentioneering.datasets.ab_cases import _sim

NAME = "push_campaign"
DESCRIPTION = (
    "Fashion shopping app, a push campaign with a 50/50 holdout assigned from the "
    "marketing table (20,000 users, 2 weeks)."
)
DEFAULT_N_USERS = 20_000
DEFAULT_SEED = 2026
FIRST_USER_ID = 700_001
SCHEMA = {
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["exp_group", "platform"],
    "custom_cols": ["order_value"],
}
TRUTH_COLS = ("compliance",)

START = pd.Timestamp("2026-09-08")  # a Tuesday, a week before the first wave
WAVES_S = (7 * _sim.DAY_S + 10 * 3600, 9 * _sim.DAY_S + 10 * 3600)
N_DAYS = 23  # to 2026-10-01 00:00

EVENTS = (
    "app_open",
    "push_open",
    "promo_view",
    "home",
    "catalog",
    "product_view",
    "add_to_cart",
    "checkout_start",
    "purchase",
)
(
    _OPEN,
    _PUSH_OPEN,
    _PROMO,
    _HOME,
    _CATALOG,
    _PRODUCT,
    _ADD,
    _CHECKOUT,
    _PURCHASE,
) = range(len(EVENTS))
_N_EVENTS = len(EVENTS)
_N_STATES = _N_EVENTS + 1
_EXIT = _N_EVENTS

_PLATFORMS = ("ios", "android")
_PLATFORM_P = (0.5, 0.5)
_COMPLIANCE = ("always", "push_only", "never")
_COMPLIANCE_P = (0.24, 0.12, 0.64)
_BUY = {"always": 0.22, "push_only": 0.12}
_PRE_ACTIVE_P = {"always": 0.15, "never": 0.022}  # about 5 % of the table
_PRE_BUY_P = 0.25
_PUSH_FIRST_P = 0.6  # treatment "always" users whose first session is the push
_RATE = 0.15  # return sessions per day after the first one
_PRE_RATE = 0.35
_CART_ONLY_P = 0.12
_ORDER_LOG_MU, _ORDER_LOG_SIGMA = 4.0, 0.5

_BROWSE = {
    _HOME: {_CATALOG: 70, _PRODUCT: 10, _EXIT: 20},
    _PROMO: {_CATALOG: 75, _PRODUCT: 10, _EXIT: 15},
    _CATALOG: {_PRODUCT: 62, _CATALOG: 16, _EXIT: 22},
    _PRODUCT: {_PRODUCT: 24, _CATALOG: 30, _EXIT: 46},
}


def generate(n_users: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(events, assignment)``; ``events`` includes the truth column."""
    n = _sim.check_n_users(n_users)
    rng_users, rng_sessions, rng_browse, rng_funnel, rng_time, rng_orders = _sim.streams(
        seed, 6
    )
    end_s = N_DAYS * _sim.DAY_S

    # ── the table and the planted compliance types ───────────────────────────
    treated = rng_users.random(n) < 0.5
    platform = rng_users.choice(len(_PLATFORMS), size=n, p=_PLATFORM_P)
    wave = rng_users.random(n) < 0.5
    assigned_s = np.where(wave, WAVES_S[1], WAVES_S[0]).astype(np.int64)
    comp = rng_users.choice(len(_COMPLIANCE), size=n, p=_COMPLIANCE_P)
    always, push_only, never = comp == 0, comp == 1, comp == 2
    opens = always | (push_only & treated)
    buys = opens & (
        rng_users.random(n) < np.where(always, _BUY["always"], _BUY["push_only"])
    )
    pre_active = (always & (rng_users.random(n) < _PRE_ACTIVE_P["always"])) | (
        never & (rng_users.random(n) < _PRE_ACTIVE_P["never"])
    )
    pre_buys = pre_active & (rng_users.random(n) < _PRE_BUY_P)

    # ── sessions after the push ──────────────────────────────────────────────
    by_push = treated & (push_only | (always & (rng_users.random(n) < _PUSH_FIRST_P)))
    delay = np.where(
        by_push,
        rng_users.integers(60, 3 * 3600, n),
        rng_users.random(n) * (end_s - 3 * 3600 - assigned_s),
    ).astype(np.int64)
    openers = np.flatnonzero(opens)
    first_s = assigned_s[openers] + delay[openers]
    rate = _RATE * rng_users.gamma(2.0, 0.5, len(openers))
    p_user, p_start, _ = _sim.sessions(rng_sessions, first_s, rate, end_s)
    p_user = openers[p_user]
    p_first = np.r_[True, p_user[1:] != p_user[:-1]]
    p_push = p_first & by_push[p_user]
    # ── sessions in the week before the push ─────────────────────────────────
    pre_users = np.flatnonzero(pre_active)
    pre_first = rng_users.integers(0, 3 * _sim.DAY_S, len(pre_users))
    q_user, q_start, _ = _sim.sessions(
        rng_sessions,
        pre_first,
        np.full(len(pre_users), _PRE_RATE),
        WAVES_S[0] - 3 * 3600,
    )
    q_user = pre_users[q_user]
    keep = q_start < assigned_s[q_user] - 3600
    q_user, q_start = q_user[keep], q_start[keep]

    sess_user = np.concatenate([p_user, q_user])
    sess_start = np.concatenate([p_start, q_start])
    sess_push = np.concatenate([p_push, np.zeros(len(q_user), dtype=bool)])
    sess_post = np.concatenate(
        [np.ones(len(p_user), dtype=bool), np.zeros(len(q_user), dtype=bool)]
    )
    order = np.lexsort((sess_start, sess_user))
    sess_user, sess_start = sess_user[order], sess_start[order]
    sess_push, sess_post = sess_push[order], sess_post[order]
    sess_no = _sim.session_numbers(sess_user)
    n_sess = len(sess_user)

    # one purchase session per buyer: a random session of the right period
    rand = rng_sessions.random(n_sess)
    buy_sess = np.zeros(n_sess, dtype=bool)
    for period_mask, wants in ((sess_post, buys), (~sess_post, pre_buys)):
        cand = np.flatnonzero(period_mask & wants[sess_user])
        if len(cand):
            best = pd.Series(rand[cand]).groupby(sess_user[cand]).idxmax().to_numpy()
            buy_sess[cand[best]] = True
    add_sess = buy_sess | (rng_sessions.random(n_sess) < _CART_ONLY_P)

    head = np.full((n_sess, 3), -1, dtype=np.int16)
    head[:, 0] = _OPEN
    head[sess_push, 1] = _PUSH_OPEN
    head[sess_push, 2] = _PROMO
    head[~sess_push, 2] = _HOME
    cdf = _sim.chain_cdf([_BROWSE], _N_STATES)
    chain, _ = _sim.walk(
        rng_browse,
        cdf,
        np.zeros(n_sess, dtype=np.int64),
        np.where(sess_push, _PROMO, _HOME),
        14,
        _N_EVENTS,
    )
    chain_len = (chain >= 0).sum(1)
    last_chain = chain[np.arange(n_sess), np.maximum(chain_len - 1, 0)]
    tail = np.full((n_sess, 1), -1, dtype=np.int16)
    tail[add_sess & ((chain_len == 0) | (last_chain != _PRODUCT)), 0] = _PRODUCT
    post = np.full((n_sess, 3), -1, dtype=np.int16)
    a = np.flatnonzero(add_sess)
    post[a, 0] = _ADD
    checkout = buy_sess[a] | (rng_funnel.random(len(a)) < 0.3)
    post[a[checkout], 1] = _CHECKOUT
    post[a[buy_sess[a]], 2] = _PURCHASE

    grid = np.concatenate([head, chain, tail, post], axis=1)
    ev_sess, ev_code, ev_t = _sim.flatten(grid, sess_start, rng_time)

    # ── frame ────────────────────────────────────────────────────────────────
    ev_user = sess_user[ev_sess]
    user_id = FIRST_USER_ID + np.arange(n, dtype=np.int64)
    is_buy = ev_code == _PURCHASE
    value = np.full(len(ev_code), np.nan)
    value[is_buy] = _sim.money(
        rng_orders, int(is_buy.sum()), _ORDER_LOG_MU, _ORDER_LOG_SIGMA
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
            "compliance": np.asarray(_COMPLIANCE, dtype=object)[comp][ev_user],
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
