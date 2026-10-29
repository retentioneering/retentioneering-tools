"""Synthetic A/B case "trial conversion": a paywall test read too early.

About 10,000 users of a subscription design app, each starting a 14-day free
trial, and a 50/50 test of an early paywall. Stories are planted in the data; the
analysis is the exercise, and the solutions are at the end of this docstring.

The app and the test
--------------------
Users are assigned when they start the trial (``assigned_at``, the
``trial_start`` event), 50/50, on any day of the six weeks from Monday
2026-01-05; the data end on 2026-02-18 00:00, three days after the last trial
started, when the product team asked for a readout. Trial users open the app
(``app_open``) and work in it (``project_create``, ``editor_use``,
``export``); the trial ends on day 14 (``trial_end``, logged by the backend for
every trial that ended before the data end) and the app asks for a plan
(``paywall_view``, ``plan_select``, ``subscribe``). A user may still subscribe in
the grace week after the trial; nobody subscribes later than day 20.

Where the treatment sits: on day 3 of the trial, treatment users who open the app
see an annual-plan offer (``annual_offer_view``, treatment only) that leads to the
paywall. Exposure starts at that view; nothing differs before day 3.

Fixed at assignment (pre-exposure, declare ``strata_pre_exposure=True``):
``platform`` (``"ios"`` / ``"android"``) and ``cohort_week`` (trial start week,
``"W1"`` .. ``"W6"``).

Schema
------
``path_cols=["user_id", "session_id"]``, ``segment_cols=["exp_group",
"platform", "cohort_week"]``, no custom columns. User ids start at 600001.
``with_truth=True`` adds ``planted_subscribe_day`` (custom column, float: the day
since trial start on which the user subscribes or would subscribe, NaN for users
who never do).

Solutions (spoilers)
--------------------
1. The early offer moves conversion earlier, not up: 18 % of users subscribe in
   both arms, all by day 20. In control they subscribe around the end of the
   trial (days 11-20, most on day 14); in treatment 70 % of the subscribers take
   the day-3 offer (days 3-6) and the rest subscribe as in control.
2. The whole-window read (ITT over the table, no horizon) shows an increase of
   about +3 pp with a small p-value (+3.3 pp, p < 0.001 at the default seed): the recent cohorts are immature. A user who
   started the trial ten days before the data end has had the chance to take the
   day-3 offer but not yet to reach the end of the trial, so treatment conversions
   are counted and control conversions are not yet.
3. ``horizon="21D"`` (the trial plus the grace week) gives every user the same
   window and keeps only users whose window closed (``immature_paths`` counts the
   others): the delta is near 0 and inconclusive.
4. Since-assignment bins show the shift: with ``period="7D",
   period_basis="since_assignment"`` the ``"0-6"`` bin increases and the
   ``"14-20"`` bin decreases.

Correct conclusion: declare the horizon (21 days) before reading a trial test;
at 21 days the offer brings conversions forward and does not add subscribers.
The whole-window lift is an artefact of immature cohorts.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from retentioneering.datasets.ab_cases import _sim

NAME = "trial_conversion"
DESCRIPTION = (
    "Subscription design app with a 14-day free trial, 50/50 test of an early "
    "paywall offer (10,000 users, 6 weeks of trial starts)."
)
DEFAULT_N_USERS = 10_000
DEFAULT_SEED = 2026
FIRST_USER_ID = 600_001
SCHEMA = {
    "path_cols": ["user_id", "session_id"],
    "segment_cols": ["exp_group", "platform", "cohort_week"],
    "custom_cols": [],
}
TRUTH_COLS = ("planted_subscribe_day",)

START = pd.Timestamp("2026-01-05")  # a Monday
ENROL_DAYS = 42
N_DAYS = 44

EVENTS = (
    "trial_start",
    "app_open",
    "project_create",
    "editor_use",
    "export",
    "annual_offer_view",
    "trial_end",
    "paywall_view",
    "plan_select",
    "subscribe",
)
(
    _TRIAL_START,
    _OPEN,
    _PROJECT,
    _EDITOR,
    _EXPORT,
    _OFFER,
    _TRIAL_END,
    _PAYWALL,
    _PLAN,
    _SUBSCRIBE,
) = range(len(EVENTS))
_N_EVENTS = len(EVENTS)
_N_STATES = _N_EVENTS + 1
_EXIT = _N_EVENTS

_PLATFORMS = ("ios", "android")
_PLATFORM_P = (0.55, 0.45)
_WEEKDAY = (1.10, 1.05, 1.00, 1.00, 0.95, 0.90, 0.95)
_CONVERT_P = 0.18
_EARLY_SHARE = 0.70  # treatment subscribers who take the day-3 offer
# control (and late treatment) subscribe day: (first day, last day, weight)
_LATE_DAYS = ((11, 13, 0.15), (14, 14, 0.60), (15, 20, 0.25))
_EARLY_DAYS = (3, 6)
_TRIAL_DAYS = 14
_RATE = {"subscriber": 0.55, "other": 0.22}  # app sessions per day in the trial
_AFTER_RATE = 0.30  # subscribers after subscribing
_PAYWALL_BROWSE_P = 0.12  # a trial session that peeks at the paywall

_WORK = {
    _OPEN: {_EDITOR: 55, _PROJECT: 25, _EXIT: 20},
    _PROJECT: {_EDITOR: 80, _EXIT: 20},
    _EDITOR: {_EDITOR: 45, _EXPORT: 18, _PROJECT: 7, _EXIT: 30},
    _EXPORT: {_EDITOR: 40, _EXIT: 60},
}


def _late_day(rng: np.random.Generator, n: int) -> np.ndarray:
    weights = np.asarray([w for _, _, w in _LATE_DAYS])
    which = rng.choice(len(_LATE_DAYS), size=n, p=weights / weights.sum())
    low = np.asarray([lo for lo, _, _ in _LATE_DAYS])[which]
    high = np.asarray([hi for _, hi, _ in _LATE_DAYS])[which]
    # a day d means [d, d + 1); 0.95 keeps the subscribe events of day 20 before 21D
    return low + rng.random(n) * (high - low + 0.95)


def generate(n_users: int, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(events, assignment)``; ``events`` includes the truth column."""
    n = _sim.check_n_users(n_users)
    rng_users, rng_sessions, rng_browse, rng_funnel, rng_time = _sim.streams(seed, 5)

    # ── users and the planted subscribe day ──────────────────────────────────
    treated = rng_users.random(n) < 0.5
    platform = rng_users.choice(len(_PLATFORMS), size=n, p=_PLATFORM_P)
    start_s = _sim.arrival_seconds(rng_users, n, ENROL_DAYS, _WEEKDAY)
    subscriber = rng_users.random(n) < _CONVERT_P
    early = treated & (rng_users.random(n) < _EARLY_SHARE)
    sub_day = np.where(
        early,
        _EARLY_DAYS[0] + rng_users.random(n) * (_EARLY_DAYS[1] - _EARLY_DAYS[0] + 1),
        _late_day(rng_users, n),
    )
    sub_day = np.where(subscriber, sub_day, np.nan)
    end_s = N_DAYS * _sim.DAY_S
    sub_s = start_s + np.nan_to_num(sub_day, nan=1e6) * _sim.DAY_S
    sub_s = (sub_s // 60 * 60).astype(np.int64)
    subscribes = subscriber & (sub_s < end_s - 2 * 3600)

    # ── sessions: the trial start, app sessions, the subscribe session ───────
    trial_end_s = start_s + _TRIAL_DAYS * _sim.DAY_S
    rate = np.where(subscriber, _RATE["subscriber"], _RATE["other"]) * rng_users.gamma(
        3.0, 1 / 3.0, n
    )
    s_user, s_start, _ = _sim.sessions(
        rng_sessions, start_s, rate, end_s, gap_s=3 * 3600
    )
    # app sessions of non-subscribers stop at the end of the trial; subscribers
    # slow down after subscribing
    keep = (s_start < trial_end_s[s_user]) | (
        subscriber[s_user]
        & (rng_sessions.random(len(s_user)) < _AFTER_RATE / _RATE["subscriber"])
    )
    s_user, s_start = s_user[keep], s_start[keep]
    is_first = np.r_[True, s_user[1:] != s_user[:-1]]
    kind = np.where(is_first, 0, 1)  # 0 = trial start session, 1 = app session
    # the subscribe session and the backend's trial_end, as sessions of their own
    subs = np.flatnonzero(subscribes)
    ended = np.flatnonzero(trial_end_s < end_s)
    sess_user = np.concatenate([s_user, subs, ended])
    sess_start = np.concatenate([s_start, sub_s[subs], trial_end_s[ended]])
    kind = np.concatenate([kind, np.full(len(subs), 2), np.full(len(ended), 3)])
    order = np.lexsort((sess_start, sess_user))
    sess_user, sess_start, kind = sess_user[order], sess_start[order], kind[order]
    # an app session after the user's subscription is not a trial session
    sess_no = _sim.session_numbers(sess_user)
    n_sess = len(sess_user)

    # ── events ───────────────────────────────────────────────────────────────
    days_in = (sess_start - start_s[sess_user]) / _sim.DAY_S
    head = np.full((n_sess, 2), -1, dtype=np.int16)
    head[kind == 0, 0] = _TRIAL_START
    head[kind != 3, 1] = _OPEN
    head[kind == 3, 0] = _TRIAL_END
    work_rows = kind <= 1
    cdf = _sim.chain_cdf([_WORK], _N_STATES)
    chain, _ = _sim.walk(
        rng_browse,
        cdf,
        np.zeros(n_sess, dtype=np.int64),
        np.full(n_sess, _OPEN),
        14,
        _N_EVENTS,
    )
    chain[~work_rows] = -1
    sub_s_user = np.where(subscribes, sub_s, np.iinfo(np.int64).max)[sess_user]
    before_sub = sess_start < sub_s_user
    # the offer: treatment app sessions from day 3, until the user subscribes
    offer = (
        treated[sess_user]
        & (kind == 1)
        & (days_in >= _EARLY_DAYS[0])
        & (days_in < _TRIAL_DAYS)
        & before_sub
    )
    first_offer = offer & ~np.r_[False, offer[:-1] & (sess_user[1:] == sess_user[:-1])]
    tail = np.full((n_sess, 3), -1, dtype=np.int16)
    tail[first_offer, 0] = _OFFER
    peek = work_rows & before_sub & (rng_funnel.random(n_sess) < _PAYWALL_BROWSE_P)
    tail[peek, 1] = _PAYWALL
    sub_rows = kind == 2
    sub_offer = sub_rows & early[sess_user]
    sub_post = np.full((n_sess, 3), -1, dtype=np.int16)
    sub_post[sub_offer, 0] = _OFFER
    sub_post[sub_rows, 1] = _PAYWALL
    sub_post[sub_rows, 2] = _PLAN
    final = np.full((n_sess, 1), -1, dtype=np.int16)
    final[sub_rows, 0] = _SUBSCRIBE

    grid = np.concatenate([head, chain, tail, sub_post, final], axis=1)
    ev_sess, ev_code, ev_t = _sim.flatten(grid, sess_start, rng_time, 4, 60)
    # the backend logs trial_start at the assignment second
    is_start = ev_code == _TRIAL_START
    ev_t[is_start] = sess_start[ev_sess[is_start]]

    # ── frame ────────────────────────────────────────────────────────────────
    ev_user = sess_user[ev_sess]
    user_id = FIRST_USER_ID + np.arange(n, dtype=np.int64)
    arm = np.where(treated, "treatment", "control")
    cohort = np.asarray([f"W{w + 1}" for w in range(6)], dtype=object)[
        np.minimum(start_s // (7 * _sim.DAY_S), 5)
    ]
    events = pd.DataFrame(
        {
            "user_id": user_id[ev_user],
            "session_id": _sim.session_ids(user_id[ev_user], sess_no[ev_sess]),
            "event": np.asarray(EVENTS, dtype=object)[ev_code],
            "timestamp": START + pd.to_timedelta(ev_t, unit="s"),
            "exp_group": arm[ev_user],
            "platform": np.asarray(_PLATFORMS, dtype=object)[platform][ev_user],
            "cohort_week": cohort[ev_user],
            "planted_subscribe_day": np.round(sub_day, 2)[ev_user],
        }
    )
    assignment = pd.DataFrame(
        {
            "arm": arm,
            "assigned_at": START + pd.to_timedelta(start_s, unit="s"),
        },
        index=pd.Index(user_id, name="user_id"),
    )
    return events, assignment
