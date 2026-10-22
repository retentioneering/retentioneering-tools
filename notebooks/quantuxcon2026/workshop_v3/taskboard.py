"""Taskboard: a synthetic event log of a team task-tracker, for the workshop.

Users adopt the product gradually, visit by visit: onboarding, then working
alone, then with their team, then with power features. Several stories are
planted in the data so the workshop can find them:

1. ADOPTION LADDER. Every visit has an intent (onboarding, solo work,
   teamwork, power use, planning, a quick check-in), but it only shifts the
   mix of everyday actions, so visits of different intents overlap; checking
   on an invite and paying happen inside such visits. Clustered on everyday
   actions and collapsed into one event per visit, paths read as a ladder:
   who climbs, who stalls.

2. A-HA MOMENT: a teammate joins (`teammate_joined`, when someone accepts the
   user's invite). Users who reach it climb to collaboration and power use,
   stay and pay; the rest plateau in solo work or leave.

3. FUNNEL signup → create_project → invite_member → upgrade. Many users invite
   someone and still never upgrade: their invite isn't accepted. After
   `invite_member` they keep checking the invite (`view_invite_status`,
   `resend_invite`) and never get to `teammate_joined`. Waiting alone doesn't
   tell the story: many whose teammate does join waited a visit or two too.

4. ANOMALY: for a week (CHECKOUT_BUG) the payment form fails for most users:
   `start_checkout` → `payment_error` → `support_ticket`, and daily upgrades
   drop. Outside that week checkout works.

5. A/B TEST of Timeline, a Gantt-style planning view. Every new user is
   assigned to `ab_group` control or test; in test, a new project opens on
   Timeline with a "Plan with your team" prompt. The team hoped planning with
   assignees would bring teams in. It doesn't: team activation
   (`teammate_joined`) and upgrades barely move, every funnel step is grey.
   Two things hide behind the grey numbers:
   - BOUNCE BACK. Some teams fall apart right after they form: the user gets
     past `invite_member` and `teammate_joined`, works with the team once and
     goes back to working alone. The funnel counts them as activated; the
     graph and the step matrix show them coming back. The prompt makes a few
     more such fragile teams in test.
   - PLANNERS. A modest share of users (freelancers and consultants planning
     client work) love the timeline: they plan visit after visit
     (`view_timeline`, `set_due_date`, `add_dependency`) and stay much longer.
     In control most of them never find the view; in test most do. Clustered
     by behavior, the planner cluster is mostly test users. Planners work
     alone, so they don't move team activation.

Segments: `channel` (organic / ads / invite), `platform` (web / mobile),
`ab_group` (control / test). Paths: `user_id`, and `session_id` for visits.
Deterministic for a given seed.
"""

from __future__ import annotations

import math
import random

import pandas as pd

START = pd.Timestamp("2026-03-02")  # a Monday
DAYS = 56  # eight weeks of data
SIGNUP_DAYS = 42  # users sign up during the first six weeks
CHECKOUT_BUG = ("2026-04-06", "2026-04-13")  # payment form broken (week 6)

# Which kind of visit each event belongs to (named like the visit types the
# workshop recovers by clustering), for coloring figures by stage.
EVENT_STAGE = {
    **dict.fromkeys(
        ["signup", "tour_step", "template_view", "create_project"], "onboarding"
    ),
    **dict.fromkeys(
        ["open_board", "create_task", "complete_task", "edit_task"], "solo"
    ),
    **dict.fromkeys(
        [
            "invite_member",
            "accept_invite",
            "teammate_joined",
            "comment",
            "mention",
            "assign_task",
        ],
        "teamwork",
    ),
    **dict.fromkeys(
        ["view_report", "create_automation", "connect_integration", "export"], "power"
    ),
    **dict.fromkeys(["view_timeline", "set_due_date", "add_dependency"], "planning"),
    "view_notifications": "check_in",
    **dict.fromkeys(["view_invite_status", "resend_invite"], "waiting"),
    **dict.fromkeys(
        [
            "view_pricing",
            "select_plan",
            "start_checkout",
            "enter_payment",
            "upgrade",
            "payment_error",
            "support_ticket",
        ],
        "billing",
    ),
}

# The everyday actions, the features for clustering visits along with
# CHECKOUT: everything but signup, the invite and setup events. Those are
# rare, and with standardized counts a rare event weighs so much that k-means
# would spend a cluster on each of them.
EVERYDAY = [
    "open_board",
    "create_task",
    "complete_task",
    "edit_task",
    "comment",
    "assign_task",
    "mention",
    "view_notifications",
    "view_report",
    "export",
    "view_timeline",
    "set_due_date",
    "add_dependency",
    "tour_step",
    "template_view",
    "create_project",
]

# Checkout, the other clustering features: a visit with a checkout is a kind
# of its own. (Invite events are left out: checking on an invite happens in
# passing, in visits of any kind.)
CHECKOUT = [
    "view_pricing",
    "select_plan",
    "start_checkout",
    "enter_payment",
    "upgrade",
    "payment_error",
    "support_ticket",
]

ARCHETYPES = {
    # name: (share, channel weights {organic, ads})
    "adopter": (0.25, {"organic": 0.7, "ads": 0.3}),
    "solo": (0.25, {"organic": 0.5, "ads": 0.5}),
    "staller": (0.28, {"organic": 0.3, "ads": 0.7}),
    "invited": (0.12, None),  # joins through a teammate's invite
    "planner": (0.10, {"organic": 0.6, "ads": 0.4}),  # plans client work alone
}

# The A/B test: how likely a planner finds the Timeline view, and how likely
# an adopter's team falls apart right after it forms, by group.
FINDS_TIMELINE = {"control": 0.25, "test": 0.9}

# A UI problem on the timeline: the task bars look clickable, but aren't. Some
# planners keep clicking them; all the log shows is the timeline opened again
# and again (`view_timeline` → `view_timeline`), and these planners give up
# after a few planning visits. Drawn from a stream of its own (UX_SEED), and
# the visits they no longer make are still generated, just not written, so
# that every other number in the data stays as it is.
STUCK_ON_BARS = 0.4  # the share of planners who found the timeline
UX_SEED = 7
FRAGILE_TEAM = {"control": 0.30, "test": 0.32}


def _shuffled(events: list[str], rng: random.Random) -> list[str]:
    events = list(events)
    rng.shuffle(events)
    return events


def _poisson(lam: float, rng: random.Random) -> int:
    """A Poisson draw (Knuth's method; the rates here are small)."""
    limit, k, p = math.exp(-lam), 0, rng.random()
    while p > limit:
        k += 1
        p *= rng.random()
    return k


# What a visit is mostly about (its intent) only shifts how often each action
# happens in it: every visit is a mix of the same everyday actions, in random
# amounts. Expected counts per visit, by intent.
INTENSITY = {
    "solo": {
        "create_task": 2.2,
        "complete_task": 2.0,
        "edit_task": 0.7,
        "comment": 0.15,
        "view_notifications": 0.25,
        "view_report": 0.05,
        "view_timeline": 0.08,
    },
    "teamwork": {
        "create_task": 0.7,
        "complete_task": 0.9,
        "edit_task": 0.3,
        "comment": 1.9,
        "assign_task": 0.9,
        "mention": 0.6,
        "view_notifications": 0.4,
        "view_report": 0.1,
    },
    "power": {
        "create_task": 0.5,
        "complete_task": 0.8,
        "edit_task": 0.3,
        "comment": 0.5,
        "assign_task": 0.25,
        "view_report": 2.2,
        "export": 1.0,
        "view_notifications": 0.2,
    },
    "planning": {
        "view_timeline": 0.05,
        "set_due_date": 2.0,
        "add_dependency": 1.1,
        "create_task": 0.7,
        "complete_task": 0.5,
        "edit_task": 0.4,
        "comment": 0.1,
    },
}
# Besides what a visit is about, any visit (onboarding aside) now and then has
# a bit of everything else: expected counts added to every intent's. Team
# actions only for users who have a team.
BACKGROUND = {
    "comment": 0.12,
    "edit_task": 0.12,
    "view_notifications": 0.25,
    "view_report": 0.08,
    "export": 0.03,
    "view_timeline": 0.03,
    "set_due_date": 0.06,
    "add_dependency": 0.02,
    "create_automation": 0.01,
    "connect_integration": 0.01,
    "view_pricing": 0.02,
}
TEAM_BACKGROUND = {"assign_task": 0.06, "mention": 0.05}

# A quick visit (`checkin`) is everyday work, only less of it, and more of a
# look at the notifications: a light solo or teamwork visit, not a kind of its
# own.
LIGHT = 0.35


def _session(
    intent: str, rng: random.Random, first: bool = False, team: bool = True
) -> list[str]:
    """The events of one visit: how it opens, then a mix of actions in random
    amounts (INTENSITY), in random order. `team` is whether the user works
    with anyone yet; without a team there is little to comment on."""
    if intent in ("onboarding", "abandoned_onboarding"):
        events = ["signup"] if first else []
        intro = ["tour_step"] * (1 + _poisson(1.3, rng))
        if rng.random() < 0.6:  # a template, looked at during or after the tour
            intro.insert(rng.randint(0, len(intro)), "template_view")
        events += intro
        if intent == "abandoned_onboarding":  # leaves before creating a project
            return events
        body = ["open_board"] * (rng.random() < 0.85) + ["create_task"] * _poisson(
            1.0, rng
        )
        body += ["edit_task"] * _poisson(0.2, rng)
        return events + ["create_project"] + _shuffled(body, rng)
    if intent == "join":  # first visit of an invited user: straight into teamwork
        return ["accept_invite"] + _session("teamwork", rng)
    if intent == "checkin":
        rates = {
            e: lam * LIGHT
            for e, lam in INTENSITY["teamwork" if team else "solo"].items()
        }
        rates["view_notifications"] = rates.get("view_notifications", 0) + 0.15
    else:
        rates = dict(INTENSITY[intent])
    for event, lam in {**BACKGROUND, **(TEAM_BACKGROUND if team else {})}.items():
        rates[event] = rates.get(event, 0) + lam
    if not team:
        rates["comment"] = min(rates.get("comment", 0), 0.15)
        rates.pop("assign_task", None)
        rates.pop("mention", None)
    body = [e for e, lam in rates.items() for _ in range(_poisson(lam, rng))]
    if intent == "planning":
        opening = (
            ["view_timeline"]
            if rng.random() < 0.75
            else ["open_board", "view_timeline"]
        )
    elif rng.random() < (0.25 if intent == "checkin" else 0.15):
        opening = ["view_notifications", "open_board"]  # came in from a notification
    else:
        opening = ["open_board"]
    return opening + _shuffled(body, rng)


def _checkout(at: pd.Timestamp, rng: random.Random) -> tuple[list[str], bool]:
    """A billing visit starting at `at`; returns (events, upgraded). Some users
    compare plans or walk away before paying; during CHECKOUT_BUG the payment
    form fails three times out of four. (A visit lasts minutes and starts in
    the daytime, so it never crosses midnight into or out of the bug week.)"""
    events = ["view_pricing"] * (1 + _poisson(0.3, rng))
    events += ["select_plan"] * (1 + (rng.random() < 0.2))  # some switch plans
    if rng.random() < 0.12:  # changes their mind before checkout
        return events, False
    events += ["start_checkout"] + ["enter_payment"] * (
        1 + (rng.random() < 0.15)
    )  # a typo

    bug = pd.Timestamp(CHECKOUT_BUG[0]) <= at < pd.Timestamp(CHECKOUT_BUG[1])
    if bug and rng.random() < 0.75:
        events += ["payment_error"]
        if rng.random() < 0.7:
            events += ["support_ticket"]
        return events, False
    return events + ["upgrade"], True


def _user(uid: int, rng: random.Random, ux: random.Random) -> list[dict]:
    names = list(ARCHETYPES)
    kind = rng.choices(names, weights=[ARCHETYPES[n][0] for n in names])[0]
    weights = ARCHETYPES[kind][1]
    channel = (
        "invite"
        if kind == "invited"
        else rng.choices(list(weights), list(weights.values()))[0]
    )
    platform = "mobile" if rng.random() < 0.3 else "web"
    group = "test" if rng.random() < 0.5 else "control"
    signup_day = rng.randrange(SIGNUP_DAYS)  # one of the first six weeks
    end = DAYS * 86400  # seconds since START: the end of the data

    # The plan of visits, by archetype.
    plan: list[str] = []
    if kind == "staller":
        first = "abandoned_onboarding" if rng.random() < 0.45 else "onboarding"
        plan = [first] + ["checkin"] * rng.randint(0, 2)
    elif kind == "solo":
        plan = ["onboarding"] + ["solo"] * rng.randint(2, 5)
        if rng.random() < 0.7:  # invites someone, who mostly never comes
            plan += ["invite"] + ["waiting"] * rng.randint(1, 2)
            if rng.random() < 0.15:
                # The teammate comes after all, late, and doesn't stay.
                plan += ["aha"] + ["solo"] * rng.randint(1, 2)
        plan += rng.choices(["solo", "checkin"], k=rng.randint(1, 4))
        if rng.random() < 0.06:  # a few pay for themselves anyway
            plan.append("billing")
    elif kind == "adopter":
        plan = ["onboarding"] + ["solo"] * rng.randint(1, 3) + ["invite"]
        plan += ["waiting"] * rng.choice([0, 0, 1, 1, 2])  # the teammate takes a while
        plan += ["aha"]
        if rng.random() < FRAGILE_TEAM[group]:
            # Bounce back: one visit together, then back to working alone.
            plan += ["collaboration"] + ["solo"] * rng.randint(2, 4)
            plan += ["checkin"] * rng.randint(0, 2)
            if rng.random() < 0.1:
                plan.append("billing")
        else:
            plan += ["collaboration"] * rng.randint(2, 4)
            plan += rng.choices(
                ["collaboration", "power", "checkin"], [3, 3, 1], k=rng.randint(3, 7)
            )
            if rng.random() < 0.75:
                plan.insert(rng.randint(len(plan) - 4, len(plan)), "billing")
    elif kind == "planner":
        plan = ["onboarding"] + ["solo"] * rng.randint(1, 2)
        found = rng.random() < FINDS_TIMELINE[group]
        if found:
            # Found the timeline: plans visit after visit, and stays.
            plan += rng.choices(
                ["planning", "solo", "checkin"], [6, 3, 1], k=rng.randint(6, 12)
            )
            if rng.random() < 0.1:
                plan.append("billing")
        else:
            # Never found it: a few more tasks, then gone.
            plan += ["solo"] * rng.randint(0, 2) + ["checkin"] * rng.randint(0, 2)
            if rng.random() < 0.02:
                plan.append("billing")
        if rng.random() < 0.12:  # works alone, but now and then invites a client
            plan.insert(rng.randint(2, len(plan)), "invite")
            plan.insert(plan.index("invite") + 1, "waiting")
    else:  # invited: lands in collaboration through the invite
        plan = ["join"] + ["collaboration"] * rng.randint(1, 3)
        plan += rng.choices(
            ["collaboration", "solo", "power", "checkin"],
            [3, 2, 2, 1],
            k=rng.randint(2, 5),
        )
        if rng.random() < 0.35:
            plan.insert(rng.randint(len(plan) - 2, len(plan)), "billing")

    stuck = kind == "planner" and found and ux.random() < STUCK_ON_BARS
    gives_up_after = ux.randint(2, 4)  # planning visits, if stuck
    planned = 0

    rows, n = [], 0
    upgraded = False
    powered = False  # set up an automation or an integration already
    team = kind == "invited"  # works with anyone yet
    pending = False  # an invite sent, nobody has joined yet
    # The first visit is on the signup day; each next one starts after the
    # previous one ends: later the same day, or on a later day, in the daytime.
    start = signup_day * 86400 + rng.randint(8, 20) * 3600 + rng.randint(0, 59) * 60
    i = 0
    while i < len(plan):
        visit = plan[i]
        if start >= end:
            break
        at = START + pd.Timedelta(seconds=start)
        everyday = "teamwork" if team else "solo"
        if visit == "invite":  # the invite goes out at the end of a work visit
            intent, events = (
                "solo",
                _session("solo", rng, team=team) + ["invite_member"],
            )
            pending = True
        elif visit == "aha":  # a teammate joins and work turns social
            intent, events = "teamwork", _session("teamwork", rng)
            events.insert(1, "teammate_joined")
            team, pending = True, False
        elif visit == "billing":  # checkout at the end of an everyday visit
            if upgraded:
                i += 1
                continue
            checkout, upgraded = _checkout(at, rng)
            intent = rng.choice(
                [everyday, everyday, "checkin"]
                + (["planning"] if kind == "planner" else [])
                + (["power"] if powered else [])
            )
            events = _session(intent, rng, team=team) + checkout
            if not upgraded and rng.random() < 0.6:  # tries again a bit later
                plan.insert(min(i + 1 + rng.randint(0, 2), len(plan)), "billing")
        elif visit == "waiting":  # waiting for the teammate: a short visit, like any
            intent, events = "checkin", _session("checkin", rng, team=team)
        else:
            intent = {"collaboration": "teamwork"}.get(visit, visit)
            events = _session(
                intent, rng, first=(i == 0), team=team or intent == "teamwork"
            )
            if intent == "power" and not powered:
                events.append(rng.choice(["create_automation", "connect_integration"]))
                powered = True
            elif intent == "power" and rng.random() < 0.12:
                events.append(rng.choice(["create_automation", "connect_integration"]))
            if visit == "onboarding" and group == "test":
                # Test: the new project opens on the timeline; most switch
                # back to the board right away.
                at_ = events.index("create_project") + 1
                events = events[:at_] + ["view_timeline"] + events[at_:]
        if pending and visit != "invite":
            # While the invite hangs, any visit now and then checks on it.
            if rng.random() < 0.35:
                events.insert(rng.randint(1, len(events)), "view_invite_status")
                if rng.random() < 0.4:
                    events.insert(
                        events.index("view_invite_status") + 1, "resend_invite"
                    )
        elif team and intent != "onboarding" and rng.random() < 0.05:
            # A team grows: invite one more person, or check who hasn't come yet.
            at_ = rng.randint(1, len(events))
            events[at_:at_] = rng.choice(
                [
                    ["invite_member"],
                    ["view_invite_status"],
                    ["view_invite_status", "resend_invite"],
                ]
            )
        events = [(e, "rng") for e in events]
        if stuck and intent == "planning":
            # Clicks on a task bar do nothing; the user opens the timeline again.
            at_ = next(k for k, (e, _) in enumerate(events) if e == "view_timeline") + 1
            for _ in range(ux.randint(1, 3)):
                events.insert(at_, ("view_timeline", "ux"))
        t = start
        visit_rows = []
        for event, source in events:
            visit_rows.append(
                (
                    f"user_{uid:05d}",
                    f"user_{uid:05d}_{n + 1:02d}",
                    event,
                    t,
                    channel,
                    platform,
                    group,
                    intent,
                    kind,
                )
            )
            t += (ux if source == "ux" else rng).randint(15, 120)
        if visit_rows[-1][3] >= end:  # the data ends before this visit does
            break
        planned += intent == "planning"
        if not (stuck and planned > gives_up_after):  # gave up: not written
            rows += visit_rows
            n += 1
        gap_days = rng.choice([0, 1, 1, 2, 2, 3, 4, 6])
        if gap_days == 0 and t + 2 * 3600 < (t // 86400) * 86400 + 22 * 3600:
            start = t + rng.randint(1, 3) * 3600 + rng.randint(0, 59) * 60
        else:
            day = t // 86400 + max(gap_days, 1)
            start = day * 86400 + rng.randint(8, 20) * 3600 + rng.randint(0, 59) * 60
        i += 1
    return rows


COLUMNS = [
    "user_id",
    "session_id",
    "event",
    "timestamp",
    "channel",
    "platform",
    "ab_group",
    "visit_kind",
    "archetype",  # ground truth, for checking only
]


def taskboard_events(n_users: int = 20000, seed: int = 18) -> pd.DataFrame:
    """The Taskboard event log as a DataFrame (one row per event)."""
    rng = random.Random(seed)
    ux = random.Random(seed + UX_SEED)  # the timeline UI problem's own stream
    rows = [row for uid in range(1, n_users + 1) for row in _user(uid, rng, ux)]
    df = pd.DataFrame(rows, columns=COLUMNS)
    df["timestamp"] = START + pd.to_timedelta(df["timestamp"], unit="s")
    return df.sort_values(["user_id", "timestamp"], kind="stable").reset_index(
        drop=True
    )


def load_taskboard(n_users: int = 20000, seed: int = 18, keep_truth: bool = False):
    """The Taskboard event log as an Eventstream (users and visits as paths,
    `channel`, `platform` and `ab_group` as segments). `keep_truth` keeps the
    generator's `visit_kind` and `archetype` columns, to check what the
    analysis recovers."""
    import retentioneering as rete

    df = taskboard_events(n_users, seed)
    truth = ["visit_kind", "archetype"]
    if not keep_truth:
        df = df.drop(columns=truth)
    return rete.Eventstream(
        df,
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["channel", "platform", "ab_group"],
            "custom_cols": truth if keep_truth else [],
        },
    )


def check_invariants(df: pd.DataFrame) -> dict[str, int]:
    """Counts of what must never happen in `taskboard_events()` output; all
    zeros means the time and causal order hold. Cheap enough to run after
    every change to the generator."""
    visits = (
        df.assign(n=df["session_id"].str[-2:].astype(int))
        .groupby("session_id")
        .agg(
            user=("user_id", "first"),
            n=("n", "first"),
            start=("timestamp", "min"),
            end=("timestamp", "max"),
        )
        .sort_values(["user", "n"])
    )
    previous_end = visits.groupby("user")["end"].shift()
    first = df.groupby(["user_id", "event"])["timestamp"].min().unstack()

    def before(a: str, b: str) -> int:
        both = first[a].notna() & first[b].notna()
        return int((first.loc[both, a] < first.loc[both, b]).sum())

    bug = df["timestamp"].between(*map(pd.Timestamp, CHECKOUT_BUG), inclusive="left")
    starts_with_signup = (
        df.groupby("user_id")["event"].first().isin(["signup", "accept_invite"])
    )
    return {
        "visit starts before the previous one ends": int(
            (visits["start"] <= previous_end).sum()
        ),
        "events after the end of the data": int(
            (df["timestamp"] >= START + pd.Timedelta(days=DAYS)).sum()
        ),
        "signups after the signup weeks": int(
            (
                df.loc[df["event"] == "signup", "timestamp"]
                >= START + pd.Timedelta(days=SIGNUP_DAYS)
            ).sum()
        ),
        "payment errors outside the bug week": int(
            ((df["event"] == "payment_error") & ~bug).sum()
        ),
        "paths not starting with signup or accept_invite": int(
            (~starts_with_signup).sum()
        ),
        "teammate_joined before invite_member": before(
            "teammate_joined", "invite_member"
        ),
        "upgrade before view_pricing": before("upgrade", "view_pricing"),
        "duplicate (user, timestamp) pairs": int(
            df.duplicated(["user_id", "timestamp"]).sum()
        ),
    }
