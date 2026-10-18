"""Taskboard: a synthetic event log of a team task-tracker, for the workshop.

Users adopt the product gradually, visit by visit: onboarding, then working
alone, then with their team, then with power features. Several stories are
planted in the data so the workshop can find them:

1. ADOPTION LADDER. Every visit has a type (onboarding, solo work,
   collaboration, power use, quick check-in, billing). Clustered and collapsed
   into one event per visit, paths read as a ladder: who climbs, who stalls.

2. A-HA MOMENT: a teammate joins (`teammate_joined`, when someone accepts the
   user's invite). Users who reach it climb to collaboration and power use,
   stay and pay; the rest plateau in solo work or leave.

3. FUNNEL signup → create_project → invite_member → upgrade. Many users invite
   someone and still never upgrade: their invite isn't accepted. After
   `invite_member` they loop through `invite_pending` / `resend_invite` and
   never get to `teammate_joined`.

4. ANOMALY: for a week (CHECKOUT_BUG) the payment form fails for most users:
   `start_checkout` → `payment_error` → `support_ticket`, and daily upgrades
   drop. Outside that week checkout works.

Segments: `channel` (organic / ads / invite), `platform` (web / mobile).
Paths: `user_id`, and `session_id` for visits. Deterministic for a given seed.
"""

from __future__ import annotations

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
    "view_notifications": "check_in",
    **dict.fromkeys(["invite_pending", "resend_invite"], "waiting"),
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

ARCHETYPES = {
    # name: (share, channel weights {organic, ads})
    "adopter": (0.28, {"organic": 0.7, "ads": 0.3}),
    "solo": (0.27, {"organic": 0.5, "ads": 0.5}),
    "staller": (0.30, {"organic": 0.3, "ads": 0.7}),
    "invited": (0.15, None),  # joins through a teammate's invite
}


def _shuffled(events: list[str], rng: random.Random) -> list[str]:
    events = list(events)
    rng.shuffle(events)
    return events


def _session(kind: str, rng: random.Random, first: bool = False) -> list[str]:
    """The events of one visit of the given kind.

    Each kind has its own signature events in steady amounts, so the visit
    types stay recoverable by k-means on event counts (the workshop's
    `add_clusters(features=[{"metric": "event_count_bulk"}])`). The order of
    those events and how a visit opens vary, the way real usage does."""
    if kind in ("onboarding", "abandoned_onboarding"):
        events = ["signup"] if first else []
        intro = ["tour_step"] * rng.randint(2, 3)
        if rng.random() < 0.6:  # a template, looked at during or after the tour
            intro.insert(rng.randint(0, len(intro)), "template_view")
        events += intro
        if kind == "abandoned_onboarding":  # leaves before creating a project
            return events
        return (
            events + ["create_project"] + _shuffled(["open_board", "create_task"], rng)
        )
    if kind == "join":  # first visit of an invited user: straight into teamwork
        return ["accept_invite"] + _shuffled(
            ["open_board", "comment", "comment", "mention"], rng
        )
    if kind == "solo":
        body = ["create_task", "create_task", "complete_task"]
        body += rng.choice(
            [[], ["edit_task"], ["complete_task"], ["edit_task", "complete_task"]]
        )
        return ["open_board"] + _shuffled(body, rng)
    if kind == "collaboration":
        body = ["comment", "comment", "assign_task"]
        body += rng.choice([["mention"], ["mention", "comment"], ["complete_task"], []])
        return ["open_board"] + _shuffled(body, rng)
    if kind == "power":
        body = ["view_report", rng.choice(["create_automation", "connect_integration"])]
        body += rng.choice(
            [
                ["export"],
                ["view_report"],
                ["create_automation"],
                ["export", "view_report"],
            ]
        )
        return ["open_board"] + _shuffled(body, rng)
    if kind == "checkin":
        return rng.choice(
            [
                ["open_board"],
                ["view_notifications"],
                ["view_notifications", "open_board"],
                ["open_board", "view_notifications"],
            ]
        )
    if kind == "waiting":  # invited someone, nobody came yet
        events = ["view_notifications", "invite_pending"]
        if rng.random() < 0.6:
            events.insert(rng.randint(1, 2), "resend_invite")
        return events
    raise ValueError(kind)


def _checkout(day: pd.Timestamp, rng: random.Random) -> tuple[list[str], bool]:
    """A billing visit; returns (events, upgraded). Some users compare plans
    or walk away before paying; during CHECKOUT_BUG the payment form fails
    three times out of four."""
    events = ["view_pricing"] * (2 if rng.random() < 0.25 else 1) + ["select_plan"]
    if rng.random() < 0.12:  # changes their mind before checkout
        return events, False
    events += ["start_checkout", "enter_payment"]
    bug = pd.Timestamp(CHECKOUT_BUG[0]) <= day < pd.Timestamp(CHECKOUT_BUG[1])
    if bug and rng.random() < 0.75:
        events += ["payment_error"]
        if rng.random() < 0.7:
            events += ["support_ticket"]
        return events, False
    return events + ["upgrade"], True


def _user(uid: int, rng: random.Random) -> list[dict]:
    names = list(ARCHETYPES)
    kind = rng.choices(names, weights=[ARCHETYPES[n][0] for n in names])[0]
    weights = ARCHETYPES[kind][1]
    channel = (
        "invite"
        if kind == "invited"
        else rng.choices(list(weights), list(weights.values()))[0]
    )
    platform = "mobile" if rng.random() < 0.3 else "web"
    day = START + pd.Timedelta(days=rng.randint(0, SIGNUP_DAYS))
    end = START + pd.Timedelta(days=DAYS)

    # The plan of visits, by archetype.
    plan: list[str] = []
    if kind == "staller":
        first = "abandoned_onboarding" if rng.random() < 0.45 else "onboarding"
        plan = [first] + ["checkin"] * rng.randint(0, 2)
    elif kind == "solo":
        plan = ["onboarding"] + ["solo"] * rng.randint(2, 5)
        if rng.random() < 0.7:  # invites someone, who never comes: no a-ha
            plan += ["invite"] + ["waiting"] * rng.randint(1, 2)
        plan += rng.choices(["solo", "checkin"], k=rng.randint(1, 4))
        if rng.random() < 0.06:  # a few pay for themselves anyway
            plan.append("billing")
    elif kind == "adopter":
        plan = ["onboarding"] + ["solo"] * rng.randint(1, 3) + ["invite", "aha"]
        plan += ["collaboration"] * rng.randint(2, 4)
        plan += rng.choices(
            ["collaboration", "power", "checkin"], [3, 3, 1], k=rng.randint(3, 7)
        )
        if rng.random() < 0.75:
            plan.insert(rng.randint(len(plan) - 4, len(plan)), "billing")
    else:  # invited: lands in collaboration through the invite
        plan = ["join"] + ["collaboration"] * rng.randint(1, 3)
        plan += rng.choices(
            ["collaboration", "solo", "power", "checkin"],
            [3, 2, 2, 1],
            k=rng.randint(2, 5),
        )
        if rng.random() < 0.35:
            plan.insert(rng.randint(len(plan) - 2, len(plan)), "billing")

    rows, n = [], 0
    upgraded = False
    for i, visit in enumerate(plan):
        if day >= end:
            break
        if visit == "invite":  # the invite happens at the end of a solo visit
            events, kind_ = _session("solo", rng) + ["invite_member"], "solo"
        elif visit == "aha":  # a teammate joins and work turns social
            teamwork = [
                e
                for e in _session("collaboration", rng)
                if e not in ("open_board", "view_notifications")
            ]
            events, kind_ = (
                ["open_board", "teammate_joined"] + teamwork,
                "collaboration",
            )
        elif visit == "billing":
            if upgraded:
                continue
            events, upgraded = _checkout(day, rng)
            if not upgraded and rng.random() < 0.6:  # tries again later
                plan.append("billing")
            kind_ = "billing"
        else:
            events, kind_ = _session(visit, rng, first=(i == 0)), visit
        n += 1
        t = day + pd.Timedelta(hours=rng.randint(8, 20), minutes=rng.randint(0, 59))
        for event in events:
            rows.append(
                {
                    "user_id": f"user_{uid:04d}",
                    "session_id": f"user_{uid:04d}_{n:02d}",
                    "event": event,
                    "timestamp": t,
                    "channel": channel,
                    "platform": platform,
                    "visit_kind": kind_,  # ground truth, for checking only
                }
            )
            t += pd.Timedelta(seconds=rng.randint(15, 120))
        day += pd.Timedelta(
            days=rng.choice([0, 1, 1, 2, 2, 3, 4, 6]), hours=rng.randint(2, 6)
        )
    return rows


def taskboard_events(n_users: int = 1000, seed: int = 3) -> pd.DataFrame:
    """The Taskboard event log as a DataFrame (one row per event)."""
    rng = random.Random(seed)
    rows = [row for uid in range(1, n_users + 1) for row in _user(uid, rng)]
    return (
        pd.DataFrame(rows).sort_values(["user_id", "timestamp"]).reset_index(drop=True)
    )


def load_taskboard(n_users: int = 1000, seed: int = 3, keep_truth: bool = False):
    """The Taskboard event log as an Eventstream (users and visits as paths,
    `channel` and `platform` as segments). `keep_truth` keeps the generator's
    `visit_kind` column, to check what clustering recovers."""
    import retentioneering as rete

    df = taskboard_events(n_users, seed)
    if not keep_truth:
        df = df.drop(columns="visit_kind")
    return rete.Eventstream(
        df,
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["channel", "platform"],
            "custom_cols": ["visit_kind"] if keep_truth else [],
        },
    )
