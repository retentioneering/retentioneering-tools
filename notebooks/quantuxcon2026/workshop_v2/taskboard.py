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
FRAGILE_TEAM = {"control": 0.30, "test": 0.34}


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
    if kind in ("power", "power_setup"):
        # Reports and exports; setting up an automation or an integration only
        # on a user's first power visits (the `power_setup` kind), now and
        # then later.
        body = ["view_report"] + rng.choice(
            [["export"], ["view_report"], ["export", "view_report"]]
        )
        if kind == "power_setup" or rng.random() < 0.15:
            body.append(rng.choice(["create_automation", "connect_integration"]))
        return ["open_board"] + _shuffled(body, rng)
    if kind == "planning":  # a planner at work on the timeline
        body = ["set_due_date"] * rng.randint(2, 3) + ["add_dependency"] * rng.randint(
            1, 2
        )
        body += ["create_task"] * rng.randint(0, 1)
        return ["view_timeline"] + _shuffled(body, rng)
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
        events = ["view_notifications", "view_invite_status"]
        if rng.random() < 0.6:
            events.insert(rng.randint(1, 2), "resend_invite")
        return events
    raise ValueError(kind)


# Besides its own events, a visit of each kind now and then has events of
# other kinds: (event, probability) pairs. They come from a separate random
# stream, so the plans, groups and funnel events stay as they are; none of
# them is a funnel event.
MIX_SEED = 38  # offset of the crossover stream's seed
CROSSOVER = {
    "solo": [("comment", 0.1), ("view_timeline", 0.08), ("view_report", 0.05)],
    "collaboration": [
        ("create_task", 0.4),
        ("edit_task", 0.2),
        ("view_report", 0.08),
        ("view_timeline", 0.08),
    ],
    "power": [
        ("comment", 0.25),
        ("create_task", 0.25),
        ("assign_task", 0.1),
        ("view_timeline", 0.1),
    ],
    "planning": [
        ("open_board", 0.4),
        ("complete_task", 0.3),
        ("edit_task", 0.2),
        ("comment", 0.1),
    ],
    "onboarding": [("edit_task", 0.1)],
    "checkin": [("comment", 0.15), ("complete_task", 0.15)],
    "waiting": [("open_board", 0.2), ("create_task", 0.1)],
    "billing": [("open_board", 0.2), ("view_report", 0.1)],
}


def _crossover(
    kind: str, events: list[str], rng: random.Random
) -> list[tuple[str, bool]]:
    """`events` with a few events of other kinds mixed in (see CROSSOVER), as
    (event, mixed_in) pairs."""
    events = [(e, False) for e in events]
    for event, p in CROSSOVER.get(kind, []):
        if rng.random() < p:
            events.insert(rng.randint(1, len(events)), (event, True))
    return events


def _checkout(at: pd.Timestamp, rng: random.Random) -> tuple[list[str], bool]:
    """A billing visit starting at `at`; returns (events, upgraded). Some users
    compare plans or walk away before paying; during CHECKOUT_BUG the payment
    form fails three times out of four. (A visit lasts minutes and starts in
    the daytime, so it never crosses midnight into or out of the bug week.)"""
    events = ["view_pricing"] * (2 if rng.random() < 0.25 else 1) + ["select_plan"]
    if rng.random() < 0.12:  # changes their mind before checkout
        return events, False
    events += ["start_checkout", "enter_payment"]
    bug = pd.Timestamp(CHECKOUT_BUG[0]) <= at < pd.Timestamp(CHECKOUT_BUG[1])
    if bug and rng.random() < 0.75:
        events += ["payment_error"]
        if rng.random() < 0.7:
            events += ["support_ticket"]
        return events, False
    return events + ["upgrade"], True


def _user(
    uid: int, rng: random.Random, mix: random.Random, ux: random.Random
) -> list[dict]:
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
    # The first visit is on the signup day; each next one starts after the
    # previous one ends: later the same day, or on a later day, in the daytime.
    start = signup_day * 86400 + rng.randint(8, 20) * 3600 + rng.randint(0, 59) * 60
    i = 0
    while i < len(plan):
        visit = plan[i]
        if start >= end:
            break
        at = START + pd.Timedelta(seconds=start)
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
                i += 1
                continue
            events, upgraded = _checkout(at, rng)
            if not upgraded and rng.random() < 0.6:  # tries again a bit later
                plan.insert(min(i + 1 + rng.randint(0, 2), len(plan)), "billing")
            kind_ = "billing"
        elif visit == "power":
            events, kind_ = (
                _session("power" if powered else "power_setup", rng),
                "power",
            )
            powered = True
        else:
            events, kind_ = _session(visit, rng, first=(i == 0)), visit
            if visit == "onboarding" and group == "test":
                # Test: the new project opens on the timeline; most switch
                # back to the board right away.
                at_ = events.index("create_project") + 1
                events = events[:at_] + ["view_timeline"] + events[at_:]
        events = _crossover(kind_, events, mix)
        if stuck and kind_ == "planning":
            # Clicks on a task bar do nothing; the user opens the timeline again.
            for _ in range(ux.randint(1, 3)):
                events.insert(1, ("view_timeline", "ux"))
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
                    kind_,
                    kind,
                )
            )
            t += {False: rng, True: mix, "ux": ux}[source].randint(15, 120)
        if visit_rows[-1][3] >= end:  # the data ends before this visit does
            break
        planned += kind_ == "planning"
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


def taskboard_events(n_users: int = 20000, seed: int = 3) -> pd.DataFrame:
    """The Taskboard event log as a DataFrame (one row per event)."""
    rng = random.Random(seed)
    mix = random.Random(seed + MIX_SEED)  # the crossover events' own stream
    ux = random.Random(seed + UX_SEED)  # the timeline UI problem's own stream
    rows = [row for uid in range(1, n_users + 1) for row in _user(uid, rng, mix, ux)]
    df = pd.DataFrame(rows, columns=COLUMNS)
    df["timestamp"] = START + pd.to_timedelta(df["timestamp"], unit="s")
    return df.sort_values(["user_id", "timestamp"], kind="stable").reset_index(
        drop=True
    )


def load_taskboard(n_users: int = 20000, seed: int = 3, keep_truth: bool = False):
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
