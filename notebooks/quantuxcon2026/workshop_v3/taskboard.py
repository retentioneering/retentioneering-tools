"""Taskboard: a synthetic event log of a team task tracker, for the workshop.

The legend (pricing, kinds of visits, kinds of users, the planted stories) is
in LEGEND.md; this module generates the click stream from it. In short:

- A workspace is free for up to FREE_SEATS members; a bigger team needs the
  Team plan, which the owner buys. Power features are free for everyone.
- Users are owners (signed up themselves, own a workspace) or members (came
  through an owner's invite). Members never see the paywall.
- Every visit has a kind (check-in, focus, planning, sync, review, admin,
  onboarding): a mix of events from all over the app, in random amounts, with
  each user's own taste on top. Users differ by the mix of visits they make.

Segments: `channel` (organic / ads / invite), `platform` (web / mobile),
`ab_group` (control / test, per workspace). `workspace_id` is a plain column.
Paths: `user_id`, and `session_id` for visits. Deterministic for a seed.
"""

from __future__ import annotations

import math
import random

import pandas as pd

START = pd.Timestamp("2026-03-02")  # a Monday
DAYS = 56  # eight weeks of data
SIGNUP_DAYS = 42  # owners sign up during the first six weeks
CHECKOUT_BUG = ("2026-04-06", "2026-04-13")  # payment form broken (week 6)
FREE_SEATS = 2  # members a free workspace may have, the owner included

# Where in the app each event happens: the columns of the events slide.
EVENT_GROUP = {
    **dict.fromkeys(
        ["signup", "tour_step", "template_view", "create_project"], "getting_started"
    ),
    **dict.fromkeys(
        [
            "open_board",
            "create_task",
            "edit_task",
            "complete_task",
            "assign_task",
            "set_due_date",
            "add_dependency",
            "view_timeline",
        ],
        "tasks",
    ),
    **dict.fromkeys(["comment", "mention", "view_notifications"], "discussion"),
    **dict.fromkeys(
        [
            "invite_member",
            "view_invite_status",
            "resend_invite",
            "accept_invite",
            "teammate_joined",
        ],
        "team",
    ),
    **dict.fromkeys(
        ["view_report", "export", "create_automation", "connect_integration"], "power"
    ),
    **dict.fromkeys(
        [
            "view_pricing",
            "select_plan",
            "start_checkout",
            "enter_payment",
            "payment_error",
            "support_ticket",
            "upgrade",
        ],
        "billing",
    ),
}

# Clustering features for visits: everything but signup, templates and the
# team events, which are rare and would each take a cluster of their own.
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
    "create_project",
]
CHECKOUT = [
    "view_pricing",
    "select_plan",
    "start_checkout",
    "enter_payment",
    "upgrade",
    "payment_error",
    "support_ticket",
]

# ── Visits ────────────────────────────────────────────────────────────────────
# Expected counts of each event in a visit, by the kind of visit. Comments,
# mentions and assignments need someone to work with (TEAM_EVENTS).
VISIT_MIX = {
    "check_in": {
        "view_notifications": 1.5,
        "complete_task": 0.8,
        "comment": 0.35,
        "edit_task": 0.25,
        "create_task": 0.2,
    },
    "focus": {
        "create_task": 1.6,
        "edit_task": 1.4,
        "complete_task": 2.3,
        "set_due_date": 0.35,
        "comment": 0.3,
        "view_notifications": 0.2,
    },
    "planning": {
        "create_task": 2.0,
        "set_due_date": 1.8,
        "add_dependency": 0.8,
        "assign_task": 1.1,
        "edit_task": 0.7,
        "complete_task": 0.3,
        "view_timeline": 0.2,
    },
    "sync": {
        "comment": 2.3,
        "mention": 0.9,
        "view_notifications": 0.7,
        "assign_task": 0.6,
        "complete_task": 0.6,
        "edit_task": 0.4,
    },
    "review": {
        "view_report": 2.4,
        "export": 1.0,
        "view_timeline": 0.6,
        "complete_task": 0.5,
        "comment": 0.4,
        "edit_task": 0.3,
    },
    "admin": {
        "create_automation": 0.9,
        "connect_integration": 0.7,
        "create_task": 0.5,
        "create_project": 0.4,
        "view_report": 0.3,
        "edit_task": 0.3,
    },
}
# A little of everything in any visit, on top of its kind's mix.
BACKGROUND = {
    "create_task": 0.1,
    "edit_task": 0.12,
    "complete_task": 0.15,
    "comment": 0.08,
    "view_notifications": 0.15,
    "set_due_date": 0.05,
    "view_report": 0.04,
    "export": 0.015,
    "view_timeline": 0.03,
    "add_dependency": 0.02,
    "assign_task": 0.04,
    "mention": 0.03,
    "create_automation": 0.005,
    "connect_integration": 0.005,
}
TEAM_EVENTS = {"comment", "mention", "assign_task"}
SOLO_COMMENT = 0.1  # notes to self: a user with no team still comments a bit
TASTE = 0.4  # spread of each user's own taste for each event (lognormal sigma)

# ── Users ─────────────────────────────────────────────────────────────────────
# Owners by kind: (share, channel weights, visits range). Members come from the
# invites owners send; their kinds are drawn when they join (MEMBER_KINDS).
OWNER_KINDS = {
    "staller": (0.30, {"organic": 0.3, "ads": 0.7}, (1, 3)),
    "solo": (0.28, {"organic": 0.5, "ads": 0.5}, (4, 10)),
    "duo": (0.15, {"organic": 0.7, "ads": 0.3}, (8, 18)),
    "lead": (0.15, {"organic": 0.75, "ads": 0.25}, (12, 26)),
    "planner": (0.12, {"organic": 0.6, "ads": 0.4}, (8, 16)),
}
MEMBER_KINDS = {"active": (0.6, (6, 16)), "passive": (0.4, (3, 8))}

# Which kinds of visits a user makes, by kind and state (weights).
VISIT_WEIGHTS = {
    "alone": {"focus": 4, "check_in": 3, "planning": 1, "review": 0.4},
    # A pair shares the work: half the visits are spent working it out together.
    "duo": {"sync": 6, "focus": 2, "check_in": 1.5, "planning": 1.5, "review": 0.5},
    "lead": {
        "sync": 3,
        "planning": 3,
        "focus": 2,
        "check_in": 2,
        "review": 2.0,
        "admin": 0.5,
    },
    "planner": {"planning": 6, "focus": 3, "check_in": 1, "review": 0.5},
    "active": {"focus": 3, "sync": 3, "check_in": 2, "planning": 0.3, "review": 0.5},
    "passive": {"check_in": 4, "sync": 1, "focus": 1},
}
POWER_SHARE = {"lead": 0.4, "active": 0.12}  # the core audience among them
POWER_BOOST = {"review": 3.0, "admin": 4.0}

# The A/B test of Timeline (per workspace): in test, a new project opens on
# Timeline with a "Plan with your team" prompt. Planners find the view far
# more often; a team's first member drops off a little more often.
FINDS_TIMELINE = {"control": 0.25, "test": 0.9}
FRAGILE_TEAM = {"control": 0.30, "test": 0.32}
# A UI problem on the timeline: the task bars look clickable, but aren't. Some
# planners keep reopening the timeline and give up after a few planning visits.
STUCK_ON_BARS = 0.4
UX_SEED = 7


def _poisson(lam: float, rng: random.Random) -> int:
    """A Poisson draw (Knuth's method; the rates here are small)."""
    limit, k, p = math.exp(-lam), 0, rng.random()
    while p > limit:
        k += 1
        p *= rng.random()
    return k


def _shuffled(events: list[str], rng: random.Random) -> list[str]:
    events = list(events)
    rng.shuffle(events)
    return events


def _pick(weights: dict[str, float], rng: random.Random) -> str:
    return rng.choices(list(weights), list(weights.values()))[0]


def _boosted(weights: dict[str, float], user: dict) -> dict[str, float]:
    if not user["power"]:
        return weights
    return {k: w * POWER_BOOST.get(k, 1) for k, w in weights.items()}


ALL_EVENTS = sorted(set(BACKGROUND) | {e for mix in VISIT_MIX.values() for e in mix})


def _taste(rng: random.Random) -> dict[str, float]:
    """A user's own multiplier for each event: some write more comments,
    some hardly ever set due dates."""
    return {e: math.exp(rng.gauss(0, TASTE)) for e in ALL_EVENTS}


def _visit(kind: str, user: dict, rng: random.Random) -> list[str]:
    """The events of one visit of a given kind: how it opens, then a mix of
    actions in random amounts, in random order."""
    rates = dict(VISIT_MIX[kind])
    for event, lam in BACKGROUND.items():
        rates[event] = rates.get(event, 0) + lam
    if user["timeline"] and kind == "planning":
        rates["view_timeline"] += 0.8
    if not user["team"]:
        for event in TEAM_EVENTS:
            rates.pop(event, None)
        rates["comment"] = SOLO_COMMENT
    taste = user["taste"]
    body = [e for e, lam in rates.items() for _ in range(_poisson(lam * taste[e], rng))]
    if kind == "planning" and user["timeline"]:
        opening = (
            ["view_timeline"]
            if rng.random() < 0.75
            else ["open_board", "view_timeline"]
        )
    elif rng.random() < (0.7 if kind == "check_in" else 0.12):
        opening = ["view_notifications", "open_board"]  # came in from a notification
    else:
        opening = ["open_board"]
    return opening + _shuffled(body, rng)


def _onboarding(rng: random.Random, abandoned: bool, test: bool) -> list[str]:
    events = ["signup"] + ["tour_step"] * (1 + _poisson(1.3, rng))
    for _ in range(_poisson(0.8, rng)):  # templates, looked at during the tour
        events.insert(rng.randint(1, len(events)), "template_view")
    if abandoned:  # leaves before creating a project
        return events
    events.append("create_project")
    if test:  # test: the new project opens on the timeline
        events.append("view_timeline")
    body = ["open_board"] * (rng.random() < 0.85)
    body += ["create_task"] * _poisson(1.0, rng) + ["edit_task"] * _poisson(0.2, rng)
    return events + body


def _checkout(at: pd.Timestamp, rng: random.Random) -> tuple[list[str], bool]:
    """The paywall opens the pricing page; returns (events, upgraded). Some
    owners compare plans or walk away; during CHECKOUT_BUG the payment form
    fails three times out of four."""
    events = ["view_pricing"] * (1 + _poisson(0.3, rng))
    events += ["select_plan"] * (1 + (rng.random() < 0.2))
    if rng.random() < 0.15:  # not now
        return events, False
    events += ["start_checkout"] + ["enter_payment"] * (1 + (rng.random() < 0.15))
    bug = pd.Timestamp(CHECKOUT_BUG[0]) <= at < pd.Timestamp(CHECKOUT_BUG[1])
    if bug and rng.random() < 0.75:
        events += ["payment_error"]
        if rng.random() < 0.7:
            events += ["support_ticket"]
        return events, False
    return events + ["upgrade"], True


def _next_start(t: int, rng: random.Random) -> int:
    """When the next visit starts, the previous one having ended at `t`:
    later the same day, or on a later day, in the daytime."""
    gap_days = rng.choice([0, 1, 1, 2, 2, 3, 4, 6])
    if gap_days == 0 and t + 2 * 3600 < (t // 86400) * 86400 + 22 * 3600:
        return t + rng.randint(1, 3) * 3600 + rng.randint(0, 59) * 60
    day = t // 86400 + max(gap_days, 1)
    return day * 86400 + rng.randint(8, 20) * 3600 + rng.randint(0, 59) * 60


class _Generator:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)
        self.ux = random.Random(seed + UX_SEED)  # the timeline UI problem's own stream
        self.rows: list[tuple] = []
        self.n_users = 0
        self.end = DAYS * 86400  # seconds since START: the end of the data

    def _new_user(self, kind, ws, channel, group) -> dict:
        self.n_users += 1
        rng = self.rng
        return {
            "id": f"user_{self.n_users:05d}",
            "kind": kind,
            "ws": ws,
            "channel": channel,
            "platform": "mobile" if rng.random() < 0.3 else "web",
            "group": group,
            "taste": _taste(rng),
            "power": rng.random() < POWER_SHARE.get(kind, 0),
            "team": False,
            "timeline": False,
            "visits": 0,
        }

    def _write(self, user, events, start, visit_kind, ux_marks=()) -> int | None:
        """Writes one visit; returns when it ends, or None past the data end.
        Timeline opened again right after itself is written only when it is
        the UI problem (`ux_marks`): an ordinary visit doesn't reopen a view
        it is on. (The skipped event still takes its time, so the rest of the
        data stays as it is.)"""
        rng, t, out, last = self.rng, start, [], start
        sid = f"{user['id']}_{user['visits'] + 1:02d}"
        for i, event in enumerate(events):
            last = t
            reopened = i and event == "view_timeline" == events[i - 1]
            if reopened and i not in ux_marks:
                t += rng.randint(15, 120)
                continue
            out.append(
                (
                    user["id"],
                    sid,
                    event,
                    t,
                    user["ws"],
                    user["channel"],
                    user["platform"],
                    user["group"],
                    visit_kind,
                    user["kind"],
                )
            )
            t += (self.ux if i in ux_marks else rng).randint(15, 120)
        if last >= self.end:
            return None
        user["visits"] += 1
        self.rows += out
        return t

    def _invite(
        self, owner, start, accepts, first=False
    ) -> tuple[int | None, dict | None]:
        """An invite sent at `start`: (accept time or None, the new member)."""
        if not accepts:
            return None, None
        accept_at = start + self.rng.randint(2, 96) * 3600
        return accept_at, self.member(owner, accept_at, first)

    # ── Owners ────────────────────────────────────────────────────────────────
    def owner(self, ws: int) -> None:
        rng = self.rng
        kinds = list(OWNER_KINDS)
        kind = rng.choices(kinds, [OWNER_KINDS[k][0] for k in kinds])[0]
        _, channels, (lo, hi) = OWNER_KINDS[kind]
        group = "test" if rng.random() < 0.5 else "control"
        user = self._new_user(kind, f"ws_{ws:05d}", _pick(channels, rng), group)
        n_visits = rng.randint(lo, hi)
        found = kind == "planner" and rng.random() < FINDS_TIMELINE[group]
        if kind == "planner" and not found:
            n_visits = rng.randint(3, 5)  # never finds the view: a few tasks, gone
        user["timeline"] = found
        stuck = found and self.ux.random() < STUCK_ON_BARS
        gives_up_after = self.ux.randint(2, 4)

        first_invite = rng.randint(1, 3)
        invites = {
            "solo": rng.random() < 0.7,  # nobody comes
            "duo": True,
            "lead": True,
            "planner": rng.random() < 0.12,  # now and then a client
        }.get(kind, False)
        accepted = kind in ("duo", "lead") or (kind == "planner" and rng.random() < 0.5)

        seats = 1  # the owner
        pending: list[int | None] = []  # accept times; None: never accepted
        bounced = False  # the first member dropped off: back to working alone
        team_visits = 0
        upgraded = False
        wants_more = kind == "lead"
        extra_invites = 0
        planned = 0

        start = rng.randrange(SIGNUP_DAYS) * 86400 + rng.randint(8, 20) * 3600
        start += rng.randint(0, 59) * 60
        abandoned = kind == "staller" and rng.random() < 0.25
        events = _onboarding(rng, abandoned, group == "test")
        t = self._write(user, events, start, "onboarding")
        if t is None or abandoned:
            return

        for i in range(1, n_visits):
            start = _next_start(t, rng)
            if start >= self.end:
                break
            at = START + pd.Timedelta(seconds=start)
            # Teammates who accepted since the last visit: a notification.
            arrived = [a for a in pending if a is not None and a <= start]
            pending = [a for a in pending if a is None or a > start]
            seats += len(arrived)
            user["team"] = user["team"] or bool(arrived)

            if kind == "planner":
                weights = VISIT_WEIGHTS["planner" if found else "alone"]
            elif not user["team"] or (bounced and team_visits >= 1):
                weights = VISIT_WEIGHTS["alone"]
            elif team_visits == 0:
                weights = {"sync": 1}  # the first visit together
            else:
                weights = VISIT_WEIGHTS["lead" if kind == "lead" else "duo"]
            visit_kind = _pick(_boosted(weights, user), rng)
            team_visits += user["team"]
            events = _visit(visit_kind, user, rng)
            suffix: list[str] = []

            if i == first_invite and invites:
                suffix.append("invite_member")
                accept_at, member = self._invite(user, start, accepted, first=True)
                pending.append(accept_at)
                bounced = member is not None and member["kind"] == "dropped"
            elif (
                wants_more and not bounced and seats >= FREE_SEATS and team_visits >= 3
            ):
                # A lead invites a third person: the paywall.
                checkout, upgraded = _checkout(at, rng)
                suffix += checkout
                if upgraded:
                    wants_more = False
                    extra_invites = rng.randint(1, 4)
                elif rng.random() < 0.4:
                    wants_more = False  # gives up: the team stays at two
            elif upgraded and extra_invites and rng.random() < 0.5:
                extra_invites -= 1
                suffix.append("invite_member")
                accept_at, _ = self._invite(user, start, rng.random() < 0.8)
                pending.append(accept_at)
            elif kind != "staller" and rng.random() < 0.02:
                suffix.append("view_pricing")  # just curious

            if pending and not suffix and rng.random() < 0.35:
                # An invite hangs: now and then the owner checks on it.
                suffix.append("view_invite_status")
                suffix += ["resend_invite"] * (rng.random() < 0.4)

            events = (
                events[:1] + ["teammate_joined"] * len(arrived) + events[1:] + suffix
            )
            ux_marks: set[int] = set()
            if stuck and visit_kind == "planning":
                # Clicks on a task bar do nothing; the user opens the timeline again.
                at_ = events.index("view_timeline") + 1
                for k in range(self.ux.randint(1, 3)):
                    events.insert(at_, "view_timeline")
                    ux_marks.add(at_ + k)
                planned += 1
                if planned > gives_up_after:
                    break  # gave up
            t = self._write(user, events, start, visit_kind, ux_marks)
            if t is None:
                break

    # ── Members ───────────────────────────────────────────────────────────────
    def member(self, owner: dict, accept_at: int, first: bool = False) -> dict | None:
        """A member joining `owner`'s workspace at `accept_at`; the first one
        of a team drops off at once with FRAGILE_TEAM's odds."""
        rng = self.rng
        if accept_at >= self.end:
            return None
        if first and rng.random() < FRAGILE_TEAM[owner["group"]]:
            kind, (lo, hi) = "dropped", (1, 2)
        else:
            names = list(MEMBER_KINDS)
            kind = rng.choices(names, [MEMBER_KINDS[k][0] for k in names])[0]
            lo, hi = MEMBER_KINDS[kind][1]
        user = self._new_user(kind, owner["ws"], "invite", owner["group"])
        user["team"] = True
        start = accept_at
        for i in range(rng.randint(lo, hi)):
            if i == 0:
                visit_kind = "sync"  # joins to work on something together
            elif kind == "dropped":
                visit_kind = "check_in"
            else:
                visit_kind = _pick(_boosted(VISIT_WEIGHTS[kind], user), rng)
            events = _visit(visit_kind, user, rng)
            if i == 0:
                events = ["accept_invite"] + events
            t = self._write(user, events, start, visit_kind)
            if t is None:
                break
            start = _next_start(t, rng)
        return user


COLUMNS = [
    "user_id",
    "session_id",
    "event",
    "timestamp",
    "workspace_id",
    "channel",
    "platform",
    "ab_group",
    "visit_kind",  # ground truth, for checking only
    "archetype",  # ground truth, for checking only
]
TRUTH = ["visit_kind", "archetype"]


def taskboard_events(n_workspaces: int = 14000, seed: int = 1) -> pd.DataFrame:
    """The Taskboard event log as a DataFrame (one row per event)."""
    gen = _Generator(seed)
    for ws in range(1, n_workspaces + 1):
        gen.owner(ws)
    df = pd.DataFrame(gen.rows, columns=COLUMNS)
    df["timestamp"] = START + pd.to_timedelta(df["timestamp"], unit="s")
    return df.sort_values(["user_id", "timestamp"], kind="stable").reset_index(
        drop=True
    )


def load_taskboard(n_workspaces: int = 14000, seed: int = 1, keep_truth: bool = False):
    """The Taskboard event log as an Eventstream (users and visits as paths,
    `channel`, `platform` and `ab_group` as segments). `keep_truth` keeps the
    generator's `visit_kind` and `archetype` columns."""
    import retentioneering as rete

    df = taskboard_events(n_workspaces, seed)
    if not keep_truth:
        df = df.drop(columns=TRUTH)
    return rete.Eventstream(
        df,
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["channel", "platform", "ab_group"],
            "custom_cols": ["workspace_id"] + (TRUTH if keep_truth else []),
        },
    )


def check_invariants(df: pd.DataFrame) -> dict[str, int]:
    """Counts of what must never happen in `taskboard_events()` output; all
    zeros means the time, causal and pricing rules hold."""
    visits = (
        df.groupby("session_id")
        .agg(
            user=("user_id", "first"),
            start=("timestamp", "min"),
            end=("timestamp", "max"),
        )
        .sort_values(["user", "start"])
    )
    previous_end = visits.groupby("user")["end"].shift()
    first = df.groupby(["user_id", "event"])["timestamp"].min().unstack()

    def before(a: str, b: str) -> int:
        both = first[a].notna() & first[b].notna()
        return int((first.loc[both, a] < first.loc[both, b]).sum())

    member = df["channel"] == "invite"
    members = set(df.loc[member, "user_id"])
    starts = df.groupby("user_id")["event"].first()
    is_member = starts.index.isin(members)
    bug = df["timestamp"].between(*map(pd.Timestamp, CHECKOUT_BUG), inclusive="left")
    by_ws = df.groupby(["workspace_id", "event"])["timestamp"].min().unstack()
    joined = by_ws["teammate_joined"].notna()
    seats = (
        df[df["event"].isin(["signup", "accept_invite"])].groupby("workspace_id").size()
    )
    paid = set(df.loc[df["event"] == "upgrade", "workspace_id"])
    return {
        "visit starts before the previous one ends": int(
            (visits["start"] <= previous_end).sum()
        ),
        "events after the end of the data": int(
            (df["timestamp"] >= START + pd.Timedelta(days=DAYS)).sum()
        ),
        "payment errors outside the bug week": int(
            ((df["event"] == "payment_error") & ~bug).sum()
        ),
        "owners not starting with signup": int((starts[~is_member] != "signup").sum()),
        "members not starting with accept_invite": int(
            (starts[is_member] != "accept_invite").sum()
        ),
        "billing events by members": int((member & df["event"].isin(CHECKOUT)).sum()),
        "teammate_joined with no member accepted before": int(
            (
                by_ws.loc[joined, "accept_invite"].isna()
                | (
                    by_ws.loc[joined, "teammate_joined"]
                    < by_ws.loc[joined, "accept_invite"]
                )
            ).sum()
        ),
        "teammate_joined before invite_member": before(
            "teammate_joined", "invite_member"
        ),
        "free workspaces over the seat limit": int(
            (seats[~seats.index.isin(paid)] > FREE_SEATS).sum()
        ),
        "duplicate (user, timestamp) pairs": int(
            df.duplicated(["user_id", "timestamp"]).sum()
        ),
    }


if __name__ == "__main__":
    # The static snapshot the slides read (without the ground truth columns).
    taskboard_events().drop(columns=TRUTH).to_csv("taskboard.csv.gz", index=False)
