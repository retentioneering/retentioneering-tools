# Taskboard: the legend behind the workshop dataset

Taskboard is a made-up task tracker for teams. `taskboard.py` generates its
click stream from this legend; the slides read a static snapshot of it,
`taskboard.csv.gz`. The snapshot isn't tracked: `python taskboard.py` makes
it, the same for the same seed. Change the legend first, then the generator.

## The product and how it makes money

- Anyone can sign up, create a **workspace** and work in it for free.
- A free workspace has room for **two members**, the owner included. A third
  member needs the **Team plan**, which the **owner** buys. Inviting a third
  person opens the paywall: the pricing page (`view_pricing`).
- **Power features** (reports, export, automations, integrations) are free for
  everyone. A small core audience uses them a lot.
- So the business needs owners to bring their team in, grow it past two, and
  upgrade. Members of a team never see the paywall: for them the goal is to
  get going and keep coming back, not `upgrade`.

## Events: where in the app they happen

29 events, grouped by the part of the app, not by who uses them or why; any
kind of session mixes them.

| Getting started | Tasks | Discussion | Team | Power features | Billing |
|---|---|---|---|---|---|
| `signup` | `open_board` | `comment` | `invite_member` | `view_report` | `view_pricing` |
| `tour_step` | `create_task` | `mention` | `accept_invite` | `export` | `select_plan` |
| `template_view` | `edit_task` | `view_notifications` | `teammate_joined` | `create_automation` | `start_checkout` |
| `create_project` | `complete_task` | | | `connect_integration` | `enter_payment` |
| | `assign_task` | | | | `payment_error` |
| | `set_due_date` | | | | `support_ticket` |
| | `add_dependency` | | | | `upgrade` |
| | `view_timeline` | | | | |

Due dates and dependencies are fields of a task card; Timeline is one more way
to look at the tasks.

Owner's happy path: `signup` → `create_project` → `invite_member` →
`teammate_joined` → `invite_member` (a third person: the paywall) →
`view_pricing` → `upgrade`.

## Kinds of sessions

A session is a mix of events from all over the app, in random amounts. Each
user also has their own taste for each event (some write more comments, some
hardly ever set due dates), so sessions of one kind differ from user to user.

| Session | What happens | Typical events |
|---|---|---|
| onboarding | the first session: the tour, templates, a project, first tasks | `signup`, `tour_step`, `template_view`, `create_project` |
| short | a quick look, mostly from a notification | `view_notifications`, a task completed |
| focus | heads-down work on one's own tasks | `create_task`, `edit_task`, `complete_task` |
| planning | lays out the work: tasks, dates, dependencies, who does what | `create_task`, `set_due_date`, `add_dependency`, `assign_task`, `view_timeline` |
| teamwork | works it out with the team | `comment`, `mention`, `assign_task`, `view_notifications` |
| review | the power features | `view_report`, `export`, `create_automation`, `connect_integration` |

The team's rare events are not a kind of session: they happen in sessions of
any kind. An invite (`invite_member`) is sent at any point of a session; a
teammate who has joined since shows up as a notification (`teammate_joined`)
at the start of the owner's next session; an invited member's first session,
of whatever kind, opens with `accept_invite`. Checkout comes at the end of
whatever session the owner hit the paywall in.

Why it's built this way: the slides cluster sessions on the counts of all 29
events (a bag of events), standardized. Standardized, a rare event that shows
up in only part of one kind of session is worth a cluster of its own, however
rare it is. Mixed into every kind, it isn't, and k-means (`k=8`, `n_init=30`)
finds the kinds above, plus checkout and checkout_failed, the same on every
seed tried.

## Kinds of users

Users differ by the mix of sessions they make, and by how it changes over
time. Clustering users on the kinds of their sessions recovers these.

**Owners** sign up themselves (`channel` organic or ads) and own a workspace.

| Owner | Share | Story |
|---|---|---|
| staller | 30% | onboarding, sometimes not even a project, then a short session or two at most |
| solo | 28% | works alone: focus and short sessions; most invite someone who never comes |
| duo | 15% | invites one person, who joins; the two share the work (half the sessions are teamwork) and stay on the free plan |
| team lead | 15% | brings a teammate in, plans, works with the team, reviews; invites a third person, hits the paywall and mostly pays; then invites more |
| planner | 12% | a freelancer planning client work alone; those who find Timeline plan session after session, the rest leave early |

**Members** come in through an owner's invite (`channel` invite,
`accept_invite`), into the owner's workspace.

| Member | Story |
|---|---|
| active | focus, teamwork and short sessions, like a regular user |
| passive | mostly short sessions: reads notifications, closes a task now and then |
| dropped | accepts the invite, looks around once or twice and is gone |

A share of team leads (40%) and active members (12%) are the **core
audience**: many more review sessions.

## Stories planted in the data

1. **Sessions, then users.** Sessions clustered on all their events give the
   kinds of sessions above, not the event groups; users clustered on their
   sessions give the kinds of users.
2. **A-ha moment: a teammate joins** (`teammate_joined`). Owners whose
   teammate comes and stays go on to work with the team, plan and review; the
   rest stay in focus and short sessions, or leave.
3. **The invite funnel.** Many solo owners invite someone who never comes.
4. **The paywall.** Upgrades come from team leads inviting a third person;
   duos never need to pay. Small teams that stop at two are a segment of
   their own.
5. **Bounce back.** In some new teams the first member drops off right away;
   the owner works with them once and goes back to working alone.
6. **Anomaly.** For a week (`CHECKOUT_BUG`) the payment form fails three times
   out of four: `start_checkout` → `payment_error` → `support_ticket`, and
   upgrades drop.

## Data

- `user_id`, `session_id` (one session), `event`, `timestamp`;
- `workspace_id`: a plain column, not a path: the events of different members
  interleaved by time would make transitions nobody made;
- segments `channel` (organic / ads / invite), `platform` (web / mobile);
- eight weeks from 2026-03-02; owners sign up during the first six;
- 5,000 workspaces, about 7,000 users, 48,000 sessions, 390,000 events.
