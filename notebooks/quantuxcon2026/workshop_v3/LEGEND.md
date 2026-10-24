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

31 events, grouped by the part of the app, not by who uses them or why; any
kind of visit mixes them.

| Getting started | Tasks | Discussion | Team | Power features | Billing |
|---|---|---|---|---|---|
| `signup` | `open_board` | `comment` | `invite_member` | `view_report` | `view_pricing` |
| `tour_step` | `create_task` | `mention` | `view_invite_status` | `export` | `select_plan` |
| `template_view` | `edit_task` | `view_notifications` | `resend_invite` | `create_automation` | `start_checkout` |
| `create_project` | `complete_task` | | `accept_invite` | `connect_integration` | `enter_payment` |
| | `assign_task` | | `teammate_joined` | | `payment_error` |
| | `set_due_date` | | | | `support_ticket` |
| | `add_dependency` | | | | `upgrade` |
| | `view_timeline` | | | | |

Due dates and dependencies are fields of a task card; Timeline is one more way
to look at the tasks.

Owner's happy path: `signup` → `create_project` → `create_task` →
`invite_member` → `teammate_joined` → `invite_member` (a third person: the
paywall) → `view_pricing` → `upgrade`.

## Kinds of visits

A visit is a mix of events from all over the app, in random amounts. Each
user also has their own taste for each event (some write more comments, some
hardly ever set due dates), so visits of one kind differ from user to user.
Clustering visits on their event counts should recover these kinds.

| Visit | What happens | Typical events |
|---|---|---|
| onboarding | the first visit: the tour, a project, first tasks | `tour_step`, `template_view`, `create_project`, `create_task` |
| check-in | a quick look, mostly from a notification | `view_notifications`, `complete_task`, a comment |
| focus | heads-down work on one's own tasks | `create_task`, `edit_task`, `complete_task` |
| planning | lays out the work: tasks, dates, dependencies, who does what | `create_task`, `set_due_date`, `add_dependency`, `assign_task`, `view_timeline` |
| sync | works it out with the team | `comment`, `mention`, `assign_task`, `view_notifications` |
| review | how the project is going: reports and exports | `view_report`, `export`, `view_timeline` |
| admin | sets the workspace up | `create_automation`, `connect_integration`, `create_project` |

Checkout is not a kind of visit: it happens at the end of whatever visit the
owner hit the paywall in. Comments, mentions and assignments need someone to
work with: a user without a team only leaves an occasional note to self.

## Kinds of users

Users differ by the mix of visits they make, and by how it changes over time.
Clustering users on the kinds of their visits should recover these kinds.

**Owners** sign up themselves (`channel` organic or ads) and own a workspace.

| Owner | Share | Story |
|---|---|---|
| staller | 30% | onboarding, sometimes not even a project, then a check-in or two at most |
| solo | 28% | works alone: focus and check-ins; most invite someone who never comes |
| duo | 15% | invites one person, who joins; the two share the work (half the visits are sync) and stay on the free plan |
| team lead | 15% | brings a teammate in, plans, syncs, reviews; invites a third person, hits the paywall and mostly pays; then invites more |
| planner | 12% | a freelancer planning client work alone: planning visit after visit on Timeline |

**Members** come in through an owner's invite (`channel` invite,
`accept_invite`), into the owner's workspace and A/B group.

| Member | Story |
|---|---|
| active | focus, sync and check-ins, like a regular user |
| passive | mostly check-ins: reads notifications, closes a task now and then |
| dropped | accepts the invite, looks around once or twice and is gone |

A share of team leads (40%) and active members (12%) are the **core
audience**: many more review and admin visits.

## Stories planted in the data

1. **Visits, then users.** Visits clustered on events give the kinds of visits
   above, not the event groups; users clustered on their visits give the kinds
   of users.
2. **A-ha moment: a teammate joins** (`teammate_joined`). Owners whose teammate
   comes and stays go on to sync, plan and review; the rest stay in focus and
   check-ins, or leave.
3. **The invite funnel.** Many solo owners invite someone who never comes;
   they keep checking the invite (`view_invite_status`, `resend_invite`).
4. **The paywall.** Upgrades come from team leads inviting a third person;
   duos never need to pay. Small teams that stop at two are a segment of
   their own.
5. **Bounce back.** In some new teams the first member drops off right away;
   the owner works with them once and goes back to working alone.
6. **Anomaly.** For a week (`CHECKOUT_BUG`) the payment form fails three times
   out of four: `start_checkout` → `payment_error` → `support_ticket`, and
   upgrades drop.
7. **The A/B test of Timeline.** Randomized by workspace: in test, a new
   project opens on Timeline with a "Plan with your team" prompt. Team
   activation and upgrades don't move, every funnel step is grey. Behind the
   grey numbers:
   - planners find Timeline far more often in test (90% vs 25%) and stay much
     longer, but they work alone and never hit the paywall;
   - a few more new teams fall apart right away in test (32% vs 30%).
8. **A UI problem on Timeline.** Task bars look clickable but aren't: some
   planners keep reopening the timeline (`view_timeline` → `view_timeline`)
   and give up after a few planning visits.

## Data

- `user_id`, `session_id` (one visit), `event`, `timestamp`;
- `workspace_id`: a plain column, not a path: the events of different members
  interleaved by time would make transitions nobody made;
- segments `channel` (organic / ads / invite), `platform` (web / mobile),
  `ab_group` (control / test, per workspace);
- eight weeks from 2026-03-02; owners sign up during the first six;
- about 14,000 workspaces, 20,000 users, a million events.
