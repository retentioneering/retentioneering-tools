---
name: retentioneering-contributing
description: >
  Help the user turn their Retentioneering ideas, friction reports, bug
  findings, or feature needs into high-quality upstream contributions: from
  capturing and validating the idea, through minimal reproductions and issue
  drafts, to preparing, testing, and submitting a pull request that follows
  this repository's conventions. Use when the user says they found a bug,
  wants a feature, wrote a workaround worth upstreaming, or asks how to
  contribute, open an issue, or make a PR to retentioneering-tools.
license: Apache-2.0
compatibility: >
  Requires a git checkout of retentioneering-tools, Python >= 3.11 with uv,
  and Node.js only when JS/widget code is touched. The gh CLI is optional
  but recommended for PR submission.
metadata:
  author: retentioneering
  version: "1.0.0"
  package: retentioneering
  category: developer-tools
  keywords: >-
    open source contribution, pull request, bug report, issue template,
    minimal reproduction, code review, oss workflow, github, retentioneering
  homepage: https://retentioneering.com
  documentation: https://retentioneering.com/docs
  repository: https://github.com/retentioneering/retentioneering-tools
---

# Contributing to retentioneering-tools

## Objective

Convert a user's observation — a bug, a paper cut, a missing capability, a workaround
they keep re-writing — into the smallest upstream change that would have prevented it,
packaged so maintainers can accept it quickly.

## Bundled references

| File | Read it when |
|---|---|
| `references/repo-conventions.md` | before touching code — build/test/docs commands, architecture rules, naming, sync obligations |
| `references/proposal-templates.md` | when drafting — issue/feature/PR templates with worked examples |

## The full route: idea → merged PR

### Stage 1 — Capture the observation properly (do this even for "small" ideas)

Record four things while they are fresh:
1. **Expectation** — what the user believed would happen (quote the docstring/docs page
   that created the expectation, if any).
2. **Reality** — what actually happened (exact error text or wrong output).
3. **Cost** — time lost, wrong conclusion nearly shipped, workaround written.
4. **Environment** — `retentioneering.__version__`, Python, OS, install source
   (pip wheel vs source checkout).

Field lesson: reports formatted as expectation/reality/cost/repro get acted on;
"X is broken" reports stall.

### Stage 2 — Validate against the CURRENT version

Many pain points are already fixed on `v5-migration` — verify before drafting:

1. `git log --oneline -30` and `CHANGELOG.md` — search keywords from the observation.
2. Search existing issues/PRs: `gh issue list --search "<keywords>"`, `gh pr list ...`.
3. Reproduce on the current checkout (see Stage 3). If it no longer reproduces, the
   contribution may become a docs clarification or a regression test instead — both welcome.

### Stage 3 — Minimal reproduction (the heart of a bug report)

Build the smallest toy that shows the gap, e.g.:

```python
import pandas as pd
from retentioneering import Eventstream
df = pd.DataFrame({"user_id": ["u1","u1","u2"], "event": ["a","b","a"],
                   "timestamp": pd.date_range("2026-01-01", periods=3, freq="1min")})
# EXPECTED: ...       ACTUAL: ...
```

Rules: synthetic data only (never the user's real log); deterministic (fixed frames, no
randomness without seed); one behavior per repro; assert the expectation so the repro
doubles as a failing test.

### Stage 4 — Choose the contribution shape

| Situation | Shape |
|---|---|
| Clear defect with repro | Issue with repro; PR with fix + regression test if user wants to go further |
| Surprising-but-documented behavior | Docs PR (docstring is the source of truth — site pages regenerate from it) |
| Missing capability | Feature issue: use-case first, proposed signature second, evidence third (see templates) |
| Repeated workaround in user's code | Extract as proposed API: show the workaround, its cost, the proposed call replacing it |
| Wrong-conclusion trap (library was silent) | Frame as "missing signal": what the library knew and did not surface; propose the warning/field |

For API proposals, the accepted framing (from templates): problem → evidence of frequency
→ proposed signature → semantics incl. edge cases → acceptance criteria → migration notes.

### Stage 5 — Implement (when making a PR, not just an issue)

Read `references/repo-conventions.md` first. Non-negotiables:

1. Fork or branch from `master`-tracking `v5-migration`; one logical change per PR.
2. `uv sync` (or `make install`, which also runs `npm install`), then **`uv run pre-commit
   install`** — a one-time-per-clone step that wires the git hook so commits are auto-checked;
   skip it and commits bypass the hooks locally and CI's `lint` job flags the formatting on your
   PR. Add `make build` only when touching widgets/JS.
3. Follow naming conventions (ADR-0008): `path_col/event_col/timestamp_col/session_col`,
   `start_event/end_event`, verb-first processors, noun widgets, `<widget>_data` twins.
4. All DuckDB execution goes through the unified query engine (L1) — no ad-hoc
   `duckdb.sql` with replacement-scan idioms (superseded ADR-0002).
5. Add/extend tests next to the code area (`tests/...`); a bug fix MUST include the
   failing-before test from Stage 3.
6. Keep the loud-and-helpful error pattern: errors that list valid values/keys are the
   house style; silent degradation (dropped rows, empty results without a signal) is an
   auto-reject.
7. Sync obligations when renaming/changing API: MCP tool layer mirrors Eventstream
   method names; JS metric editor consumes the Python metric schema; docstrings feed the
   docs site — update all in the same PR (`uv run python docs/scripts/render_pages.py`).

### Stage 6 — Pre-flight and submit

```bash
uv run pre-commit run --all-files      # ruff lint+format, gitleaks, hygiene
uv run pytest tests/ -v                # full suite (CI runs 3.11–3.13)
uv run python docs/scripts/render_pages.py   # if docstrings changed
```

Commit style: imperative, scoped, explaining WHY when non-obvious (see `git log` for the
house voice). Update `CHANGELOG.md` under the unreleased/current section for
user-visible changes.

Submit:

```bash
git push -u origin <branch>
gh pr create --title "<imperative summary>" --body-file pr_body.md
```

PR body (template in `references/proposal-templates.md`): what & why → linked issue →
repro/before-after → tests added → sync checklist (docs/MCP/JS if applicable) →
breaking-change note. CI must pass: `lint` + `test (3.11/3.12/3.13)`. `master` is
PR-only; merging does not release (releases are tag-driven by maintainers).

### Stage 7 — Follow through

Respond to review within the PR (avoid force-push after review starts; append commits).
If maintainers ask for direction changes, update the issue first, then the code — the
issue is the contract.

## Portfolio mode: many observations at once

When the user accumulated a batch (e.g., a journal of friction from a project):
deduplicate → verify each against current version (Stage 2) → rank by
(frequency × silent-failure risk) → file the top 3–5 as separate issues with repros →
offer one PR for the cheapest verified fix to build credibility, referencing the issues
for the rest. Do not open one mega-issue.
