# Agent skills for retentioneering-tools

Task-oriented skill packages for coding agents (Codex, Claude Code, and compatible
tools). Each skill is a directory with a `SKILL.md` entry point; supporting material is
linked from it and read on demand.

| Skill | Use when |
|---|---|
| [`retentioneering-product-analytics/`](retentioneering-product-analytics/SKILL.md) | The user has event-level data (user, event, timestamp) or asks why users convert, churn, loop, or abandon a flow. Full workflow: inspect → recipe → execute → validate → interpret. |
| [`retentioneering-contributing/`](retentioneering-contributing/SKILL.md) | The user found a bug, wants a feature, keeps re-writing a workaround, or asks how to open an issue/PR against this repository. Full route: capture → validate → repro → issue/PR. |

## How to consume a skill (for agents without a native skill loader)

1. Read the target `SKILL.md` top to bottom; its YAML frontmatter carries `name`,
   `description` (activation criteria), and compatibility notes.
2. Follow the workflow sections in order. Load files under `references/` only at the
   step that cites them (they are sized to be read whole when needed).
3. Scripts under `scripts/` are directly executable helpers; run them with the
   project's Python environment, e.g.:
   `python .agents/skills/retentioneering-product-analytics/scripts/inspect_event_log.py data.csv`
4. All relative paths inside a skill resolve from that skill's directory.

## Layout convention

```
<skill-name>/
├── SKILL.md            # entry point: objective, activation, workflow
├── references/         # deep material, linked from SKILL.md, read on demand
└── scripts/            # executable helpers (plain Python, stdlib+pandas only)
```

The same skills are published for Claude Code under `.claude/skills/` — the content is
identical by design; treat `.claude/skills/` as the canonical source when they diverge.

## Maintenance

API facts inside `references/api-map.md` are version-verified against this checkout
(retentioneering 5.0, branch `v5-migration`). When the public API changes, re-verify the
map's claims (it states how) and update both copies.
