# Agent Skills

retentioneering ships two Agent Skills — task-oriented instruction packages that coding agents (Claude Code, Codex, Cursor, and other compatible tools) load automatically when a task matches, so the agent follows a tested workflow instead of improvising one from scratch each time.

This is different from the [MCP server](/docs/mcp): the MCP server runs inside a local Jupyter kernel, so using it means keeping a kernel process alive for the agent to connect to. Agent Skills carry no such requirement — they teach an agent to write and run retentioneering code directly, e.g. against your own files in its normal working environment, with no server to keep running.

## Available skills

| Skill | Use when |
|---|---|
| `retentioneering-product-analytics` | You have event-level data (user, event, timestamp columns) and ask an agent why users convert, churn, loop, or abandon a flow. Drives the full workflow: inspect the log → pick a recipe → execute → validate → interpret. |
| `retentioneering-contributing` | You ask an agent to turn a bug, friction report, or feature idea into an issue or pull request against this repository. Drives: capture → validate → minimal reproduction → issue/PR. |

## Where they live

The skills are duplicated under two directories in the repository, kept identical by design:

- `.claude/skills/` — loaded automatically by Claude Code.
- `.agents/skills/` — the generic/Codex-compatible location, per the open Agent Skills spec.

Each skill is a directory with a `SKILL.md` entry point (objective, activation criteria, step-by-step workflow) plus `references/` (deep material, read on demand) and, for the product-analytics skill, a `scripts/inspect_event_log.py` profiler.

## Using a skill

Inside a clone of `retentioneering-tools`, `.claude/skills/` already sits at the repo root, so
Claude Code picks it up with no setup. Elsewhere:

- **Claude Code**: copy the skill directory into `~/.claude/skills/` — it then applies across
  all your projects.

  ```bash
  cp -r /path/to/retentioneering-tools/.claude/skills/retentioneering-product-analytics \
        ~/.claude/skills/
  ```

- **Claude Cowork**: in the app, go to Customize → Skills → Add, and paste in the contents of
  the skill's `SKILL.md` from the repository.

- **Codex/other tools**: copy the skill directory into your project's `.agents/skills/`.

## Contributing to the skills

Skills are versioned alongside the library and welcome the same kind of contributions as the code — see [Contributing](https://github.com/retentioneering/retentioneering-tools/blob/master/CONTRIBUTING.md). If you change the public API, the product-analytics skill's `references/api-map.md` is version-verified against the checkout and should be re-checked; update both `.claude/skills/` and `.agents/skills/` copies together.
