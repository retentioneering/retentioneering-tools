# ADR-0013: Features deliberately not carried over from 3.3.0

Status: Accepted (5.0 rewrite; recorded 2026-07)

## Context

5.0 is a ground-up rewrite, not an incremental port. Carrying every 3.3.0
feature would have delayed the
release indefinitely; some features also depended on infrastructure that was
removed (iframe/CDN widgets, the pydantic GUI-schema system).

## Decision

Cut from 5.0, with the possibility of returning later:

- **Preprocessing Graph** — the interactive no-code pipeline builder. Its
  spirit survives in chained data processors (code) and in the MCP
  `update_base_stream` preprocessor lists (agents).
- **Cohorts**, **StatTests**, **Sequences** tools.
- Assorted 3.x Eventstream methods with no 5.0 equivalent (full list in
  `CHANGELOG.md` `[5.0.0]` → "Removed").

The `3.x` branch preserves the old engine for reference and patches;
`master` still reflects 3.3.0 until `v5-migration` lands.

## Consequences

- `README.md` still describes the 3.3.0 API until the migration branch
  merges; `CHANGELOG.md` is the accurate account of the differences.
- Requests for the removed tools should be answered with the roadmap
  position ("deliberately cut, may return"), not treated as regressions.
- Any future reimplementation should follow the 5.0 architecture (headless
  tool + optional widget twin, single metrics registry) rather than porting
  3.x code.
