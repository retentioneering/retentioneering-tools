# ADR-0009: Cloud save/load ships disabled, with no bundled backend

Status: Accepted (5.0 rewrite; recorded 2026-07)

## Context

The Transition Graph and Step Matrix widgets support saving/loading display
state to a Supabase-backed cloud (`widgets/cloud.py`, `cloud_mixin.py`),
built for a hosted platform offering. This public repository does not run
that backend.

## Decision

- The cloud code ships in the open-source package, fully functional, but is
  **off by default** and gated entirely by environment variables with
  empty/false defaults:
  - `RETENTIONEERING_CLOUD_ENABLED` — hides the cloud UI when unset;
  - `RETENTIONEERING_CLOUD_SUPABASE_URL` / `_ANON_KEY` — read at JS build
    time (Vite `define`) and Python runtime;
  - `RETENTIONEERING_CLOUD_MANAGE_URL` — optional "manage" link.
- **No real credentials are ever hardcoded**, even for testing — the design
  goal is that the public artifact contains no bundled backend.
- Widgets without cloud support wrap content in an `AuthGate` with
  `disabled={true}` hardcoded on the JS side (currently a dead code path).
- Saved state carries the eventstream `fingerprint` (content hash); loading
  state saved for a different stream shows a mismatch warning and disables
  auto-save.

## Consequences

- `cloud_file_name` remains in `transition_graph` / `step_matrix` signatures
  but is intentionally undocumented until the hosted platform lands.
- gitleaks runs in pre-commit as a backstop against credential leaks.
