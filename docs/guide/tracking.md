# Tracking

retentioneering collects anonymous usage analytics to help improve the library. This page explains exactly what is tracked and how to opt out.

## What we track

We track **method calls and widget actions only** — the fact that a method was called and whether it succeeded, never the data it operated on.

**Eventstream creation** (`eventstream_created`). The properties collected are exactly:

- dataset shape: the number of rows and the number of columns,
- the number of path columns, segment columns, and event columns declared in the schema — counts only, never the column names themselves.

This event does **not** carry the `default_args`/`non_default_args` split described below — `Eventstream(df, schema=...)` itself isn't wrapped, so there's no per-argument report of what you passed to the constructor, only these derived shape metrics.

**Data processor calls** (`dp_filter_events`, `dp_collapse_events`, ...) — which processor was called.

**Widget and headless method calls** (`widget_transition_graph`, `headless_step_matrix`, ...) — which visualization or `*_data` method was used.

**MCP server start** (`mcp_serve`) — whether a semantic-layer `context` was supplied (`has_context`, a boolean, never its contents).

For every tracked method call other than `eventstream_created` we also record:

- **Parameter names, never values.** Parameters that have a default are split into two lists, `default_args` and `non_default_args` — so we learn, for example, that `funnel_events` was customized, but never what it was set to. Values are never sent, since they may contain column names, event names, or query text.
- **Call status.** If the call raises, the event carries `status: "error"` plus `error_type` — either the library's stable internal error code (for `retentioneering.exceptions.RetentioneeringError` subclasses, e.g. `EMPTY_EVENTSTREAM`) or the exception's class name — never the exception message, which could echo your data back (e.g. a DuckDB error quoting the offending SQL or column name). On success the event carries `status: "success"`.

Every event also records whether the call was made by your code or by an agent through the MCP server.

## What we do not track

We **never** collect any sensitive data from your eventstream — no event names, no user identifiers, no path contents, no segment values, and no business metrics. We also never collect method parameter values or exception messages. Your data stays entirely on your machine.

We don't collect anything about your environment beyond: an anonymous device identifier (a one-way hash derived from the machine id, or a random UUID stored in `~/.retentioneering/config.json`, from which nothing about you or your machine can be recovered), the library version, OS and OS version, Python version, and the runtime environment (Jupyter, VS Code, Google Colab, or script).

## Opting out

Set the environment variable `RETENTIONEERING_NO_TRACK=1` before starting your notebook kernel:

```bash
# In your shell profile (.zshrc, .bashrc)
export RETENTIONEERING_NO_TRACK=1

# Or at the top of a notebook cell
import os
os.environ["RETENTIONEERING_NO_TRACK"] = "1"
```

### Google Colab

Add a secret named `RETENTIONEERING_NO_TRACK` with value `1` in Colab → Settings → Secrets, then enable notebook access for it. The secret persists across all Colab sessions automatically.
