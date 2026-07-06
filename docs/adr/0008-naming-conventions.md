# ADR-0008: 5.0 naming conventions

Status: Accepted (2026-07, pre-release naming review)

## Context

Before the 5.0 release (no compatibility burden yet) we
ran a full naming review optimizing for two audiences at once: humans coming
from Amplitude/Mixpanel/pandas/sklearn, and LLM agents whose priors come from
those same ecosystems. See `CHANGELOG.md` `[5.0.0]` → "Naming conventions"
for the shipped rename list.

## Decision

The rules, in force for all future API additions:

1. **One concept — one name.** Column-name arguments end in `_col` and use a
   single vocabulary: `path_col`, `event_col`, `timestamp_col`,
   `session_id_col`, `segment_col`. Window anchors are always the
   `start_event` / `end_event` pair (`truncate_paths`, `split_sessions`, the
   `time_between` metric). The "everything else" diff sentinel is `<REST>`.
2. **Match ecosystem priors** where one exists: `n`/`frac`
   (pandas `sample`), `n_clusters`, `min_cluster_size`, `random_state`,
   `nmf_components` (sklearn), `keep`/`drop` column filters. Do not invent a
   local synonym for a concept the user's tools already name.
3. **Units live in names or in types.** Duration *inputs* are strings with an
   explicit unit (`"30m"`) or `pd.Timedelta`; bare numbers are rejected
   (`pd.Timedelta("1800")` silently means nanoseconds). Day-granularity args
   embed the unit (`inactivity_days`, `max_dormant_days`). Time *outputs*
   are always seconds.
4. **No implementation jargon in public names** (`ast_condition` →
   `condition`, `strip_cgi` → `strip_query`).
5. **Morphology encodes the API layer**: data processors are verb-first
   (`filter_`, `add_`, `drop_`, `rename_`, `collapse_`, `split_`,
   `truncate_`, `sample_`) or conversion-idiom (`to_daily_states`,
   `urls_to_events`, `to_dataframe`); widgets are nouns; headless methods
   are `<widget>_data`. The first positional "name of the created thing" is
   always `name`.
6. **Mode selection = argument selection.** Where a method has alternative
   modes, each mode is one mutually-exclusive argument
   (`keep`/`drop`/`func`/`sql`), not a dict of generic keys or a flag.
7. Deliberate exceptions are allowed but must be documented: `proba_out` /
   `proba_in` keep the sklearn-flavoured "proba" because the values are
   transition probabilities and that framing is the point.

## Consequences

- LLM-facing surfaces (docstrings, MCP tool docs) must enumerate every
  string-enum in full; enum values are API.
- Naming reviews are cheap only pre-release; post-5.0 renames require
  deprecation cycles, so new names get bikeshedded *before* merge against
  rules 1–6.
