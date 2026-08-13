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
   `session_col`, `segment_col`. A **positional anchor** — one place in each
   path, described by an anchor spec (`pattern` / `at` / `occurrence` /
   `offset` / `offset_side`, see `paths/anchors.py`) — is `start_anchor` /
   `end_anchor` for a window (`truncate_paths`, `get_conversion_rate`) and
   `anchor` on its own (`add_events`). A parameter naming *boundary events*
   as a plain set of names, which is not a position, stays `start_event` /
   `end_event` (`split_sessions`, `collapse_events`, the `time_between`
   metric); it is renamed if and when it starts taking anchors. The
   "everything else" diff sentinel is `<REST>`.
2. **Number encodes the role, not the arity of the accepted value.** Singular
   when the parameter names *one role*, whether it then takes exactly one
   value (`event` in `has_event`, `path_col`, `segment_level`) or several
   alternatives for filling that one role (`start_anchor`, which takes a name,
   a spec or a list of them; `source_event`, where a list means "any of
   these"). Plural only when the collection *is* the concept — arity or order
   carries meaning (`funnel_events`: one event is not a funnel; `path_cols`:
   an ordered, nested grain hierarchy; `metrics`), or it is a set-valued
   definition against a wider vocabulary (`active_events` — "what counts as
   activity") — or when a singular of the same stem is already taken by
   "exactly one of these" and the plural has to contrast with it
   (`segment_level` / `segment_levels`, `path_col` / `path_cols`).

   The reason for tying the name to the role rather than to the accepted type:
   accepted types widen freely, and a name that encoded them would have to be
   renamed for it. `start_anchor` went from taking only a string to taking
   `str | dict | list` without the name becoming wrong; `has_event`'s
   list-accepting `events` was renamed to `event` in 5.0 precisely because the
   plural described the argument instead of the question, and "any of these"
   now belongs inside one value as an event class (`[a|b]`).
3. **Match ecosystem priors** where one exists: `n`/`frac`
   (pandas `sample`), `n_clusters`, `min_cluster_size`, `random_state`,
   `nmf_components` (sklearn), `keep`/`drop` column filters. Do not invent a
   local synonym for a concept the user's tools already name.
4. **Units live in names or in types.** Duration *inputs* are strings with an
   explicit unit (`"30m"`) or `pd.Timedelta`; bare numbers are rejected
   (`pd.Timedelta("1800")` silently means nanoseconds). Day-granularity args
   embed the unit (`inactivity_days`, `max_dormant_days`). Time *outputs*
   are always seconds.
5. **No implementation jargon in public names** (`ast_condition` →
   `condition`, `strip_cgi` → `strip_query`).
6. **Morphology encodes the API layer**: data processors are verb-first
   (`filter_`, `add_`, `drop_`, `rename_`, `collapse_`, `split_`,
   `truncate_`, `sample_`) or conversion-idiom (`to_daily_states`,
   `urls_to_events`, `to_dataframe`); widgets are nouns; headless methods
   are `<widget>_data`. The first positional "name of the created thing" is
   always `name`.
7. **Mode selection = argument selection.** Where a method has alternative
   modes, each mode is one mutually-exclusive argument
   (`keep`/`drop`/`func`/`sql`), not a dict of generic keys or a flag.
8. Deliberate exceptions are allowed but must be documented: `proba_out` /
   `proba_in` keep the sklearn-flavoured "proba" because the values are
   transition probabilities and that framing is the point.

## Consequences

- LLM-facing surfaces (docstrings, MCP tool docs) must enumerate every
  string-enum in full; enum values are API.
- Naming reviews are cheap only pre-release; post-5.0 renames require
  deprecation cycles, so new names get bikeshedded *before* merge against
  rules 1–7.
