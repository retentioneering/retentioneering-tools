/**
 * Single framework-agnostic interface between a viz-core component/store and
 * whatever is hosting it: a live anywidget model in Jupyter, a static blob of
 * pre-computed data (HTML export), a REST backend (future web platform), or a
 * test double. Nothing in viz-core should know which of these it's talking to.
 *
 * `WidgetHost` supersedes `DataProvider` (below): `DataProvider` used to be
 * "the compute-only slice of whatever transport we're on"; `WidgetHost` is
 * the whole transport — param get/set/subscribe *and* compute. Every
 * `WidgetHost` is structurally a `DataProvider`, so existing call sites typed
 * against `DataProvider` keep working unchanged when handed a `WidgetHost`.
 */
export interface WidgetHost {
  /** Read a widget parameter (mirrors an anywidget traitlet / model.get key). */
  get(key: string): unknown;
  /** Write a single parameter and flush it to whatever is on the other side. */
  set(key: string, value: unknown): void;
  /**
   * Write several parameters as one atomic flush — used by "Apply" style
   * actions that stage a batch of changes and commit them together. This is
   * not just a convenience: on the anywidget host it maps to a single
   * `save_changes()` call (one comm message, one round of Python-side
   * observers), matching what the hand-written model.set(...)×N +
   * save_changes() blocks did before this abstraction existed. Calling
   * `set()` N times instead would fire N separate updates.
   */
  setMany(values: Record<string, unknown>): void;
  /** Subscribe to changes of a single parameter; returns an unsubscribe fn. */
  onChange(key: string, cb: () => void): () => void;
  /** Run a named backend computation (mirrors the compute_request/compute_response RPC). */
  compute<T = unknown>(tool: string, params: Record<string, unknown>): Promise<T>;
}

// ── DataProvider (legacy alias) ─────────────────────────────────────────────

/**
 * The historical, compute-only abstraction. Kept as a derived subset of
 * `WidgetHost` — rather than a separate hand-maintained interface — so it can
 * never drift, and so any `WidgetHost` is usable wherever a `DataProvider`
 * was expected (structural typing: a `WidgetHost` has everything a
 * `DataProvider` needs). New viz-core code should depend on `WidgetHost`;
 * `DataProvider` remains only for call sites that genuinely need nothing but
 * `compute()`.
 */
export type DataProvider = Pick<WidgetHost, "compute">;
