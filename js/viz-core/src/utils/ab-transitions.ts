/**
 * Significance overlay of a diff transition graph: the frame of
 * `Eventstream.get_ab_transitions`, passed to
 * `transition_graph(diff=..., ab_transitions=frame)` and shipped to the browser
 * as the `ab_transitions` payload (Python: `tools/ab_transitions.py`,
 * `transitions_payload`). The frame IS the data: there is no `_data` twin.
 *
 * Pure functions only, so the styling rules are unit-tested without cytoscape.
 */

export type AbVerdict =
  | "increase"
  | "decrease"
  | "inconclusive"
  | "insufficient_data"
  | "descriptive";

export interface AbEdgeStat {
  source: string;
  target: string;
  group1_value: number | null;
  group2_value: number | null;
  delta: number | null;
  ci_low: number | null;
  ci_high: number | null;
  p_adjusted: number | null;
  verdict: AbVerdict | string;
  support_warning?: string;
  conditional?: boolean;
}

export interface AbTransitionsPayload {
  edge_weight: string;
  /** The graph's `diff` the frame was validated against (JSON of the traitlet). */
  diff: unknown;
  correction: string | null;
  alpha: number | null;
  ci_level: number | null;
  n_tested: number | null;
  /** Rows of the whole frame; fewer `edges` than this is a filtered frame. */
  n_rows: number | null;
  population: string | null;
  horizon: string | null;
  path_col: string | null;
  group1_label: string | null;
  group2_label: string | null;
  edges: AbEdgeStat[];
}

/** How an edge is drawn under the overlay: `untested` has a row below the
 *  support rule, `absent` has no row (a filtered frame, or an edge outside the
 *  frame's window). */
export type AbEdgeClass = "increase" | "decrease" | "inconclusive" | "untested" | "absent";

export const AB_RED = "239, 68, 68"; // red-500: group 1 above group 2 (the diff palette)
export const AB_BLUE = "59, 130, 246"; // blue-500: group 1 below group 2
export const AB_MUTED = "156, 163, 175"; // gray-400

/** Parse the `ab_transitions` traitlet (JSON string or object); null when absent. */
export function parseAbTransitions(raw: unknown): AbTransitionsPayload | null {
  let value: unknown = raw;
  if (typeof raw === "string") {
    if (!raw) return null;
    try {
      value = JSON.parse(raw);
    } catch {
      return null;
    }
  }
  if (!value || typeof value !== "object") return null;
  const payload = value as Partial<AbTransitionsPayload>;
  if (typeof payload.edge_weight !== "string" || !Array.isArray(payload.edges)) {
    return null;
  }
  return {
    edge_weight: payload.edge_weight,
    diff: payload.diff ?? null,
    correction: payload.correction ?? null,
    alpha: payload.alpha ?? null,
    ci_level: payload.ci_level ?? null,
    n_tested: payload.n_tested ?? null,
    n_rows: payload.n_rows ?? null,
    population: payload.population ?? null,
    horizon: payload.horizon ?? null,
    path_col: payload.path_col ?? null,
    group1_label: payload.group1_label ?? null,
    group2_label: payload.group2_label ?? null,
    edges: payload.edges as AbEdgeStat[],
  };
}

/** A diff with every value as a string: a segment value re-picked in the
 *  sidebar comes back as a string even when the frame holds an integer. */
function normaliseDiff(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normaliseDiff);
  if (value === null || value === undefined) return null;
  return String(value);
}

/**
 * Whether the overlay describes what the graph draws now: the same edge weight,
 * the same diff (segment form or id lists; values compared as strings) and,
 * when known, the same path column (the sidebar can change all three; the
 * significance of one weight says nothing about another).
 */
export function abPayloadApplies(
  payload: AbTransitionsPayload | null,
  valuesType: string,
  diff: unknown,
  pathCol?: string | null,
): boolean {
  if (!payload) return false;
  if (payload.edge_weight !== valuesType) return false;
  if (pathCol && payload.path_col && pathCol !== payload.path_col) return false;
  return (
    JSON.stringify(normaliseDiff(payload.diff ?? null)) ===
    JSON.stringify(normaliseDiff(diff ?? null))
  );
}

export const abEdgeKey = (source: string, target: string) => `${source}|${target}`;

/** Edge statistics by `source|target`. */
export function abEdgeIndex(payload: AbTransitionsPayload | null): Map<string, AbEdgeStat> {
  const index = new Map<string, AbEdgeStat>();
  payload?.edges.forEach((edge) => index.set(abEdgeKey(edge.source, edge.target), edge));
  return index;
}

/** The overlay class of an edge: significant with its sign, tested but
 *  inconclusive, not tested (below the support rule), or absent from the frame. */
export function abEdgeClass(stat: AbEdgeStat | undefined): AbEdgeClass {
  if (!stat) return "absent";
  if (stat.verdict === "increase") return "increase";
  if (stat.verdict === "decrease") return "decrease";
  if (stat.verdict === "inconclusive" && stat.p_adjusted != null) return "inconclusive";
  return "untested";
}

export interface EdgeLook {
  edgeSize: number;
  edgeOpacity: number;
  baseColor: string;
  dashed: boolean;
}

/**
 * The look of an edge under the overlay, from the look the diff mode gives it
 * (`base`): significant edges keep their diff thickness (at least 3 px), full
 * colour of their sign and near-full opacity; inconclusive edges turn grey,
 * thin and faint; untested edges are grey, faint and dashed; edges without a
 * row in the frame are the faintest, solid.
 */
export function abEdgeLook(cls: AbEdgeClass, base: EdgeLook): EdgeLook {
  switch (cls) {
    case "increase":
    case "decrease":
      return {
        edgeSize: Math.max(3, base.edgeSize),
        edgeOpacity: Math.max(0.85, base.edgeOpacity),
        baseColor: cls === "increase" ? AB_RED : AB_BLUE,
        dashed: false,
      };
    case "inconclusive":
      return {
        edgeSize: Math.min(base.edgeSize, 2),
        edgeOpacity: 0.22,
        baseColor: AB_MUTED,
        dashed: false,
      };
    case "untested":
      return {
        edgeSize: Math.min(base.edgeSize, 1.5),
        edgeOpacity: 0.15,
        baseColor: AB_MUTED,
        dashed: true,
      };
    default:
      // absent: no row in the frame (filtered out, or outside its window)
      return {
        edgeSize: Math.min(base.edgeSize, 1),
        edgeOpacity: 0.07,
        baseColor: AB_MUTED,
        dashed: false,
      };
  }
}

function fmt(value: number | null | undefined, scale: number, digits: number): string {
  if (value == null || !Number.isFinite(value)) return "-";
  const v = value * scale;
  const text = v.toFixed(digits);
  return v > 0 ? `+${text}` : text;
}

/** p-value text: two significant digits, "< 0.0001" below (as the readings). */
export function formatP(p: number | null | undefined): string {
  if (p == null || !Number.isFinite(p)) return "-";
  if (p < 1e-4) return "< 0.0001";
  if (p >= 0.995) return "1";
  return p.toPrecision(2);
}

/** Whether the payload's values are shares (read in pp) rather than counts. */
export function abIsShare(payload: AbTransitionsPayload): boolean {
  return payload.edge_weight !== "avg_per_path";
}

/** An arm value of the test: a share as a percentage, a count as it is. */
export function formatAbValue(value: number | null | undefined, payload: AbTransitionsPayload): string {
  if (value == null || !Number.isFinite(value)) return "-";
  return abIsShare(payload) ? `${(value * 100).toFixed(2)} %` : value.toFixed(3);
}

/** A delta of the test: percentage points for shares, the count otherwise. */
export function formatAbDelta(value: number | null | undefined, payload: AbTransitionsPayload): string {
  const pp = abIsShare(payload);
  return `${fmt(value, pp ? 100 : 1, pp ? 2 : 3)}${pp ? " pp" : ""}`;
}

/**
 * The values the diff tooltip shows under the overlay: the test's arm values
 * and delta, so that the rows above the test lines are in the test's units
 * (for `unique_paths` the graph draws path counts while the test compares
 * shares, and with unequal arms the two deltas can have opposite signs).
 * Null for an edge without a row: the tooltip keeps the graph's values.
 */
export function abTooltipValues(
  stat: AbEdgeStat | undefined,
  payload: AbTransitionsPayload,
): { group1: number | null; group2: number | null; delta: number | null; subtitle: string } | null {
  if (!stat) return null;
  const what = abIsShare(payload)
    ? payload.edge_weight === "unique_paths"
      ? "share of paths with the transition"
      : `${payload.edge_weight}`
    : "transitions per path";
  return {
    group1: stat.group1_value,
    group2: stat.group2_value,
    delta: stat.delta,
    subtitle: `test values (ab_transitions): ${what}`,
  };
}

/** Lines the edge tooltip adds under the overlay: the interval, the adjusted
 *  p-value and the verdict (the delta is the tooltip's diff row, see
 *  `abTooltipValues`). Shares are shown in percentage points, as the readings. */
export function abTooltipLines(
  stat: AbEdgeStat | undefined,
  payload: AbTransitionsPayload,
): { label: string; value: string }[] {
  if (!stat) {
    return [{ label: "test", value: "no row in ab_transitions (filtered out, or outside its window)" }];
  }
  const pp = abIsShare(payload);
  const scale = pp ? 100 : 1;
  const unit = pp ? " pp" : "";
  const digits = pp ? 2 : 3;
  const level = payload.ci_level != null ? `${Math.round(payload.ci_level * 100)}% ` : "";
  const correction = payload.correction && payload.correction !== "none" ? ` (${payload.correction})` : "";
  const lines = [
    {
      label: `${level}interval`,
      value: `[${fmt(stat.ci_low, scale, digits)}, ${fmt(stat.ci_high, scale, digits)}]${unit}`,
    },
    { label: `adjusted p${correction}`, value: formatP(stat.p_adjusted) },
    { label: "verdict", value: String(stat.verdict) },
  ];
  if (stat.support_warning) lines.push({ label: "support", value: stat.support_warning });
  if (stat.conditional) lines.push({ label: "conditional", value: "post-treatment denominator" });
  return lines;
}

/** Whether the frame was filtered: fewer edges than the frame's rows. */
export function abIsFiltered(payload: AbTransitionsPayload): boolean {
  return payload.n_rows != null && payload.edges.length < payload.n_rows;
}

/** One-line legend text of the overlay: the level, the correction, the
 *  family size, the population and window, and how each class is drawn. */
export function abLegendText(payload: AbTransitionsPayload): string {
  const alpha = payload.alpha != null ? payload.alpha : 0.05;
  const correction = payload.correction && payload.correction !== "none" ? `${payload.correction}, ` : "";
  const tested = payload.n_tested != null ? `${payload.n_tested} tested edges` : "tested edges";
  const scope: string[] = [];
  if (payload.population === "assignment") scope.push("ITT over the assignment table");
  if (payload.horizon) scope.push(`horizon ${payload.horizon}`);
  if (abIsFiltered(payload)) scope.push(`frame filtered to ${payload.edges.length} of ${payload.n_rows} rows`);
  const where = scope.length ? `; ${scope.join(", ")}` : "";
  return (
    `Significant at ${alpha} (${correction}${tested}${where}): red higher, blue lower in group 1; ` +
    "grey inconclusive; dashed not tested; faint not in the frame"
  );
}
