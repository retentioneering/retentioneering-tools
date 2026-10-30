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
  population: string | null;
  group1_label: string | null;
  group2_label: string | null;
  edges: AbEdgeStat[];
}

/** How an edge is drawn under the overlay. */
export type AbEdgeClass = "increase" | "decrease" | "inconclusive" | "untested";

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
    population: payload.population ?? null,
    group1_label: payload.group1_label ?? null,
    group2_label: payload.group2_label ?? null,
    edges: payload.edges as AbEdgeStat[],
  };
}

/**
 * Whether the overlay describes what the graph draws now: the same edge weight
 * and the same diff (the sidebar can change both; the significance of one
 * weight says nothing about another).
 */
export function abPayloadApplies(
  payload: AbTransitionsPayload | null,
  valuesType: string,
  diff: unknown,
): boolean {
  if (!payload) return false;
  if (payload.edge_weight !== valuesType) return false;
  return JSON.stringify(payload.diff ?? null) === JSON.stringify(diff ?? null);
}

export const abEdgeKey = (source: string, target: string) => `${source}|${target}`;

/** Edge statistics by `source|target`. */
export function abEdgeIndex(payload: AbTransitionsPayload | null): Map<string, AbEdgeStat> {
  const index = new Map<string, AbEdgeStat>();
  payload?.edges.forEach((edge) => index.set(abEdgeKey(edge.source, edge.target), edge));
  return index;
}

/** The overlay class of an edge: significant with its sign, tested but
 *  inconclusive, or not tested (below the support rule, or not in the frame). */
export function abEdgeClass(stat: AbEdgeStat | undefined): AbEdgeClass {
  if (!stat) return "untested";
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
 * thin and faint; untested edges are grey, faint and dashed.
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
    default:
      return {
        edgeSize: Math.min(base.edgeSize, 1.5),
        edgeOpacity: 0.15,
        baseColor: AB_MUTED,
        dashed: true,
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

/** Lines the edge tooltip adds under the overlay: the delta with its interval,
 *  the adjusted p-value and the verdict. Shares (every weight but
 *  avg_per_path) are shown in percentage points, as the readings do. */
export function abTooltipLines(
  stat: AbEdgeStat | undefined,
  payload: AbTransitionsPayload,
): { label: string; value: string }[] {
  if (!stat) {
    return [{ label: "test", value: "not in ab_transitions" }];
  }
  const pp = payload.edge_weight !== "avg_per_path";
  const scale = pp ? 100 : 1;
  const unit = pp ? " pp" : "";
  const digits = pp ? 2 : 3;
  const level = payload.ci_level != null ? `${Math.round(payload.ci_level * 100)}% ` : "";
  const correction = payload.correction && payload.correction !== "none" ? ` (${payload.correction})` : "";
  const lines = [
    { label: "delta", value: `${fmt(stat.delta, scale, digits)}${unit}` },
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

/** One-line legend text of the overlay. */
export function abLegendText(payload: AbTransitionsPayload): string {
  const alpha = payload.alpha != null ? payload.alpha : 0.05;
  const correction = payload.correction && payload.correction !== "none" ? `${payload.correction}, ` : "";
  const tested = payload.n_tested != null ? `${payload.n_tested} tested edges` : "tested edges";
  return `Significant at ${alpha} (${correction}${tested}): red higher, blue lower in group 1; grey inconclusive; dashed not tested`;
}
