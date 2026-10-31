/**
 * A/B compression widget (`ab_compression`): the payload and the pure rules of
 * the path chart and the mapping table, unit-tested without React.
 *
 * Every number is the twin's (`ab_compression_data`, an alias of
 * `get_ab_compression`): the path rows of `attrs["path"]` and, per event, the
 * steps at which its label changes (`attrs["mappings"]`, sent as `changes`).
 * The label of an event at step k is its last change at or before k.
 */

/** One step of the greedy path (`attrs["path"]`). */
export interface PathStep {
  step: number;
  operation: string;
  events: string;
  label: string | null;
  vocabulary: number;
  cells: number;
  testable: number;
  untestable: number;
  median_mde: number | null;
  transitions: number;
  mass_retained: number | null;
  artificial_transitions: number;
  artificial_mass: number;
  reason: string;
}

/** One segment's plan (`by`), or the only one (`key` "all"). */
export interface Segment {
  key: string;
  chosen_step: number;
  path: PathStep[];
  events: string[];
  path_share: (number | null)[];
  /** Per event, `[step, label]` at step 0 and at every change (null: dropped). */
  changes: Record<string, [number, string | null][]>;
  /** Why a kept event is kept (the plan's reason at the chosen step). */
  kept_reason: Record<string, string>;
  homogeneity: Record<string, unknown> | null;
}

export interface CompressionPayload {
  tool: string;
  screen: "transitions" | "steps" | string;
  edge_weight: string | null;
  max_steps: number | null;
  unit: "pp" | "per_path";
  target: unknown;
  other: string;
  keep: string[] | null;
  by: string | null;
  by_pre_exposure: boolean | null;
  min_path_share: number;
  chain_threshold: number;
  synonym_threshold: number;
  bridge_threshold: number;
  max_events: number | null;
  min_support: number;
  correction: string;
  ci_level: number;
  power: number;
  population: string | null;
  horizon: string | null;
  path_col: string | null;
  group1_paths: number | null;
  group2_paths: number | null;
  segments: Segment[];
  warnings: string[];
}

export type Action = "keep" | "merge" | "drop" | "other";

const isNum = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);

/** Parse the `result` traitlet; null for an empty or broken payload. */
export function parseCompression(raw: unknown): CompressionPayload | null {
  let value: unknown = raw;
  if (typeof raw === "string") {
    if (!raw) return null;
    try { value = JSON.parse(raw); } catch { return null; }
  }
  if (!value || typeof value !== "object") return null;
  const p = value as Partial<CompressionPayload>;
  if (!Array.isArray(p.segments) || !p.segments.length) return null;
  return { warnings: [], other: "other", unit: "pp", ...p } as CompressionPayload;
}

/** The selected step: the view's, within the path, or the chosen one (-1). */
export function stepOf(segment: Segment, selected: number): number {
  const last = segment.path.length - 1;
  if (!Number.isInteger(selected) || selected < 0) return Math.min(segment.chosen_step, last);
  return Math.min(selected, last);
}

/** The label of `event` at `step`, and the step that set it. */
export function labelAt(segment: Segment, event: string, step: number): { label: string | null; since: number } {
  const changes = segment.changes[event] ?? [[0, event]];
  let label: string | null = event;
  let since = 0;
  for (const [s, l] of changes) {
    if (s > step) break;
    label = l;
    since = s;
  }
  return { label, since };
}

/** The plan's `action` of an event mapped to `label` (`action_of` in Python). */
export function actionOf(event: string, label: string | null, other: string): Action {
  if (label === null) return "drop";
  if (label === event) return "keep";
  if (label === other) return "other";
  return "merge";
}

export interface MappingRow {
  event: string;
  pathShare: number | null;
  action: Action;
  label: string | null;
  /** The step that set the label (0: unchanged). */
  since: number;
  operation: string;
  reason: string;
}

/** The mapping table at `step`: every event (or only those whose label changed),
 *  the changed ones first by the step that set them, then by name. */
export function mappingAt(segment: Segment, step: number, other: string, changedOnly: boolean): MappingRow[] {
  const byStep = new Map(segment.path.map((p) => [p.step, p]));
  const rows: MappingRow[] = segment.events.map((event, i) => {
    const { label, since } = labelAt(segment, event, step);
    const action = actionOf(event, label, other);
    const op = since > 0 ? byStep.get(since) : undefined;
    const reason = action === "keep"
      ? (segment.kept_reason[event] ?? "kept")
      : op?.reason ?? "";
    return {
      event, pathShare: segment.path_share[i] ?? null, action, label, since,
      operation: action === "keep" ? "keep" : op?.operation ?? "", reason,
    };
  });
  const shown = changedOnly ? rows.filter((r) => r.action !== "keep") : rows;
  return shown.sort((a, b) => (a.since - b.since) || (a.event < b.event ? -1 : a.event > b.event ? 1 : 0));
}

/** The number of events per action at `step`. */
export function actionCounts(segment: Segment, step: number, other: string): Record<Action, number> {
  const counts: Record<Action, number> = { keep: 0, merge: 0, drop: 0, other: 0 };
  for (const event of segment.events) counts[actionOf(event, labelAt(segment, event, step).label, other)] += 1;
  return counts;
}

// ── the path chart ─────────────────────────────────────────────────────────

/** The three panels of the path chart: what each draws and how it reads. */
export type Measure = "testable" | "median_mde" | "mass_retained";
export const MEASURES: { key: Measure; title: string }[] = [
  { key: "testable", title: "Testable cells" },
  { key: "median_mde", title: "Median MDE" },
  { key: "mass_retained", title: "Transition mass retained" },
];

export interface Panel {
  measure: Measure;
  path: string;
  points: { step: number; x: number; y: number | null; value: number | null; artificial: boolean }[];
  min: number;
  max: number;
}

/** x of a vocabulary size: the largest on the right (the path moves left as
 *  events are merged or dropped). */
export function xScale(path: PathStep[], width: number, pad: number): (v: number) => number {
  const values = path.map((p) => p.vocabulary).filter(isNum);
  const lo = Math.min(...values), hi = Math.max(...values);
  if (!values.length || hi === lo) return () => width / 2;
  return (v: number) => pad + ((v - lo) / (hi - lo)) * (width - 2 * pad);
}

/** One panel's geometry: a line through the steps in order, one point each. A
 *  step that creates artificial transitions is flagged. */
export function panelGeometry(path: PathStep[], measure: Measure, width: number, height: number, pad = 6): Panel {
  const x = xScale(path, width, pad);
  const values = path.map((p) => (isNum(p[measure]) ? (p[measure] as number) : null));
  const finite = values.filter(isNum) as number[];
  let min = finite.length ? Math.min(...finite) : 0;
  let max = finite.length ? Math.max(...finite) : 1;
  if (measure === "mass_retained") { min = Math.min(min, 0.9); max = 1; }
  if (max === min) { max = min + (min === 0 ? 1 : Math.abs(min) * 0.1); }
  const y = (v: number) => height - pad - ((v - min) / (max - min)) * (height - 2 * pad);
  const points = path.map((p, i) => ({
    step: p.step, x: x(p.vocabulary), y: values[i] == null ? null : y(values[i] as number), value: values[i],
    artificial: (p.artificial_transitions ?? 0) > 0,
  }));
  const drawn = points.filter((p) => p.y != null);
  const line = drawn.map((p, i) => `${i ? "L" : "M"}${p.x.toFixed(1)},${(p.y as number).toFixed(1)}`).join(" ");
  return { measure, path: line, points, min, max };
}

// ── numbers ────────────────────────────────────────────────────────────────

/** An MDE in the shown units: percentage points, or per path. */
export function fmtMde(value: number | null | undefined, unit: "pp" | "per_path"): string {
  if (!isNum(value)) return "n/a";
  return unit === "pp" ? `${(value * 100).toFixed(2)} pp` : value.toFixed(3);
}

export function fmtShare(value: number | null | undefined, digits = 1): string {
  if (!isNum(value)) return "n/a";
  return `${(value * 100).toFixed(digits)} %`;
}

/** A measure of a step, as the chart and the before/after table show it. */
export function fmtMeasure(step: PathStep, measure: Measure, unit: "pp" | "per_path"): string {
  if (measure === "median_mde") return fmtMde(step.median_mde, unit);
  if (measure === "mass_retained") return fmtShare(step.mass_retained, 2);
  return `${step.testable} of ${step.cells}`;
}

/** The rows of the before / after table: step 0 against the selected step. */
export function beforeAfter(path: PathStep[], step: number, unit: "pp" | "per_path"): [string, string, string][] {
  const a = path[0], b = path[Math.min(step, path.length - 1)];
  if (!a || !b) return [];
  return [
    ["vocabulary", String(a.vocabulary), String(b.vocabulary)],
    ["cells", String(a.cells), String(b.cells)],
    ["testable cells", String(a.testable), String(b.testable)],
    ["untestable cells", String(a.untestable), String(b.untestable)],
    ["median MDE", fmtMde(a.median_mde, unit), fmtMde(b.median_mde, unit)],
    ["transition mass retained", fmtShare(a.mass_retained, 2), fmtShare(b.mass_retained, 2)],
    ["artificial transitions", String(a.artificial_transitions), String(b.artificial_transitions)],
  ];
}

/** One step in words: what it did. */
export function stepText(step: PathStep): string {
  if (step.operation === "start") return "step 0: the stream as it is";
  const what: Record<string, string> = {
    merge_chain: "merge a chain",
    merge_synonym: "merge synonyms",
    drop: "drop",
    other: "relabel to other",
  };
  const into = step.label ? ` → ${step.label}` : "";
  return `step ${step.step}: ${what[step.operation] ?? step.operation} ${step.events}${into}`;
}
