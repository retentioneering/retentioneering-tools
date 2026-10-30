/**
 * A/B difference matrices (`ab_transitions`, `ab_steps`): the cell encoding and
 * the layout rules, as pure functions so they are unit-tested without React.
 *
 * Every number is a cell of the frame of `get_ab_transitions` / `get_ab_steps`
 * (through the widgets' twins `ab_transitions_data` / `ab_steps_data`), sent by
 * Python as it is: `null` (NaN on the Python side) is "no estimate", never 0.
 *
 *  - background: the delta on a diverging scale symmetric around 0, red where
 *    group 1 is higher, blue where it is lower (the diff graph's and the diff
 *    step matrix's hues), opacity proportional to |delta| / scale maximum;
 *  - wide inset outline: black when the adjusted p-value is below alpha; light
 *    grey and a light grey cross when tested and not significant; dashed light
 *    grey, the cross and muted text when not tested; nothing for a cell with no
 *    row in the frame.
 *
 * The same rules are written for pandas in `tools/ab_tables.py` (`styled=True`).
 */

/** group 1 above group 2: red-500, the diff palette (viz-core `AB_RED`). */
export const MATRIX_RED = "239, 68, 68";
/** group 1 below group 2: blue-500 (viz-core `AB_BLUE`). */
export const MATRIX_BLUE = "59, 130, 246";
/** Opacity of a background at the scale maximum (keeps dark text readable). */
export const MAX_ALPHA = 0.75;
export const OUTLINE_BLACK = "#111827";
export const OUTLINE_GREY = "#d1d5db";
export const CROSS_GREY = "#d1d5db";
export const TEXT_MAIN = "#111827";
export const TEXT_MUTED = "#9ca3af";
/** Width of the wide outline, px; drawn inside the cell so neighbours never overlap. */
export const OUTLINE_WIDTH = 2.5;

export type CellStatus = "significant" | "not_significant" | "untested" | "empty";
export type SortKey = "name" | "max_abs_delta" | "n_significant";
export type SignificanceFilter = "all" | "dim" | "hide";
export type IntervalKind = "per_cell" | "simultaneous";

/** One row of the frame (every column of `get_ab_transitions` / `get_ab_steps`). */
export interface MatrixRow {
  source?: string;
  target?: string;
  event?: string;
  step?: number;
  edge_weight?: string;
  group1_edge_paths?: number;
  group2_edge_paths?: number;
  population?: string;
  group1_paths?: number;
  group2_paths?: number;
  group1_support?: number;
  group2_support?: number;
  conditional?: boolean;
  group1_value?: number | null;
  group2_value?: number | null;
  delta?: number | null;
  delta_se?: number | null;
  delta_ci_low?: number | null;
  delta_ci_high?: number | null;
  delta_ci_low_adj?: number | null;
  delta_ci_high_adj?: number | null;
  rel_delta?: number | null;
  p_value?: number | null;
  p_value_adjusted?: number | null;
  correction_family?: string;
  mde_alpha?: number | null;
  mde?: number | null;
  rel_mde?: number | null;
  verdict?: string;
  test?: string;
  support_warning?: string;
  reading?: string | null;
  [key: string]: unknown;
}

/** The widget's `result` traitlet (see `widgets/_ab_matrix.py`). */
export interface MatrixPayload {
  tool: "get_ab_transitions" | "get_ab_steps" | string;
  rows: MatrixRow[];
  untested_reason: (string | null)[];
  row_keys: string[];
  col_keys: (string | number)[];
  attrs?: Record<string, unknown>;
  group1_label: string;
  group2_label: string;
  alpha: number;
  ci_level: number;
  correction: string;
  n_tested: number | null;
  n_rows: number | null;
  population: string | null;
  horizon: string | null;
  path_col: string | null;
  min_support: number;
  simultaneous: boolean;
  warnings: string[];
  unit: "pp" | "per_path";
  conditional: boolean;
  edge_weight?: string;
  edges?: string;
  max_steps?: number;
}

const isNum = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);

/** Parse the `result` traitlet; null for an empty or broken payload. */
export function parsePayload(raw: unknown): MatrixPayload | null {
  let value: unknown = raw;
  if (typeof raw === "string") {
    if (!raw) return null;
    try { value = JSON.parse(raw); } catch { return null; }
  }
  if (!value || typeof value !== "object") return null;
  const p = value as Partial<MatrixPayload>;
  if (!Array.isArray(p.rows) || !Array.isArray(p.row_keys) || !Array.isArray(p.col_keys)) return null;
  return {
    untested_reason: [],
    warnings: [],
    group1_label: "group 1",
    group2_label: "group 2",
    alpha: 0.05,
    ci_level: 0.95,
    correction: "bh",
    n_tested: null,
    n_rows: null,
    population: null,
    horizon: null,
    path_col: null,
    min_support: 10,
    simultaneous: false,
    unit: "pp",
    conditional: false,
    tool: "get_ab_transitions",
    ...p,
  } as MatrixPayload;
}

/** The two keys of a row: (source, target) or (event, step). */
export function rowKeyOf(row: MatrixRow, payload: Pick<MatrixPayload, "tool">): [string, string] {
  return payload.tool === "get_ab_steps"
    ? [String(row.event), String(row.step)]
    : [String(row.source), String(row.target)];
}

export const cellId = (r: string, c: string | number) => `${r}\u0000${c}`;

/** Rows of the frame by `row key \0 column key`, with their position in `rows`. */
export function indexRows(payload: MatrixPayload): Map<string, number> {
  const index = new Map<string, number>();
  payload.rows.forEach((row, i) => {
    const [r, c] = rowKeyOf(row, payload);
    index.set(cellId(r, c), i);
  });
  return index;
}

/** How a cell is drawn: significant (adjusted p-value below alpha), tested and
 *  not significant, not tested (no adjusted p-value), or empty (no row). The
 *  frame's columns decide, not the verdict's wording. */
export function cellStatus(row: MatrixRow | undefined | null, alpha: number): CellStatus {
  if (!row) return "empty";
  const p = row.p_value_adjusted;
  if (!isNum(p)) return "untested";
  return p < alpha ? "significant" : "not_significant";
}

export function deltaSign(delta: number | null | undefined): "positive" | "negative" | "zero" | "none" {
  if (!isNum(delta)) return "none";
  return delta > 0 ? "positive" : delta < 0 ? "negative" : "zero";
}

/** Opacity of a delta's background: |delta| / scale, capped at the maximum. */
export function backgroundAlpha(delta: number | null | undefined, scale: number): number {
  if (!isNum(delta) || delta === 0 || !(scale > 0)) return 0;
  return MAX_ALPHA * Math.min(1, Math.abs(delta) / scale);
}

/** The cell background: red for a positive delta, blue for a negative one. */
export function cellBackground(delta: number | null | undefined, scale: number): string {
  const a = backgroundAlpha(delta, scale);
  if (a === 0) return "transparent";
  return `rgba(${(delta as number) > 0 ? MATRIX_RED : MATRIX_BLUE}, ${a.toFixed(3)})`;
}

export interface CellLook {
  outlineColor: string | null;
  outlineStyle: "solid" | "dashed" | null;
  cross: boolean;
  textColor: string;
}

/** The outline, cross and text colour of a status (the maintainer's spec). */
export function cellLook(status: CellStatus): CellLook {
  switch (status) {
    case "significant":
      return { outlineColor: OUTLINE_BLACK, outlineStyle: "solid", cross: false, textColor: TEXT_MAIN };
    case "not_significant":
      return { outlineColor: OUTLINE_GREY, outlineStyle: "solid", cross: true, textColor: TEXT_MAIN };
    case "untested":
      return { outlineColor: OUTLINE_GREY, outlineStyle: "dashed", cross: true, textColor: TEXT_MUTED };
    default:
      return { outlineColor: null, outlineStyle: null, cross: false, textColor: TEXT_MUTED };
  }
}

/** A cross from corner to corner as two CSS gradients (one element per cell,
 *  so a matrix of thousands of cells stays light). */
export function crossImage(color: string = CROSS_GREY, width = 1.25): string {
  const half = width / 2;
  const line = (dir: string) =>
    `linear-gradient(${dir}, transparent calc(50% - ${half}px), ${color} calc(50% - ${half}px), ` +
    `${color} calc(50% + ${half}px), transparent calc(50% + ${half}px))`;
  return `${line("to top right")}, ${line("to bottom right")}`;
}

// ── units and numbers ──────────────────────────────────────────────────────

export interface UnitInfo {
  /** Factor from the frame's values to the shown numbers (100 for pp). */
  scale: number;
  /** Suffix of a shown delta: "pp" or "". */
  suffix: string;
  /** What a delta is, for the header. */
  label: string;
}

export function unitInfo(payload: Pick<MatrixPayload, "unit" | "tool" | "edge_weight">): UnitInfo {
  if (payload.unit === "per_path") {
    return { scale: 1, suffix: "", label: "transitions per path" };
  }
  const what = payload.tool === "get_ab_steps"
    ? "percentage points of the arm's paths"
    : payload.edge_weight === "unique_paths" || !payload.edge_weight
      ? "percentage points of the arm's paths"
      : `percentage points (${payload.edge_weight})`;
  return { scale: 100, suffix: "pp", label: what };
}

/** Decimals that show the largest magnitude with three significant digits
 *  (as the readings' `_decimals`), at most 4; percentage points: 1 from 10 up,
 *  else 2. */
export function decimalsFor(unit: UnitInfo, ...values: (number | null | undefined)[]): number {
  const top = Math.max(0, ...values.filter(isNum).map((v) => Math.abs(v * unit.scale)));
  if (unit.suffix === "pp") return top >= 10 ? 1 : 2;
  if (top === 0) return 2;
  return Math.min(4, Math.max(0, 2 - Math.floor(Math.log10(top))));
}

/** `+1.23` / `-1.07` / `0.00` (ASCII minus, as the readings). */
export function fmtSigned(value: number | null | undefined, decimals: number, scale = 1): string {
  if (!isNum(value)) return "n/a";
  const text = (value * scale).toFixed(decimals);
  if (Number(text) === 0) return (0).toFixed(decimals);
  return value > 0 ? `+${text}` : text;
}

/** The interval of a row: per cell, or Bonferroni's simultaneous one when asked
 *  for and present. */
export function intervalOf(row: MatrixRow, kind: IntervalKind): [number | null, number | null] {
  if (kind === "simultaneous" && (isNum(row.delta_ci_low_adj) || isNum(row.delta_ci_high_adj))) {
    return [row.delta_ci_low_adj ?? null, row.delta_ci_high_adj ?? null];
  }
  return [row.delta_ci_low ?? null, row.delta_ci_high ?? null];
}

/** The cell's two lines: the delta and `[low, high]`, in the frame's units. */
export function cellText(row: MatrixRow, unit: UnitInfo, kind: IntervalKind): { delta: string; interval: string | null } {
  const [lo, hi] = intervalOf(row, kind);
  const d = decimalsFor(unit, row.delta, lo, hi);
  const delta = fmtSigned(row.delta, d, unit.scale);
  const interval = isNum(lo) || isNum(hi)
    ? `[${fmtSigned(lo, d, unit.scale)}, ${fmtSigned(hi, d, unit.scale)}]`
    : null;
  return { delta, interval };
}

/** An arm's value: a share as a percentage, a count per path as it is. */
export function fmtArmValue(value: number | null | undefined, unit: UnitInfo): string {
  if (!isNum(value)) return "n/a";
  if (unit.suffix === "pp") return `${(value * 100).toFixed(2)} %`;
  return value.toFixed(Math.max(3, decimalsFor(unit, value)));
}

/** p-value text: two significant digits, "< 0.0001" below (as the readings). */
export function fmtP(p: number | null | undefined): string {
  if (!isNum(p)) return "n/a";
  if (p < 1e-4) return "< 0.0001";
  if (p >= 0.995) return "1";
  return p.toPrecision(2);
}

// ── colour scale ───────────────────────────────────────────────────────────

/** The scale maximum: the largest |delta| among the given rows, or the clip
 *  when one is set (> 0). */
export function scaleMax(rows: (MatrixRow | undefined)[], clip: number | null | undefined): number {
  if (isNum(clip) && clip > 0) return clip;
  let top = 0;
  for (const row of rows) {
    const d = row?.delta;
    if (isNum(d) && Math.abs(d) > top) top = Math.abs(d);
  }
  return top;
}

// ── axes: order, filter ────────────────────────────────────────────────────

export interface AxisStat {
  maxAbsDelta: number;
  nSignificant: number;
}

/** Per row key and per column key: the largest |delta| and the number of
 *  significant cells, over the given cells. */
export function axisStats(
  cells: { r: string; c: string; row: MatrixRow }[],
  alpha: number,
): { rows: Map<string, AxisStat>; cols: Map<string, AxisStat> } {
  const rows = new Map<string, AxisStat>();
  const cols = new Map<string, AxisStat>();
  const bump = (map: Map<string, AxisStat>, key: string, row: MatrixRow) => {
    const s = map.get(key) ?? { maxAbsDelta: 0, nSignificant: 0 };
    if (isNum(row.delta)) s.maxAbsDelta = Math.max(s.maxAbsDelta, Math.abs(row.delta));
    if (cellStatus(row, alpha) === "significant") s.nSignificant += 1;
    map.set(key, s);
  };
  for (const { r, c, row } of cells) {
    bump(rows, r, row);
    bump(cols, c, row);
  }
  return { rows, cols };
}

/** Keys in display order: pinned first (in their own order), then by `sort`
 *  (largest first; ties and "name" keep the payload's order). */
export function orderKeys(
  keys: string[],
  sort: SortKey,
  stats: Map<string, AxisStat>,
  pinned: Set<string> = new Set(),
): string[] {
  const position = new Map(keys.map((k, i) => [k, i]));
  const value = (k: string) => {
    const s = stats.get(k);
    if (!s) return 0;
    return sort === "max_abs_delta" ? s.maxAbsDelta : sort === "n_significant" ? s.nSignificant : 0;
  };
  const sorted = [...keys].sort((a, b) => {
    const pa = pinned.has(a) ? 0 : 1;
    const pb = pinned.has(b) ? 0 : 1;
    if (pa !== pb) return pa - pb;
    if (sort !== "name") {
      const diff = value(b) - value(a);
      if (diff !== 0) return diff;
    }
    return (position.get(a) ?? 0) - (position.get(b) ?? 0);
  });
  return sorted;
}

/** Whether a row or column key passes the significance filter: it has a
 *  significant cell, or it is pinned. */
export function keepsSignificance(key: string, stats: Map<string, AxisStat>, pinned: Set<string>): boolean {
  return pinned.has(key) || (stats.get(key)?.nSignificant ?? 0) > 0;
}

// ── words ──────────────────────────────────────────────────────────────────

/** How to read a conditional weight's matrix (its denominators are behaviour
 *  the treatment may change); null for the per-path weights. */
export function readingHint(edgeWeight: string | undefined): string | null {
  if (edgeWeight === "proba_out") {
    return "proba_out is row-normalised: in each arm a row sums to 100 %, so a row's deltas sum to 0. "
      + "Read across a row: where the transitions out of the source go, among the paths that reach it "
      + "(conditional on post-treatment behaviour, not an effect over the whole arm).";
  }
  if (edgeWeight === "proba_in") {
    return "proba_in is column-normalised: in each arm a column sums to 100 %, so a column's deltas sum to 0. "
      + "Read down a column: where the transitions into the target come from (conditional on post-treatment behaviour).";
  }
  if (edgeWeight === "share_of_total") {
    return "share_of_total: in each arm the whole matrix sums to 100 %, so a cell also moves when any other "
      + "transition changes (composition; conditional on post-treatment behaviour).";
  }
  return null;
}

/** The family the adjusted p-values belong to, in words. */
export function familyText(payload: Pick<MatrixPayload, "correction" | "n_tested" | "n_rows" | "tool">): string {
  const what = payload.tool === "get_ab_steps" ? "cells" : "edges";
  const tested = payload.n_tested != null ? `${payload.n_tested}` : "?";
  const of = payload.n_rows != null ? ` of ${payload.n_rows}` : "";
  return payload.correction === "none"
    ? `no correction, ${tested}${of} ${what} tested`
    : `${payload.correction} over ${tested}${of} tested ${what}`;
}

/** The population and window, in words. */
export function populationText(payload: Pick<MatrixPayload, "population" | "horizon">): string {
  const who = payload.population === "assignment"
    ? "ITT: every path of the assignment table"
    : "the stream's paths";
  return payload.horizon ? `${who}, horizon ${payload.horizon}` : who;
}

/** The note on what the intervals are. */
export function intervalNote(level: number, kind: IntervalKind, simultaneous: boolean): string {
  const pct = `${+(level * 100).toFixed(1)}%`;
  if (kind === "simultaneous" && simultaneous) {
    return `Intervals: Bonferroni simultaneous (all cells at once, ${pct} family-wise).`;
  }
  return `Intervals: ${pct} per cell, not simultaneous; the outline reads the adjusted p-value, so a grey cell's interval may exclude 0.`;
}
