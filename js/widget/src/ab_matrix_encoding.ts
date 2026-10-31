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
 * The same rules are written for pandas in `tools/ab_tables.py` (`styled=True`),
 * whose colours and opacity these are (generated, `make export-ab-encoding`).
 *
 * The "data needed" view reads the payload's `power` (the machinery of
 * `get_ab_transitions_power` / `get_ab_steps_power`): the background is the
 * cell's MDE (or, with a `target_delta`, the multiple of paths the MDE needs to
 * reach it, with the family projected at that size: `scaleNeeded`, the same
 * arithmetic as Python's `solve_scale_needed`) on one hue, a cell not testable at
 * this size is hatched and shows the multiple of paths at which it passes the
 * support rule, and the header draws the curve of testable cells and median MDE
 * against the paths. A path is one `path_col` (a user, or a session).
 */
import * as encoding from "./generated/ab_encoding.generated";

/** group 1 above group 2: red-500, the diff palette (viz-core `AB_RED`). */
export const MATRIX_RED = encoding.RED;
/** group 1 below group 2: blue-500 (viz-core `AB_BLUE`). */
export const MATRIX_BLUE = encoding.BLUE;
/** Opacity of a background at the scale maximum (keeps dark text readable). */
export const MAX_ALPHA = encoding.MAX_ALPHA;
export const OUTLINE_BLACK = encoding.SIGNIFICANT_BORDER;
export const OUTLINE_GREY = encoding.MUTED_BORDER;
export const CROSS_GREY = encoding.MUTED_BORDER;
export const TEXT_MAIN = encoding.MAIN_TEXT;
export const TEXT_MUTED = encoding.UNTESTED_TEXT;
/** The "data needed" view's hue (MDE, or the paths a target needs). */
export const NEED_HUE = encoding.NEED;
/** The hatch of a cell that is not testable at the current size. */
export const HATCH_COLOR = encoding.HATCH;
/** Width of the wide outline, px; drawn inside the cell so neighbours never overlap. */
export const OUTLINE_WIDTH = 2.5;

export type CellStatus = "significant" | "not_significant" | "untested" | "empty";
export type SortKey = "name" | "max_abs_delta" | "n_significant";
export type SignificanceFilter = "all" | "dim" | "hide";
export type IntervalKind = "per_cell" | "simultaneous";
export type ViewMode = "delta" | "data_needed";

/** One point of the power curve (`attrs["curve"]` of the power getters). */
export interface CurvePoint {
  scale: number;
  group1_paths: number;
  group2_paths: number;
  cells: number;
  testable: number;
  family_size: number;
  mde_alpha: number | null;
  mde_median: number | null;
  mde_q25: number | null;
  mde_q75: number | null;
  /** Cells that pass the support rule at this scale, and those of them whose
   *  spread is unknown (counted in the family, testable unknown). */
  supported?: number;
  unknown_spread?: number;
  /** Cells no scale makes testable: constant, or without an estimate. */
  no_spread?: number;
  no_estimate?: number;
}

/** What more paths of the same composition can do for a row (`inference` of the
 *  power getters). */
export type Inference = "tested" | "projectable" | "unknown_spread" | "no_spread" | "no_estimate";

/** The projected family as a step function of the scale (`family_steps`): on
 *  `[breakpoints[k], breakpoints[k + 1])` it has `sizes[k]` rows and an MDE is
 *  `factors[k] * base / sqrt(scale)`; `breakpoints[0]` is 0. */
export interface FamilyInfo {
  breakpoints: number[];
  sizes: number[];
  factors: number[];
  /** The factor at the current size (scale 1). */
  factor_now: number;
}

/** The payload's `power` (`power_payload` in `widgets/_ab_matrix.py`), decoded. */
export interface PowerInfo {
  scale: number[];
  curve: CurvePoint[];
  /** Per row of the frame: the multiple of paths at which it passes the support
   *  rule (null: it never joins the family). */
  scale_to_test: (number | null)[];
  /** Per row: its inference status. */
  inference?: (Inference | null)[];
  /** Per row: its standard error at the current size (null: no spread). */
  base?: (number | null)[];
  family?: FamilyInfo | null;
  sizing_basis?: string;
  /** What one path is (the screen's `path_col`). */
  unit_of_paths?: string | null;
  level: string;
  assumption: string;
}

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

/** One column of the frame as `encode_column` (`widgets/_ab_matrix.py`) sends it:
 *  the values as they are, or `levels` indexed by `codes`; without `codes` every
 *  row holds `levels[0]`. */
export interface EncodedColumn {
  values?: unknown[];
  levels?: unknown[];
  codes?: number[];
}

/** The frame's rows, column by column (`encode_frame`). */
export interface EncodedFrame {
  length: number;
  columns: (EncodedColumn & { name: string })[];
}

/** The widget's `result` traitlet as `parsePayload` returns it: the frame's rows
 *  decoded (see `widgets/_ab_matrix.py`, which sends `frame` in place of `rows`). */
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
  power?: PowerInfo | null;
}

const isNum = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);

/** The values of an encoded column of `length` rows (`decode_column`). */
export function decodeColumn(column: EncodedColumn | null | undefined, length: number): unknown[] {
  if (!column) return new Array(length).fill(null);
  if (Array.isArray(column.values)) return column.values;
  const levels = column.levels ?? [];
  if (Array.isArray(column.codes)) return column.codes.map((code) => levels[code]);
  return new Array(length).fill(levels[0] ?? null);
}

/** The frame's rows, one object per row in the frame's column order. */
export function decodeFrame(frame: EncodedFrame): MatrixRow[] {
  const columns = frame.columns.map((c) => [c.name, decodeColumn(c, frame.length)] as const);
  const rows: MatrixRow[] = [];
  for (let i = 0; i < frame.length; i++) {
    const row: MatrixRow = {};
    for (const [name, values] of columns) row[name] = values[i];
    rows.push(row);
  }
  return rows;
}

/** Parse the `result` traitlet; null for an empty or broken payload. */
export function parsePayload(raw: unknown): MatrixPayload | null {
  let value: unknown = raw;
  if (typeof raw === "string") {
    if (!raw) return null;
    try { value = JSON.parse(raw); } catch { return null; }
  }
  if (!value || typeof value !== "object") return null;
  const { frame, untested_reason, power, ...p } = value as Partial<Omit<MatrixPayload, "rows" | "untested_reason" | "power">> & {
    frame?: EncodedFrame; untested_reason?: EncodedColumn;
    power?: Omit<PowerInfo, "scale_to_test" | "inference" | "base"> & {
      scale_to_test?: EncodedColumn; inference?: EncodedColumn; base?: EncodedColumn;
    };
  };
  if (!frame || !Array.isArray(frame.columns) || !Array.isArray(p.row_keys) || !Array.isArray(p.col_keys)) return null;
  return {
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
    rows: decodeFrame(frame),
    untested_reason: decodeColumn(untested_reason, frame.length) as (string | null)[],
    power: power && Array.isArray(power.curve)
      ? {
        ...power,
        scale_to_test: decodeColumn(power.scale_to_test, frame.length) as (number | null)[],
        inference: decodeColumn(power.inference, frame.length) as (Inference | null)[],
        base: decodeColumn(power.base, frame.length) as (number | null)[],
      }
      : null,
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
    : `${payload.correction} over ${tested} tested${of} ${what}`;
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

// ── data needed ────────────────────────────────────────────────────────────

/** The suffix minimum of `breakpoints[k] / factors[k]^2` (`solve_scale_needed`),
 *  once per family. */
const suffixMins = new WeakMap<FamilyInfo, number[]>();
function suffixMin(family: FamilyInfo): number[] {
  let out = suffixMins.get(family);
  if (!out) {
    const { breakpoints: b, factors: f } = family;
    out = new Array(b.length);
    let low = Infinity;
    for (let k = b.length - 1; k >= 0; k--) {
      const g = b[k] / (f[k] * f[k]);
      if (g < low) low = g;
      out[k] = low;
    }
    suffixMins.set(family, out);
  }
  return out;
}

/** How many of the sorted `values` are below `x` (`side="left"`) or at most `x`
 *  (`side="right"`): numpy's `searchsorted`. */
function searchSorted(values: number[], x: number, side: "left" | "right"): number {
  let lo = 0, hi = values.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (side === "left" ? values[mid] < x : values[mid] <= x) lo = mid + 1; else hi = mid;
  }
  return lo;
}

/** The multiple of the current paths a row needs for its MDE to reach `target`,
 *  with the family projected at that size: the smallest multiple, never below
 *  `scaleToTest`, from which `factor(m(s)) * base / sqrt(s) <= target`. The
 *  same arithmetic as `solve_scale_needed` in `tools/ab_power.py` (the power
 *  getters' `scale_needed`); a total, not an additional multiple. Null without a
 *  target, a base spread, a scale to test or the family. */
export function scaleNeeded(
  base: number | null | undefined, scaleToTest: number | null | undefined,
  target: number | null | undefined, family: FamilyInfo | null | undefined,
): number | null {
  if (!isNum(target) || !(target > 0) || !isNum(base) || !(base > 0)) return null;
  if (!isNum(scaleToTest) || scaleToTest < 0 || !family || !family.breakpoints.length) return null;
  const b = family.breakpoints, f = family.factors;
  const ratio = base / target;
  const x = ratio * ratio;
  // the last interval whose lower end the target size exceeds (b[0] = 0 < x)
  const last = searchSorted(suffixMin(family), x, "left") - 1;
  const own = searchSorted(b, scaleToTest, "right") - 1;
  const ownMisses = x * (f[own] * f[own]) > scaleToTest;
  const worst = last > own ? last : ownMisses ? own : -1;
  if (worst < 0) return scaleToTest;
  const upper = worst + 1 < b.length ? b[worst + 1] : Infinity;
  return Math.min(x * (f[worst] * f[worst]), upper);
}

/** What the power section says about one row of the frame. */
export interface PowerCell {
  scaleToTest: number | null;
  base: number | null;
  inference: Inference | null;
}

export function powerCell(power: PowerInfo | null | undefined, idx: number | null | undefined): PowerCell {
  if (!power || idx == null) return { scaleToTest: null, base: null, inference: null };
  const s = power.scale_to_test?.[idx], b = power.base?.[idx];
  return { scaleToTest: isNum(s) ? s : null, base: isNum(b) ? b : null, inference: power.inference?.[idx] ?? null };
}

/** What a cell shows in the "data needed" view. */
export interface NeedInfo {
  /** Testable now (it has an adjusted p-value). */
  testable: boolean;
  /** The MDE now, in the frame's units (null when not tested). */
  mde: number | null;
  /** The multiple of paths: to reach the target (tested) or to pass the support
   *  rule (not tested); null when unknown or unreachable. */
  times: number | null;
  /** What `times` is for: the target, the support rule of a row that is then
   *  testable ("test"), or of a row whose spread is unknown ("support"). */
  timesFor: "target" | "test" | "support" | null;
  /** With a target, the multiple of paths it needs (null without a spread). */
  forTarget: number | null;
  inference: Inference | null;
}

export function needOf(
  row: MatrixRow | undefined | null,
  cell: PowerCell,
  alpha: number,
  target: number | null | undefined,
  family: FamilyInfo | null | undefined,
): NeedInfo {
  const status = cellStatus(row, alpha);
  const none = { testable: false, mde: null, times: null, timesFor: null, forTarget: null, inference: cell.inference };
  if (status === "empty") return none;
  const forTarget = scaleNeeded(cell.base, cell.scaleToTest, target, family);
  if (status === "untested") {
    const known = cell.inference !== "unknown_spread";
    return {
      ...none, forTarget,
      times: cell.scaleToTest,
      timesFor: cell.scaleToTest == null ? null : known ? "test" : "support",
    };
  }
  const mde = isNum(row?.mde) ? (row?.mde as number) : null;
  return { ...none, testable: true, mde, times: forTarget, timesFor: forTarget != null ? "target" : null, forTarget };
}

/** The largest multiple the background distinguishes (×64 and above are full). */
export const NEED_MAX_TIMES = 64;

/** Opacity of a "data needed" background: with a target, log2 of the multiple
 *  of paths it needs (none at ×1 or less, full at ×64); without one, the MDE
 *  over the scale maximum. A cell that is not testable has no fill (it is
 *  hatched instead). */
export function needAlpha(need: NeedInfo, mdeScale: number, withTarget: boolean): number {
  if (!need.testable) return 0;
  if (withTarget) {
    if (!isNum(need.times) || need.times <= 1) return 0;
    return MAX_ALPHA * Math.min(1, Math.log2(need.times) / Math.log2(NEED_MAX_TIMES));
  }
  if (!isNum(need.mde) || !(mdeScale > 0)) return 0;
  return MAX_ALPHA * Math.min(1, need.mde / mdeScale);
}

export function needBackground(need: NeedInfo, mdeScale: number, withTarget: boolean): string {
  const a = needAlpha(need, mdeScale, withTarget);
  return a === 0 ? "transparent" : `rgba(${NEED_HUE}, ${a.toFixed(3)})`;
}

/** Diagonal hatching for a cell that is not testable at the current size. */
export function hatchImage(color: string = HATCH_COLOR): string {
  return `repeating-linear-gradient(135deg, ${color} 0 1.5px, transparent 1.5px 6px)`;
}

/** The largest MDE among the given rows that are tested (the "data needed"
 *  view's scale maximum). */
export function mdeScaleMax(rows: (MatrixRow | undefined)[], alpha: number): number {
  let top = 0;
  for (const row of rows) {
    if (cellStatus(row, alpha) === "untested" || !row) continue;
    if (isNum(row.mde) && row.mde > top) top = row.mde;
  }
  return top;
}

/** `×2.4`, `×16`, `×1.0`; `≥ ×1000` above a thousand. */
export function fmtTimes(times: number | null | undefined): string {
  if (!isNum(times)) return "n/a";
  if (times >= 1000) return "≥ ×1000";
  if (times >= 10) return `×${Math.ceil(times)}`;
  return `×${(Math.ceil(times * 10) / 10).toFixed(1)}`;
}

/** The cell's two lines in the "data needed" view: the MDE in the frame's shown
 *  units (a tested cell; "–" otherwise) and the multiple of paths (to reach the
 *  target, or to pass the support rule: "to test", or "to test?" when whether it
 *  then has a spread is unknown; "never" when more paths of the same
 *  composition would not make it testable). */
export function needText(need: NeedInfo, unit: UnitInfo): { main: string; times: string | null } {
  if (!need.testable) {
    if (need.timesFor === "test") return { main: "–", times: `${fmtTimes(need.times)} to test` };
    if (need.timesFor === "support") return { main: "–", times: `${fmtTimes(need.times)} to test?` };
    return { main: "–", times: "never" };
  }
  const main = need.mde != null ? (need.mde * unit.scale).toFixed(decimalsFor(unit, need.mde)) : "n/a";
  if (need.timesFor === "target") {
    return { main, times: need.times != null && need.times <= 1 ? "enough" : `${fmtTimes(need.times)} paths` };
  }
  return { main, times: null };
}

/** What one path is, for the "data needed" texts: "a path is one user_id". */
export function pathUnitText(payload: Pick<MatrixPayload, "path_col" | "power">): string {
  const col = payload.power?.unit_of_paths ?? payload.path_col;
  return col ? `a path is one ${col}` : "a path is one unit of the schema's path_col";
}

/** Why a cell that is not testable now never becomes testable, or may not. */
export function inferenceText(inference: Inference | null | undefined): string | null {
  if (inference === "no_spread") return "never: the value is constant per path, and more paths of the same mix keep it constant";
  if (inference === "no_estimate") return "never: no estimate to test, and more paths of the same mix do not give one";
  if (inference === "unknown_spread") return "unknown: no spread estimated now, so more paths may or may not make it testable";
  return null;
}

/** The power curve as SVG geometry: the testable cells (left axis) and the median
 *  MDE (right axis) against the paths (log2 multiples, x), with the current size
 *  (×1) marked. */
export interface CurveGeometry {
  testablePath: string;
  mdePath: string;
  points: { x: number; yTestable: number; yMde: number | null; scale: number }[];
  currentX: number | null;
  targetY: number | null;
  maxTestable: number;
  maxMde: number;
}

export function curveGeometry(
  curve: CurvePoint[],
  width: number,
  height: number,
  pad = 4,
  target: number | null = null,
): CurveGeometry {
  const pts = curve.filter((p) => isNum(p.scale) && p.scale > 0);
  const empty = { testablePath: "", mdePath: "", points: [], currentX: null, targetY: null, maxTestable: 0, maxMde: 0 };
  if (!pts.length) return empty;
  const lx = pts.map((p) => Math.log2(p.scale));
  const x0 = Math.min(...lx), x1 = Math.max(...lx);
  const xOf = (v: number) => (x1 === x0 ? width / 2 : pad + ((v - x0) / (x1 - x0)) * (width - 2 * pad));
  const maxTestable = Math.max(1, ...pts.map((p) => p.cells || 0), ...pts.map((p) => p.testable || 0));
  const mdes = pts.map((p) => p.mde_median).filter(isNum) as number[];
  const maxMde = Math.max(0, ...mdes, isNum(target) ? target : 0);
  const yT = (v: number) => height - pad - (v / maxTestable) * (height - 2 * pad);
  const yM = (v: number) => (maxMde > 0 ? height - pad - (v / maxMde) * (height - 2 * pad) : height - pad);
  const points = pts.map((p, i) => ({
    x: xOf(lx[i]), yTestable: yT(p.testable || 0), yMde: isNum(p.mde_median) ? yM(p.mde_median) : null, scale: p.scale,
  }));
  const line = (coords: [number, number][]) =>
    coords.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const current = pts.findIndex((p) => p.scale === 1);
  return {
    testablePath: line(points.map((p) => [p.x, p.yTestable])),
    mdePath: line(points.filter((p) => p.yMde != null).map((p) => [p.x, p.yMde as number])),
    points,
    currentX: current >= 0 ? points[current].x : null,
    targetY: isNum(target) && target > 0 && maxMde > 0 ? yM(target) : null,
    maxTestable,
    maxMde,
  };
}
