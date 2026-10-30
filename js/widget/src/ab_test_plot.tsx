/**
 * A/B test widget — presentational pieces: number formatting, the verdict
 * colour rules, the forest plot, the scorecard / strata / period views.
 *
 * Every number shown is a cell of `ab_test_data` (= `get_ab_test`) sent by
 * Python as it is; `null` (NaN on the Python side) is "no estimate", never 0.
 */
import * as React from "react";
import { formatPValue } from "./widget-utils";

// ── types ──────────────────────────────────────────────────────────────────

export interface ABRow {
  metric: string;
  stratum?: string;
  period?: string;
  period_start?: string | null;
  period_complete?: boolean;
  group1_paths?: number;
  group2_paths?: number;
  group1_value?: number | null;
  group1_ci_low?: number | null;
  group1_ci_high?: number | null;
  group2_value?: number | null;
  group2_ci_low?: number | null;
  group2_ci_high?: number | null;
  delta?: number | null;
  delta_ci_low?: number | null;
  delta_ci_high?: number | null;
  rel_delta?: number | null;
  rel_delta_ci_low?: number | null;
  rel_delta_ci_high?: number | null;
  p_value?: number | null;
  p_value_adjusted?: number | null;
  mde_alpha?: number | null;
  correction_family?: string;
  mde?: number | null;
  rel_mde?: number | null;
  mde_basis?: string;
  ci_basis?: string;
  p_basis?: string;
  delta_ci_low_adj?: number | null;
  delta_ci_high_adj?: number | null;
  delta_ci_adjusted_basis?: string;
  delta_ci_adjusted_status?: string;
  strata_diagnostics_basis?: string;
  verdict?: string;
  test?: string;
  support_warning?: string;
  reading?: string | null;
  group1_share?: number | null;
  group2_share?: number | null;
  balance_p_value?: number | null;
  heterogeneity_p_value?: number | null;
  heterogeneity_dof?: number | null;
  heterogeneity_strata_used?: number | null;
  stratum_vs_rest_p_value?: number | null;
  exploratory?: boolean;
  strata_pre_exposure_source?: string;
  [key: string]: unknown;
}

export interface ABResult {
  overall?: ABRow[] | null;
  strata?: ABRow[] | null;
  periods?: ABRow[] | null;
  metric_kinds?: Record<string, string>;
  metric_units?: Record<string, string>;
  group1_label?: string;
  group2_label?: string;
  ci_level?: number;
  correction?: string;
  correction_scope?: string;
  strata_col?: string | null;
  period?: string | null;
  period_basis?: string | null;
  warnings?: string[];
}

/** How a metric's numbers are written: its kind (`binary` = 0/1 per path) and unit. */
export interface MetricFormat {
  kind: string;
  unit: string;
}

export function metricFormat(result: ABResult, metric: string): MetricFormat {
  return {
    kind: result.metric_kinds?.[metric] ?? "value",
    unit: result.metric_units?.[metric] ?? "",
  };
}

// ── colour rules ───────────────────────────────────────────────────────────

/** The library's diff colours: group 1 higher is red, group 2 higher is blue. */
export const VERDICT_COLORS: Record<string, string> = {
  increase: "#ef4444",
  decrease: "#3b82f6",
  inconclusive: "#9ca3af",
  insufficient_data: "#9ca3af",
  descriptive: "#9ca3af",
};
const GREY = "#9ca3af";

export function verdictColor(verdict: string | undefined): string {
  return (verdict && VERDICT_COLORS[verdict]) || GREY;
}

/** A point is hollow when it carries no test (`insufficient_data`) or its
 *  period is not complete; descriptive rows (survivors, not randomised groups)
 *  are squares, every other row a circle. */
export function markerStyle(row: Pick<ABRow, "verdict" | "period_complete">): {
  fill: string; stroke: string; hollow: boolean; shape: "circle" | "square";
} {
  const color = verdictColor(row.verdict);
  const hollow = row.verdict === "insufficient_data" || row.period_complete === false;
  return {
    fill: hollow ? "#ffffff" : color,
    stroke: color,
    hollow,
    shape: row.verdict === "descriptive" ? "square" : "circle",
  };
}

export const VERDICT_LABELS: Record<string, string> = {
  increase: "increase",
  decrease: "decrease",
  inconclusive: "inconclusive",
  insufficient_data: "insufficient data",
  descriptive: "descriptive",
};

export const SUPPORT_NOTES: Record<string, string> = {
  no_data: "No path of either arm has the behaviour, or an arm has no estimate (no path, or no value): nothing to compare, no test.",
  one_arm_only: "The behaviour was observed in one arm only. Read the selected method and its support status before interpreting the contrast.",
  invalid_replicates: "At least one bootstrap replicate had no value in an arm: no interval or p-value, the estimate is descriptive.",
  low_support: "Few supporting paths behind a bootstrap interval: percentile intervals under-cover here.",
  insufficient_expected_cells: "The approximate z-test requires at least five expected successes and failures in each arm. No inferential p-value is available.",
};

// ── number formatting ──────────────────────────────────────────────────────

const isNum = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);

/** A plain number at a precision that suits its size. */
export function fmtNum(v: number): string {
  const a = Math.abs(v);
  if (a === 0) return "0";
  if (a >= 1000) return v.toLocaleString("en-US", { maximumFractionDigits: 0 });
  if (a >= 100) return v.toFixed(1);
  if (a >= 1) return v.toFixed(2);
  return v.toPrecision(3);
}

/** Seconds are shown in the largest unit that keeps the numbers readable. */
export function durationUnit(maxAbsSeconds: number): [number, string] {
  if (maxAbsSeconds < 120) return [1, "s"];
  if (maxAbsSeconds < 7200) return [60, "min"];
  if (maxAbsSeconds < 172800) return [3600, "h"];
  return [86400, "d"];
}

function signed(text: string, v: number): string {
  return v > 0 ? `+${text}` : v < 0 ? `-${text.replace(/^-/, "")}` : text;
}

/** An arm's value: 0/1 metrics as a percentage, seconds as a duration, else the number. */
export function fmtValue(v: number | null | undefined, f: MetricFormat): string {
  if (!isNum(v)) return "n/a";
  if (f.kind === "binary") return `${(v * 100).toFixed(1)}%`;
  if (f.unit === "seconds") {
    const [div, suffix] = durationUnit(Math.abs(v));
    return `${fmtNum(v / div)} ${suffix}`;
  }
  return fmtNum(v);
}

/** Signed magnitude of a delta (no unit suffix); `div` rescales seconds. */
function deltaNumber(v: number, f: MetricFormat, div: number): string {
  if (f.kind === "binary") {
    const t = (v * 100).toFixed(2);
    return t === "0.00" || t === "-0.00" ? "0.00" : signed(Math.abs(v * 100).toFixed(2), v);
  }
  const x = v / div;
  return x === 0 ? "0" : signed(fmtNum(Math.abs(x)), x);
}

/** The unit a delta is written in: `pp` for 0/1 metrics, a time unit for
 *  seconds (picked from the largest of the given values), otherwise none. */
export function deltaUnit(f: MetricFormat, ...values: (number | null | undefined)[]): [number, string] {
  if (f.kind === "binary") return [1, "pp"];
  if (f.unit === "seconds") {
    const finite = values.filter(isNum).map(Math.abs);
    return durationUnit(finite.length ? Math.max(...finite) : 0);
  }
  return [1, ""];
}

/** A delta with its unit: `+0.62 pp`, `-1.3 min`, `+1.54`. `refs` (its
 *  interval ends) pick the time unit, so a delta and its interval share one. */
export function fmtDelta(v: number | null | undefined, f: MetricFormat, ...refs: (number | null | undefined)[]): string {
  if (!isNum(v)) return "n/a";
  const [div, unit] = deltaUnit(f, v, ...refs);
  const text = deltaNumber(v, f, div);
  return unit ? `${text} ${unit}` : text;
}

/** An interval in one unit: `[-1.10, +2.33] pp`. A missing end reads n/a. */
export function fmtInterval(lo: number | null | undefined, hi: number | null | undefined, f: MetricFormat, ...refs: (number | null | undefined)[]): string {
  const [div, unit] = deltaUnit(f, lo, hi, ...refs);
  const end = (v: number | null | undefined) => (isNum(v) ? deltaNumber(v, f, div) : "n/a");
  return `[${end(lo)}, ${end(hi)}]${unit ? ` ${unit}` : ""}`;
}

/** An unsigned effect size such as the MDE, in the delta's unit. */
export function fmtMagnitude(v: number | null | undefined, f: MetricFormat): string {
  if (!isNum(v)) return "n/a";
  return fmtDelta(Math.abs(v), f).replace(/^\+/, "");
}

/** A relative delta (`group1 / group2 - 1`) as a signed percentage. */
export function fmtRel(v: number | null | undefined): string {
  if (!isNum(v)) return "n/a";
  return signed(Math.abs(v * 100).toFixed(1), v) + "%";
}

/** A p-value without the `p=` prefix of `formatPValue`, for table cells. */
export function fmtP(p: number | null | undefined): string {
  if (!isNum(p)) return "n/a";
  return p < 0.001 ? "<0.001" : p.toFixed(3);
}

export function fmtShare(v: number | null | undefined): string {
  return isNum(v) ? `${(v * 100).toFixed(1)}%` : "n/a";
}

/** The hover text of a plotted delta: the absolute delta and interval, then the
 *  relative delta with its interval. */
export function pointTitle(label: string, row: ABRow, f: MetricFormat): string {
  return (
    `${label}: Δ ${fmtDelta(row.delta, f, row.delta_ci_low, row.delta_ci_high)} ${fmtInterval(row.delta_ci_low, row.delta_ci_high, f, row.delta)}` +
    `\nrelative ${fmtRel(row.rel_delta)} [${fmtRel(row.rel_delta_ci_low)}, ${fmtRel(row.rel_delta_ci_high)}]` +
    `\n${VERDICT_LABELS[row.verdict ?? ""] ?? row.verdict ?? ""}` +
    (row.period_complete === false ? " · period not complete" : "")
  );
}

// ── small pieces ───────────────────────────────────────────────────────────

const th: React.CSSProperties = { padding: "6px 8px", fontSize: 11, fontWeight: 600, color: "#6b7280", textAlign: "right", borderBottom: "1px solid #e5e7eb", whiteSpace: "nowrap" };
const td: React.CSSProperties = { padding: "5px 8px", fontSize: 12, textAlign: "right", color: "#111827", borderBottom: "1px solid #f3f4f6", whiteSpace: "nowrap", verticalAlign: "top" };
const tdl: React.CSSProperties = { ...td, textAlign: "left", fontWeight: 500 };
const sub: React.CSSProperties = { fontSize: 10, color: "#6b7280", fontWeight: 400 };

export function methodLabel(value: string | undefined): string {
  const labels: Record<string, string> = {
    fisher_exact: "Fisher exact", descriptive: "Descriptive", z_test: "Approximate z-test",
    welch_t_test: "Approximate Welch test", bootstrap: "Approximate bootstrap",
    clopper_pearson_bonferroni: "Conservative exact Clopper–Pearson difference",
    fisher_exact_two_sided: "Fisher exact, two-sided", newcombe_approximate: "Newcombe (approximate)",
    pooled_z_asymptotic: "Pooled z (asymptotic)", welch_approximate: "Welch (approximate)",
    welch_asymptotic: "Welch (asymptotic)", path_bootstrap_approximate: "Percentile bootstrap (approximate)",
    path_bootstrap_percentile_approximate: "Percentile bootstrap (approximate)",
    bootstrap_se_asymptotic: "Bootstrap SE (asymptotic)", not_requested: "Not requested",
    normal_approximation: "Normal planning approximation", unavailable: "Unavailable",
    insufficient_tail_resamples: "Insufficient resamples in the adjusted tails",
    asymptotic_exploratory: "Asymptotic exploratory diagnostics (unadjusted)",
  };
  return value ? (labels[value] ?? value.replace(/_/g, " ")) : "Unavailable";
}

function MethodDetails({ row }: { row: ABRow }) {
  return <div style={sub} data-testid="method-details">
    {methodLabel(row.test)}
    {row.ci_basis && <div>CI: {methodLabel(row.ci_basis)}</div>}
    {row.p_basis && <div>p: {methodLabel(row.p_basis)}</div>}
  </div>;
}

function AdjustedInterval({ row, format }: { row: ABRow; format: MetricFormat }) {
  if (!row.delta_ci_adjusted_status) return null;
  return <div style={sub} data-testid="adjusted-interval">
    Bonferroni CI: {row.delta_ci_adjusted_status === "ok"
      ? fmtInterval(row.delta_ci_low_adj, row.delta_ci_high_adj, format, row.delta)
      : `unavailable — ${methodLabel(row.delta_ci_adjusted_status)}`}
    <div>{methodLabel(row.delta_ci_adjusted_basis)}</div>
  </div>;
}

export function VerdictBadge({ verdict }: { verdict?: string }) {
  const color = verdictColor(verdict);
  const hollow = verdict === "insufficient_data";
  const tinted = verdict === "increase" || verdict === "decrease";
  return (
    <span data-testid="verdict-badge" data-verdict={verdict ?? ""} style={{
      display: "inline-block", padding: "1px 7px", borderRadius: 10, fontSize: 10, fontWeight: 600,
      border: `1px solid ${color}`, color: tinted ? "#ffffff" : "#374151",
      background: tinted ? color : hollow ? "#ffffff" : "#f3f4f6",
    }}>{VERDICT_LABELS[verdict ?? ""] ?? verdict ?? ""}</span>
  );
}

function SupportBadge({ warning }: { warning?: string }) {
  if (!warning) return null;
  return (
    <span data-testid="support-warning" title={SUPPORT_NOTES[warning] ?? warning} style={{
      display: "inline-block", padding: "1px 6px", borderRadius: 4, fontSize: 10,
      background: "#fef3c7", color: "#92400e", border: "1px solid #fcd34d", cursor: "help",
    }}>{warning.replace(/_/g, " ")}</span>
  );
}

/** The `reading` sentence: clipped to one line, the whole text on hover, and
 *  expanded in place on click (a DOM toggle, so the cell stays stateless). */
function ReadingCell({ text }: { text?: string | null }) {
  if (!text) return <td style={{ ...tdl, color: "#9ca3af" }} />;
  const toggle = (e: React.MouseEvent<HTMLTableCellElement>) => {
    const cell = e.currentTarget;
    const open = cell.dataset.open !== "true";
    cell.dataset.open = String(open);
    cell.style.whiteSpace = open ? "normal" : "nowrap";
    cell.style.maxWidth = open ? "420px" : "240px";
  };
  return (
    <td data-testid="reading-cell" data-open="false" title={text} onClick={toggle} style={{
      ...tdl, fontWeight: 400, fontSize: 11, color: "#374151", cursor: "pointer",
      maxWidth: 240, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
    }}>{text}</td>
  );
}

const deltaBackground = (verdict?: string) =>
  verdict === "increase" ? "rgba(239,68,68,0.14)" : verdict === "decrease" ? "rgba(59,130,246,0.14)" : undefined;

function Marker({ x, y, row, r = 5 }: { x: number; y: number; row: ABRow; r?: number }) {
  const m = markerStyle(row);
  return m.shape === "square" ? (
    <rect x={x - r} y={y - r} width={2 * r} height={2 * r} fill={m.fill} stroke={m.stroke} strokeWidth={1.6} />
  ) : (
    <circle cx={x} cy={y} r={r} fill={m.fill} stroke={m.stroke} strokeWidth={1.6} />
  );
}

// ── forest plot ────────────────────────────────────────────────────────────

const FP_LABEL_W = 170;
const FP_PLOT_W = 300;
const FP_TEXT_W = 200;
const FP_ROW_H = 26;
const FP_TOP = 8;

/** One row per entry: a dot at the delta, a whisker over its interval, a zero
 *  line. Rows are in different units, so each row has its own scale,
 *  symmetric around 0: the plot reads where an interval lies relative to 0. */
export function ForestPlot({ rows, labelOf, formatOf, testId = "forest-plot" }: {
  rows: ABRow[];
  labelOf: (row: ABRow) => string;
  formatOf: (row: ABRow) => MetricFormat;
  testId?: string;
}) {
  const width = FP_LABEL_W + FP_PLOT_W + FP_TEXT_W;
  const height = FP_TOP * 2 + rows.length * FP_ROW_H;
  const cx = FP_LABEL_W + FP_PLOT_W / 2;
  const halfW = FP_PLOT_W / 2 - 10;
  return (
    <svg data-testid={testId} viewBox={`0 0 ${width} ${height}`} width="100%"
      style={{ display: "block", maxWidth: width, fontFamily: "system-ui, sans-serif" }}>
      <line x1={cx} x2={cx} y1={FP_TOP - 4} y2={height - FP_TOP + 4} stroke="#6b7280" strokeWidth={1} />
      {rows.map((row, i) => {
        const y = FP_TOP + i * FP_ROW_H + FP_ROW_H / 2;
        const f = formatOf(row);
        const ends = [row.delta, row.delta_ci_low, row.delta_ci_high].filter(isNum).map(Math.abs);
        const half = ends.length && Math.max(...ends) > 0 ? Math.max(...ends) * 1.1 : 1;
        const x = (v: number) => cx + (v / half) * halfW;
        const color = verdictColor(row.verdict);
        const label = labelOf(row);
        const hasCi = isNum(row.delta_ci_low) && isNum(row.delta_ci_high);
        const m = markerStyle(row);
        return (
          <g key={`${label}-${i}`} data-testid="forest-row">
            <title>{pointTitle(label, row, f)}</title>
            {i % 2 === 1 && <rect x={0} y={y - FP_ROW_H / 2} width={width} height={FP_ROW_H} fill="#f9fafb" />}
            <text x={FP_LABEL_W - 8} y={y + 4} textAnchor="end" fontSize={11}
              fill={row.exploratory ? "#6b7280" : "#111827"} fontStyle={row.exploratory ? "italic" : undefined}>
              {label.length > 24 ? label.slice(0, 23) + "…" : label}{row.exploratory ? " *" : ""}
            </text>
            <line x1={cx} x2={cx} y1={y - FP_ROW_H / 2} y2={y + FP_ROW_H / 2} stroke="#6b7280" strokeWidth={1} />
            {hasCi && (
              <g data-testid="forest-whisker" stroke={color} strokeWidth={1.6}
                strokeDasharray={row.verdict === "descriptive" ? "3 2" : undefined}>
                <line x1={x(row.delta_ci_low as number)} x2={x(row.delta_ci_high as number)} y1={y} y2={y} />
                <line x1={x(row.delta_ci_low as number)} x2={x(row.delta_ci_low as number)} y1={y - 4} y2={y + 4} />
                <line x1={x(row.delta_ci_high as number)} x2={x(row.delta_ci_high as number)} y1={y - 4} y2={y + 4} />
              </g>
            )}
            {isNum(row.delta) && (
              <g data-testid="forest-point" data-verdict={row.verdict ?? ""} data-hollow={String(m.hollow)}>
                <Marker x={x(row.delta)} y={y} row={row} />
              </g>
            )}
            <text x={FP_LABEL_W + FP_PLOT_W + 10} y={y + 4} fontSize={11} fill="#111827">
              {fmtDelta(row.delta, f, row.delta_ci_low, row.delta_ci_high)}
              <tspan fontSize={10} fill="#6b7280"> {fmtInterval(row.delta_ci_low, row.delta_ci_high, f, row.delta).replace(/ (pp|s|min|h|d)$/, "")}</tspan>
            </text>
          </g>
        );
      })}
    </svg>
  );
}

function PlotNote({ children }: { children: React.ReactNode }) {
  return <div style={{ fontSize: 10, color: "#6b7280", padding: "4px 8px 0", lineHeight: 1.5 }}>{children}</div>;
}

function Legend() {
  const item = (row: ABRow, text: string) => (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 4, marginRight: 12 }}>
      <svg width={12} height={12}><Marker x={6} y={6} row={row} r={4} /></svg>{text}
    </span>
  );
  return (
    <div data-testid="ab-legend" style={{ fontSize: 10, color: "#374151", padding: "4px 8px", display: "flex", flexWrap: "wrap" }}>
      {item({ metric: "", verdict: "increase" }, "increase")}
      {item({ metric: "", verdict: "decrease" }, "decrease")}
      {item({ metric: "", verdict: "inconclusive" }, "inconclusive")}
      {item({ metric: "", verdict: "insufficient_data" }, "insufficient data (no test)")}
    </div>
  );
}

// ── scorecard ──────────────────────────────────────────────────────────────

export function ScorecardTable({ rows, result }: { rows: ABRow[]; result: ABResult }) {
  const g1 = result.group1_label || "Group 1";
  const g2 = result.group2_label || "Group 2";
  // `reading` arrives with the library version that computes it; without it
  // the column is left out.
  const hasReading = rows.some(r => typeof r.reading === "string");
  const hasWarning = rows.some(r => !!r.support_warning);
  return (
    <div style={{ overflowX: "auto" }}>
      <table data-testid="scorecard" style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th style={{ ...th, textAlign: "left" }}>Metric</th>
            <th style={th}>{g1}</th>
            <th style={th}>{g2}</th>
            <th style={th}>Δ ({g1} − {g2})</th>
            <th style={th}>Relative</th>
            <th style={th} title="p-value after the correction within the metric's family">p adj</th>
            <th style={th} title="Normal planning approximation at mde_alpha and the requested power; not exact power for the selected test or a bound on the effect">MDE (approx.)</th>
            <th style={{ ...th, textAlign: "left" }}>Verdict</th>
            {hasWarning && <th style={{ ...th, textAlign: "left" }}>Support</th>}
            {hasReading && <th style={{ ...th, textAlign: "left" }}>Reading</th>}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const f = metricFormat(result, row.metric);
            return (
              <tr key={`${row.metric}-${i}`} data-testid="scorecard-row" data-event={row.metric}
                style={{ background: i % 2 === 0 ? "#fff" : "#fafafa" }}>
                <td style={tdl}>{row.metric}<MethodDetails row={row} /></td>
                <td style={td}>{fmtValue(row.group1_value, f)}
                  <div style={sub}>n={(row.group1_paths ?? 0).toLocaleString("en-US")}</div></td>
                <td style={td}>{fmtValue(row.group2_value, f)}
                  <div style={sub}>n={(row.group2_paths ?? 0).toLocaleString("en-US")}</div></td>
                <td data-testid="delta-cell" style={{ ...td, fontWeight: 600, background: deltaBackground(row.verdict) }}>
                  {fmtDelta(row.delta, f, row.delta_ci_low, row.delta_ci_high)}
                  <div data-testid="delta-interval" style={sub}>{fmtInterval(row.delta_ci_low, row.delta_ci_high, f, row.delta)}</div>
                  <AdjustedInterval row={row} format={f} />
                </td>
                <td style={td}>{fmtRel(row.rel_delta)}
                  <div style={sub}>{row.test === "fisher_exact"
                    ? "Relative CI unavailable for Fisher exact"
                    : `[${fmtRel(row.rel_delta_ci_low)}, ${fmtRel(row.rel_delta_ci_high)}]`}</div></td>
                <td style={td} title={`raw ${formatPValue(row.p_value)}; ${row.correction_family ?? ""}; mde_alpha ${isNum(row.mde_alpha) ? +row.mde_alpha.toFixed(4) : "n/a"}`}>
                  {fmtP(row.p_value_adjusted)}
                </td>
                <td style={td}>{fmtMagnitude(row.mde, f)}{row.mde_basis && <div style={sub}>{methodLabel(row.mde_basis)}</div>}</td>
                <td style={{ ...tdl, fontWeight: 400 }}><VerdictBadge verdict={row.verdict} /></td>
                {hasWarning && <td style={{ ...tdl, fontWeight: 400 }}><SupportBadge warning={row.support_warning} /></td>}
                {hasReading && <ReadingCell text={row.reading as string | null | undefined} />}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** Which rows one correction family holds, in words (`correction_scope`). */
export function familyText(scope: string | undefined): string {
  return scope === "all" ? "across every row of every metric" : "within each metric's rows";
}

export function intervalNote(result: ABResult) {
  const level = result.ci_level ?? 0.95;
  const correction = result.correction ?? "holm";
  return (
    <>
      {`${+(level * 100).toFixed(1)}%`} primary intervals are per comparison. Any Bonferroni interval is separately labelled with its availability. The verdict reads the
      p-value adjusted with {correction === "none" ? "no correction" : correction} {familyText(result.correction_scope)},
      against {+(1 - level).toFixed(4)}. Tests and intervals use different constructions; their decisions need not agree. Read each row's exact or approximate method label.
      Inconclusive is not "no effect": the interval shows which effects remain possible.
    </>
  );
}

export function ScorecardView({ result }: { result: ABResult }) {
  const rows = result.overall ?? [];
  return (
    <div data-testid="ab-scorecard-view">
      <ScorecardTable rows={rows} result={result} />
      <div style={{ marginTop: 14, fontSize: 11, fontWeight: 600, color: "#374151", padding: "0 8px" }}>
        Δ with its interval
      </div>
      <ForestPlot rows={rows} labelOf={r => r.metric} formatOf={r => metricFormat(result, r.metric)} />
      <Legend />
      <PlotNote>Each row on its own scale, symmetric around 0. Hover a row for the relative delta. {intervalNote(result)}</PlotNote>
    </div>
  );
}

// ── strata ─────────────────────────────────────────────────────────────────

function groupByMetric(rows: ABRow[]): [string, ABRow[]][] {
  const out = new Map<string, ABRow[]>();
  for (const row of rows) {
    if (!out.has(row.metric)) out.set(row.metric, []);
    out.get(row.metric)!.push(row);
  }
  return Array.from(out.entries());
}

export function StrataView({ result }: { result: ABResult }) {
  const rows = result.strata ?? [];
  const exploratory = rows.some(r => r.exploratory);
  const g1 = result.group1_label || "Group 1";
  const g2 = result.group2_label || "Group 2";
  return (
    <div data-testid="ab-strata-view">
      {exploratory && (
        <div data-testid="strata-exploratory" style={{ margin: "0 0 10px", padding: "8px 10px", background: "#fffbeb", border: "1px solid #fcd34d", borderRadius: 6, fontSize: 11, color: "#92400e", lineHeight: 1.5 }}>
          * Exploratory: the strata of <code>{result.strata_col}</code> are not known to be fixed before exposure
          ({rows[0]?.strata_pre_exposure_source ?? "not_pre_exposure"}). A stratum the treatment can change compares
          selected groups, not randomised ones. Declare <code>strata_pre_exposure=True</code> only if it was fixed
          before exposure (a platform at signup).
        </div>
      )}
      {groupByMetric(rows).map(([metric, mrows]) => {
        const f = metricFormat(result, metric);
        const first = mrows[0];
        return (
          <div key={metric} data-testid="strata-metric" style={{ marginBottom: 18 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "baseline", padding: "0 8px 4px" }}>
              <span style={{ fontSize: 13, fontWeight: 600, color: "#111827" }}>{metric}</span>
              {first?.strata_diagnostics_basis && <span style={sub} data-testid="strata-diagnostics-basis">Secondary diagnostics: {methodLabel(first.strata_diagnostics_basis)}</span>}
              <span data-testid="strata-heterogeneity" style={{ fontSize: 11, color: "#374151" }}
                title="Optional asymptotic exploratory Cochran's Q over supported strata; unavailable by default and not part of the primary correction family.">
                heterogeneity p {fmtP(first?.heterogeneity_p_value)}
                {isNum(first?.heterogeneity_strata_used) ? ` (${first?.heterogeneity_strata_used} strata used)` : ""}
              </span>
              <span data-testid="strata-balance" style={{ fontSize: 11, color: "#374151" }}
                title="Optional asymptotic Pearson diagnostic with adequate expected cells; unavailable by default. A balanced post-exposure stratum is still selected by treatment.">
                balance p {fmtP(first?.balance_p_value)}
              </span>
            </div>
            <ForestPlot rows={mrows} testId="strata-forest" labelOf={r => String(r.stratum ?? "")} formatOf={() => f} />
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", marginTop: 4 }}>
                <thead>
                  <tr>
                    <th style={{ ...th, textAlign: "left" }}>Stratum</th>
                    <th style={th}>Share {g1}</th>
                    <th style={th}>Share {g2}</th>
                    <th style={th}>Δ</th>
                    <th style={th}>p adj</th>
                    <th style={th} title="Stratum's delta against the pooled other strata (unadjusted)">vs rest p</th>
                    <th style={{ ...th, textAlign: "left" }}>Verdict</th>
                  </tr>
                </thead>
                <tbody>
                  {mrows.map((row, i) => (
                    <tr key={`${row.stratum}-${i}`} data-testid="strata-row" data-exploratory={String(!!row.exploratory)}>
                      <td style={{ ...tdl, fontStyle: row.exploratory ? "italic" : undefined }}>
                        {row.stratum}{row.exploratory ? " *" : ""}
                        <div style={sub}>n={row.group1_paths ?? 0}/{row.group2_paths ?? 0}</div>
                        <MethodDetails row={row} />
                      </td>
                      <td style={td}>{fmtShare(row.group1_share)}</td>
                      <td style={td}>{fmtShare(row.group2_share)}</td>
                      <td style={{ ...td, fontWeight: 600, background: deltaBackground(row.verdict) }}>
                        {fmtDelta(row.delta, f, row.delta_ci_low, row.delta_ci_high)}
                        <div style={sub}>{fmtInterval(row.delta_ci_low, row.delta_ci_high, f, row.delta)}</div>
                        <AdjustedInterval row={row} format={f} />
                      </td>
                      <td style={td}>{fmtP(row.p_value_adjusted)}</td>
                      <td style={td}>{fmtP(row.stratum_vs_rest_p_value)}</td>
                      <td style={{ ...tdl, fontWeight: 400 }}><VerdictBadge verdict={row.verdict} /> <SupportBadge warning={row.support_warning} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}
      <Legend />
      <PlotNote>{intervalNote(result)}</PlotNote>
    </div>
  );
}

// ── periods ────────────────────────────────────────────────────────────────

const PC_W = 640;
const PC_H = 210;
const PC_LEFT = 64;
const PC_RIGHT = 16;
const PC_TOP = 12;
const PC_BOTTOM = 46;

function periodLabel(row: ABRow): string {
  const p = String(row.period ?? "");
  // calendar periods read "2026-03-02/2026-03-08": the start is enough on the axis
  return p.includes("/") ? p.split("/")[0] : p;
}

/** The delta over periods: an interval band, a line through the deltas, a zero
 *  line; points coloured by verdict, hollow when the period is not complete,
 *  squares for descriptive rows. One unit per metric, so one shared y scale. */
export function PeriodChart({ rows, format }: { rows: ABRow[]; format: MetricFormat }) {
  const values = rows.flatMap(r => [r.delta, r.delta_ci_low, r.delta_ci_high]).filter(isNum);
  let lo = Math.min(0, ...values);
  let hi = Math.max(0, ...values);
  if (hi === lo) { hi += 1; lo -= 1; }
  const pad = (hi - lo) * 0.08;
  lo -= pad; hi += pad;
  const plotW = PC_W - PC_LEFT - PC_RIGHT;
  const plotH = PC_H - PC_TOP - PC_BOTTOM;
  const step = rows.length > 1 ? plotW / (rows.length - 1) : 0;
  const x = (i: number) => PC_LEFT + (rows.length > 1 ? i * step : plotW / 2);
  const y = (v: number) => PC_TOP + (1 - (v - lo) / (hi - lo)) * plotH;

  // band segments over consecutive rows that have both interval ends
  const segments: number[][] = [];
  let cur: number[] = [];
  rows.forEach((r, i) => {
    if (isNum(r.delta_ci_low) && isNum(r.delta_ci_high)) cur.push(i);
    else if (cur.length) { segments.push(cur); cur = []; }
  });
  if (cur.length) segments.push(cur);
  const bandPath = (idx: number[]) => {
    if (idx.length === 1) {
      const i = idx[0];
      return `M${x(i) - 4},${y(rows[i].delta_ci_high as number)} H${x(i) + 4} V${y(rows[i].delta_ci_low as number)} H${x(i) - 4} Z`;
    }
    const top = idx.map(i => `${x(i)},${y(rows[i].delta_ci_high as number)}`);
    const bottom = idx.slice().reverse().map(i => `${x(i)},${y(rows[i].delta_ci_low as number)}`);
    return `M${top.join(" L")} L${bottom.join(" L")} Z`;
  };
  const line = rows.map((r, i) => (isNum(r.delta) ? `${x(i)},${y(r.delta)}` : null)).filter(Boolean);
  const [div, unit] = deltaUnit(format, lo, hi);
  const tick = (v: number) => {
    const text = format.kind === "binary" ? (v * 100).toFixed(1) : fmtNum(v / div);
    return v > 0 ? `+${text}` : text;
  };
  const labelEvery = Math.max(1, Math.ceil(rows.length / 12));

  return (
    <svg data-testid="period-chart" viewBox={`0 0 ${PC_W} ${PC_H}`} width="100%"
      style={{ display: "block", maxWidth: PC_W, fontFamily: "system-ui, sans-serif" }}>
      {[lo, 0, hi].map((v, k) => (
        <g key={k}>
          <line x1={PC_LEFT} x2={PC_W - PC_RIGHT} y1={y(v)} y2={y(v)}
            stroke={v === 0 ? "#6b7280" : "#e5e7eb"} strokeDasharray={v === 0 ? "4 3" : undefined} />
          <text x={PC_LEFT - 6} y={y(v) + 3} textAnchor="end" fontSize={9} fill="#6b7280">
            {v === 0 ? "0" : tick(v)}{k === 2 && unit ? ` ${unit}` : ""}
          </text>
        </g>
      ))}
      {segments.map((idx, k) => (
        <path key={k} data-testid="period-band" d={bandPath(idx)} fill="rgba(156,163,175,0.25)" stroke="none" />
      ))}
      {line.length > 1 && <polyline points={line.join(" ")} fill="none" stroke="#9ca3af" strokeWidth={1.2} />}
      {rows.map((r, i) => {
        const m = markerStyle(r);
        return (
          <g key={i}>
            <title>{pointTitle(String(r.period ?? ""), r, format)}</title>
            {isNum(r.delta) && (
              <g data-testid="period-point" data-verdict={r.verdict ?? ""} data-hollow={String(m.hollow)}
                data-complete={String(r.period_complete !== false)}>
                <Marker x={x(i)} y={y(r.delta)} row={r} r={4.5} />
              </g>
            )}
            {i % labelEvery === 0 && (
              <text x={x(i)} y={PC_H - PC_BOTTOM + 14} textAnchor="end" fontSize={9} fill="#6b7280"
                transform={`rotate(-35, ${x(i)}, ${PC_H - PC_BOTTOM + 14})`}>{periodLabel(r)}</text>
            )}
          </g>
        );
      })}
    </svg>
  );
}

export function PeriodsView({ result }: { result: ABResult }) {
  const rows = result.periods ?? [];
  const anyIncomplete = rows.some(r => r.period_complete === false);
  const anyDescriptive = rows.some(r => r.verdict === "descriptive");
  return (
    <div data-testid="ab-periods-view">
      {groupByMetric(rows).map(([metric, mrows]) => {
        const f = metricFormat(result, metric);
        return (
          <div key={metric} data-testid="period-metric" style={{ marginBottom: 18 }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: "#111827", padding: "0 8px 4px" }}>
              {metric} <span style={{ fontSize: 11, fontWeight: 400, color: "#6b7280" }}>Δ by {result.period_basis === "since_assignment" ? "time since assignment" : "period"} ({result.period})</span>
            </div>
            <PeriodChart rows={mrows} format={f} />
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", marginTop: 4 }}>
                <thead>
                  <tr>
                    <th style={{ ...th, textAlign: "left" }}>Period</th>
                    <th style={th}>Paths</th>
                    <th style={th}>Δ</th>
                    <th style={th}>p adj</th>
                    <th style={{ ...th, textAlign: "left" }}>Verdict</th>
                  </tr>
                </thead>
                <tbody>
                  {mrows.map((row, i) => (
                    <tr key={`${row.period}-${i}`} data-testid="period-row"
                      data-complete={String(row.period_complete !== false)} data-descriptive={String(row.verdict === "descriptive")}>
                      <td style={tdl}>{row.period}{row.period_complete === false ? " ◌" : ""}<MethodDetails row={row} /></td>
                      <td style={td}>{row.group1_paths ?? 0}/{row.group2_paths ?? 0}</td>
                      <td style={{ ...td, fontWeight: 600, background: deltaBackground(row.verdict) }}>
                        {fmtDelta(row.delta, f, row.delta_ci_low, row.delta_ci_high)}
                        <div style={sub}>{fmtInterval(row.delta_ci_low, row.delta_ci_high, f, row.delta)}</div>
                        <AdjustedInterval row={row} format={f} />
                      </td>
                      <td style={td}>{fmtP(row.p_value_adjusted)}</td>
                      <td style={{ ...tdl, fontWeight: 400 }}><VerdictBadge verdict={row.verdict} /> <SupportBadge warning={row.support_warning} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}
      <Legend />
      <PlotNote>
        {anyIncomplete && <>◌ / hollow: the period is not complete (the data ends inside it, or the horizon cuts it). </>}
        {anyDescriptive && <>Squares: descriptive rows (active paths only: survivors, not randomised groups; no p-value). </>}
        {intervalNote(result)}
      </PlotNote>
    </div>
  );
}
