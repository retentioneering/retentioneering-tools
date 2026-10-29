/** A read-only view of one calculated experiment payload. No compute RPCs. */
import * as React from "react";
import { createRoot } from "react-dom/client";
import { parseJson, useHostSubscriptions, type RenderContext } from "./widget-utils";

export interface ExperimentRow {
  metric: string;
  family: string;
  label?: string;
  unit?: string;
  scale?: number;
  outcome_kind?: string;
  role?: string;
  direction?: string;
  step?: string;
  step_index?: number;
  [key: string]: unknown;
}

export interface ExperimentPayload {
  schema_version: string;
  summary: ExperimentRow[];
  diagnostics: Array<Record<string, unknown>>;
  metadata: Record<string, any>;
}

const TEXT = "#172b3a";
const MUTED = "#526573";
const BORDER = "#dfe6eb";
const cell: React.CSSProperties = { padding: "11px 12px", borderBottom: `1px solid ${BORDER}`, textAlign: "left", verticalAlign: "top" };
const small: React.CSSProperties = { fontSize: 11, color: MUTED, lineHeight: 1.55 };
const button: React.CSSProperties = { padding: "7px 10px", border: `1px solid ${BORDER}`, borderRadius: 6, background: "#fff", color: TEXT, cursor: "pointer", fontSize: 12 };

function finite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function number(value: unknown, signed = false): string {
  if (!finite(value)) return "Unavailable";
  const text = value.toLocaleString("en-US", { maximumSignificantDigits: 4 });
  return signed && value > 0 ? `+${text}` : text;
}

function label(value: unknown): string {
  return value === null || value === undefined ? "—" : String(value);
}

function words(value: unknown): string {
  return label(value).replace(/_/g, " ");
}

function scale(row: ExperimentRow): number {
  return finite(row.scale) && row.scale > 0 ? row.scale : 1;
}

function measurementUnit(row: ExperimentRow, effect = false): string {
  const probability = row.outcome_kind === "binary" || row.family.startsWith("funnel_");
  if (probability && scale(row) === 100) return effect ? "pp" : "%";
  if (probability && scale(row) === 1) return effect ? "proportion difference" : "proportion";
  return row.unit || "canonical units";
}

function estimate(row: ExperimentRow, key: string, signed = false): string {
  const value = row[key];
  return number(finite(value) ? value * scale(row) : null, signed);
}

function methodName(value: unknown): string {
  const names: Record<string, string> = {
    bootstrap_percentile: "Percentile bootstrap", wilson: "Wilson", newcombe: "Newcombe",
  };
  return names[String(value)] ?? words(value);
}

export function Interval({ row, prefix }: { row: ExperimentRow; prefix: string }) {
  const lo = row[`${prefix}_ci_low`], hi = row[`${prefix}_ci_high`];
  const status = row[`${prefix}_status`];
  const confidence = row[`${prefix}_ci_confidence_level`];
  const method = row[`${prefix}_ci_method`];
  const invalid = row[`${prefix}_invalid_resamples`];
  return (
    <div style={small}>
      {finite(lo) && finite(hi)
        ? <div>[{number(lo * scale(row))}, {number(hi * scale(row))}]</div>
        : <div>CI unavailable: {words(status)}</div>}
      {method != null && <div>{finite(confidence) ? `${number(confidence * 100)}% · ` : ""}{methodName(method)}</div>}
      {finite(invalid) && invalid > 0 && <div>{number(invalid)} undefined resamples</div>}
    </div>
  );
}

function effectDomain(rows: ExperimentRow[]): [number, number] {
  const values = [0];
  for (const row of rows) {
    for (const key of ["absolute_effect", "absolute_effect_ci_low", "absolute_effect_ci_high"]) {
      const v = row[key];
      if (finite(v)) values.push(v * scale(row));
    }
  }
  const low = Math.min(...values), high = Math.max(...values);
  const padding = (high - low || 1) * .08;
  return [low - padding, high + padding];
}

function EffectPlot({ row, domain }: { row: ExperimentRow; domain: [number, number] }) {
  const [low, high] = domain;
  const x = (v: number) => 9 + (v * scale(row) - low) / (high - low) * 190;
  const zero = 9 + (0 - low) / (high - low) * 190;
  const lo = row.absolute_effect_ci_low, hi = row.absolute_effect_ci_high;
  const point = row.absolute_effect;
  return (
    <svg role="img" aria-label={`${row.label ?? row.metric}: treatment minus control ${estimate(row, "absolute_effect", true)}`} viewBox="0 0 210 38" style={{ display: "block", width: 210, maxWidth: "100%" }}>
      <line x1={zero} x2={zero} y1={2} y2={27} stroke="#a8b5be" strokeDasharray="3 2" />
      {finite(lo) && finite(hi) && <g stroke="#42657d" strokeWidth={2}>
        <line x1={x(lo)} x2={x(hi)} y1={15} y2={15} />
        <line x1={x(lo)} x2={x(lo)} y1={11} y2={19} />
        <line x1={x(hi)} x2={x(hi)} y1={11} y2={19} />
      </g>}
      {finite(point) && <circle cx={x(point)} cy={15} r={4} fill="#273a42" />}
      <text x={zero} y={36} textAnchor="middle" fontSize={9} fill={MUTED}>0</text>
    </svg>
  );
}

function ArmValue({ row, arm }: { row: ExperimentRow; arm: "control" | "treatment" }) {
  return <>
    <strong>{estimate(row, arm)}</strong> <span style={small}>{measurementUnit(row)}</span>
    <Interval row={row} prefix={arm} />
    <div style={{ ...small, marginTop: 5 }}>
      Numerator {number(row[`${arm}_numerator`])}<br />
      Denominator {number(row[`${arm}_denominator`])}<br />
      Units {number(row[`${arm}_units`])}
      {finite(row[`${arm}_eligible_units`]) && <><br />Eligible {number(row[`${arm}_eligible_units`])}</>}
      {finite(row[`${arm}_nonzero_units`]) && <><br />Nonzero {number(row[`${arm}_nonzero_units`])}</>}
    </div>
  </>;
}

export function ExperimentTable({ rows, control, treatment, sharedScale = false }: {
  rows: ExperimentRow[]; control: unknown; treatment: unknown; sharedScale?: boolean;
}) {
  const domain = effectDomain(rows);
  return <div style={{ overflowX: "auto" }}>
    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, minWidth: 800 }}>
      <thead><tr>
        {["Outcome", `Control · ${label(control)}`, `Treatment · ${label(treatment)}`, "Treatment − control", "Declared tests"].map(heading =>
          <th key={heading} scope="col" style={{ ...cell, color: MUTED, background: "#f7f9fa", fontWeight: 600 }}>{heading}</th>)}
      </tr></thead>
      <tbody>{rows.map(row => <tr key={`${row.family}:${row.metric}`} data-metric={row.metric}>
        <th scope="row" style={{ ...cell, fontWeight: 600, minWidth: 155 }}>
          {row.step_index != null ? `${row.step_index}. ` : ""}{row.label ?? row.step ?? row.metric}
          <div style={{ ...small, fontWeight: 400 }}>Values: {measurementUnit(row)}<br />Difference: {measurementUnit(row, true)}</div>
          {row.role && <div style={small}>{words(row.role)}</div>}
          {row.direction && row.direction !== "neutral" && <div style={small}>{words(row.direction)}</div>}
        </th>
        <td style={cell}><ArmValue row={row} arm="control" /></td>
        <td style={cell}><ArmValue row={row} arm="treatment" /></td>
        <td style={{ ...cell, minWidth: 210 }}>
          <strong>{estimate(row, "absolute_effect", true)}</strong> <span style={small}>{measurementUnit(row, true)}</span>
          <Interval row={row} prefix="absolute_effect" />
          <EffectPlot row={row} domain={sharedScale ? domain : effectDomain([row])} />
          <div style={small}>Relative effect: {finite(row.relative_effect) ? `${number(row.relative_effect * 100, true)}%` : words(row.relative_effect_status)}
            {finite(row.relative_effect_ci_low) && finite(row.relative_effect_ci_high)
              ? <div>Relative CI [{number(row.relative_effect_ci_low * 100)}, {number(row.relative_effect_ci_high * 100)}]%</div>
              : <div>Relative CI unavailable: {words(row.relative_effect_status)}</div>}
            {row.relative_effect_ci_method != null && <div>{finite(row.relative_effect_ci_confidence_level) ? `${number(row.relative_effect_ci_confidence_level * 100)}% · ` : ""}{methodName(row.relative_effect_ci_method)}</div>}
          </div>
        </td>
        <td style={cell}>
          {row.test_method ? <>
            <div>{words(row.test_method)}</div>
            <div style={small}>p {number(row.p_value)}<br />Adjusted p {number(row.p_value_adjusted)}<br />Status: {words(row.test_status)}</div>
          </> : <span style={small}>Not requested</span>}
        </td>
      </tr>)}</tbody>
    </table>
  </div>;
}

export function Diagnostics({ payload }: { payload: ExperimentPayload }) {
  const meta = payload.metadata ?? {};
  const observation = meta.observation ?? {};
  const bootstrap = meta.bootstrap;
  return <aside style={{ padding: 14, border: `1px solid ${BORDER}`, borderRadius: 8, background: "#f7f9fa", fontSize: 12 }}>
    <h3 style={{ fontSize: 13, margin: "0 0 10px" }}>Population and calculation</h3>
    <div style={{ overflowX: "auto" }}><table style={{ borderCollapse: "collapse", width: "100%" }}>
      <thead><tr>{["Variant", "Assigned", "Observed", "Silent"].map(h => <th key={h} scope="col" style={{ ...cell, padding: "5px 8px" }}>{h}</th>)}</tr></thead>
      <tbody>{payload.diagnostics.map((d, i) => <tr key={i}>
        {[label(d.arm), number(d.n_assigned), number(d.n_observed), number(d.n_without_events)].map((v, j) => <td key={j} style={{ ...cell, padding: "5px 8px" }}>{v}</td>)}
      </tr>)}</tbody>
    </table></div>
    <p style={small}>Population: {words(meta.population_basis)}<br />Unit: {label(meta.unit_col)}<br />Window: [{label(observation.start)}, {label(observation.end)})</p>
    <p style={small}>{bootstrap
      ? `${number(bootstrap.n_resamples)} bootstrap resamples · seed ${label(bootstrap.random_state)} · ${words(meta.sampling_design)}`
      : "Bootstrap not requested."}</p>
    {meta.srm && <p style={small}>Allocation diagnostic: {meta.srm.method ? `${words(meta.srm.method)} · p ${number(meta.srm.p_value)}` : words(meta.srm.status)}
      {meta.srm.scheme_assumption && <><br />Assumption: {words(meta.srm.scheme_assumption)}</>}
      {finite(meta.srm.expected_treatment) && <><br />Expected treatment allocation: {number(meta.srm.expected_treatment * 100)}%</>}
    </p>}
    {Array.isArray(meta.warnings) && meta.warnings.length > 0 && <div style={small}>Diagnostics: {meta.warnings.map(words).join("; ")}</div>}
    <p style={small}>Calculated snapshot. Changing the view does not resample or select a different test. Create a new experiment to change the analysis specification.</p>
  </aside>;
}

export function ExperimentContent({ payload, showDiagnostics = true, family = "scorecard", onFamilyChange }: {
  payload: ExperimentPayload; showDiagnostics?: boolean; family?: string; onFamilyChange?: (family: string) => void;
}) {
  if (!payload || payload.schema_version !== "2.0" || !Array.isArray(payload.summary)) {
    return <div role="alert" style={{ padding: 20 }}>Unsupported experiment result schema. Recalculate with the matching Retentioneering version.</div>;
  }
  const families = [
    ["scorecard", "Outcomes"], ["funnel_cumulative", "Cumulative funnel"], ["funnel_conditional", "Conditional funnel"],
  ].filter(([name]) => payload.summary.some(row => row.family === name));
  const selected = families.some(([name]) => name === family) ? family : families[0]?.[0];
  const rows = payload.summary.filter(row => row.family === selected);
  const meta = payload.metadata ?? {};
  return <>
    <nav aria-label="Experiment views" style={{ display: "flex", gap: 6, padding: "0 0 12px", flexWrap: "wrap" }}>
      {families.map(([name, title]) => <button key={name} type="button" aria-pressed={selected === name} onClick={() => onFamilyChange?.(name)} style={{ ...button, background: selected === name ? "#e9f0f4" : "#fff", fontWeight: selected === name ? 600 : 400 }}>{title}</button>)}
    </nav>
    <p style={{ ...small, marginTop: 0 }}>Intervals are pointwise. Read the method and status for each estimate; an unavailable interval is not a zero-width interval.</p>
    {selected === "funnel_conditional" && <p style={{ ...small, padding: 10, background: "#fff7e9", borderRadius: 6 }}>Conditional rates use the previous-step population, which can change with treatment. The first step uses all assigned units. These contrasts do not isolate a causal mechanism.</p>}
    {selected === "funnel_cumulative" && <p style={small}>Ordered reach divided by all assigned units, including silent assignments.</p>}
    {rows.length > 0 ? <ExperimentTable rows={rows} control={meta.control} treatment={meta.treatment} sharedScale={selected !== "scorecard"} /> : <p>No outcomes in this result.</p>}
    <p style={small}>{selected === "scorecard" ? "Effect plots use a separate scale for each outcome." : "Effect plots share a scale within this funnel."} Dashed line: zero absolute effect. Colours do not declare a winner.</p>
    {showDiagnostics && <Diagnostics payload={payload} />}
  </>;
}

export function render({ host, el }: RenderContext) {
  function App() {
    const [payload, setPayload] = React.useState<ExperimentPayload>(() => parseJson(host.get("result"), {} as ExperimentPayload));
    const [height, setHeight] = React.useState<number>(() => Number(host.get("height")) || 650);
    const [showDiagnostics, setShowDiagnostics] = React.useState<boolean>(() => (host.get("sidebar_open") as boolean) ?? true);
    const [family, setFamily] = React.useState("scorecard");
    useHostSubscriptions(host, [
      ["result", () => setPayload(parseJson(host.get("result"), {} as ExperimentPayload))],
      ["height", () => setHeight(Number(host.get("height")) || 650)],
      ["sidebar_open", () => setShowDiagnostics((host.get("sidebar_open") as boolean) ?? true)],
    ]);
    return <section aria-label="Experiment results" style={{ height, overflowY: "auto", padding: 16, boxSizing: "border-box", color: TEXT, background: "#fff", border: `1px solid ${BORDER}`, borderRadius: 8, fontFamily: "system-ui, -apple-system, sans-serif" }}>
      <header style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, marginBottom: 14 }}>
        <h2 style={{ fontSize: 17, margin: 0 }}>Experiment results</h2>
        <button type="button" aria-pressed={showDiagnostics} style={button} onClick={() => { const next = !showDiagnostics; setShowDiagnostics(next); host.set("sidebar_open", next); }}>{showDiagnostics ? "Hide" : "Show"} diagnostics</button>
      </header>
      <ExperimentContent payload={payload} showDiagnostics={showDiagnostics} family={family} onFamilyChange={setFamily} />
    </section>;
  }
  const root = createRoot(el);
  root.render(<App />);
  return () => root.unmount();
}
