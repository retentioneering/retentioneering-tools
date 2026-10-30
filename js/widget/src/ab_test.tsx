/**
 * A/B Test widget: the `get_ab_test` scorecard as a table and a forest plot,
 * with Strata and Periods tabs. Every sidebar change sets one traitlet; Python
 * recomputes through `ab_test_data` and sends the rows back in `result`.
 */
import * as React from "react";
import { createRoot } from "react-dom/client";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes, useHostSubscriptions, type RenderContext } from "./widget-utils";
import { METRIC_NAMES, MetricRow, validateMetricCfg } from "./metric_config_row";
import { type ABResult, ScorecardView, StrataView, PeriodsView } from "./ab_test_plot";

// ── options ────────────────────────────────────────────────────────────────

/** Registry metrics `get_ab_test` accepts: one number per path. */
export const AB_METRIC_NAMES = METRIC_NAMES.filter(m => !m.endsWith("_bulk") && m !== "first_event_time");
const BINARY_METRICS = ["has_event", "has_any_event", "has_all_events", "matches_pattern", "in_segment"];
/** A denominator turns a metric into a ratio of sums; a 0/1 metric takes none. */
const DENOMINATOR_NAMES = AB_METRIC_NAMES.filter(m => !BINARY_METRICS.includes(m));

const CI_LEVELS = [0.9, 0.95, 0.99];
const CORRECTIONS: [string, string][] = [
  ["holm", "Holm"], ["bonferroni", "Bonferroni"], ["bh", "Benjamini–Hochberg"], ["none", "None"],
];
const SCOPES: [string, string][] = [["metric", "Each metric"], ["all", "All metrics together"]];
export const TESTS: [string, string][] = [
  ["auto", "Auto: binary exact; other outcomes descriptive"], ["fisher_exact", "Fisher exact (binary)"],
  ["descriptive", "Descriptive (no inference)"], ["z_test", "z-test (binary, approximate)"],
  ["welch_t_test", "Welch (nonbinary, approximate)"], ["bootstrap", "Bootstrap (nonbinary, approximate)"],
];
const PERIODS: [string, string][] = [["", "None"], ["D", "Day"], ["W", "Week"], ["M", "Month"]];
const PERIOD_BASES: [string, string][] = [["calendar", "Calendar"], ["since_assignment", "Since assignment"]];
const PRE_EXPOSURE: [string, string][] = [
  ["", "Read the recipe"], ["true", "Declared pre-exposure"], ["false", "Post-exposure (exploratory)"],
];

/** A duration with a unit, as `horizon` takes it: `14D`, `28 days`, `12h`, `W`.
 *  A bare number has no unit and is refused, as in Python. */
export function isValidDuration(text: string): boolean {
  return /^\s*(\d+(\.\d+)?\s*)?(d|days?|h|hours?|min|minutes?|s|sec|seconds?|w|weeks?)\s*$/i.test(text)
    && !/^\s*$/.test(text);
}

const fmtLevel = (v: number) => `${+(v * 100).toFixed(1)}%`;

// ── metric editor ──────────────────────────────────────────────────────────

/** The checks `get_ab_test` would make, done before the round trip. */
export function validateAbMetric(entry: any, names: string[]): string | null {
  if (!entry?.metric) return "Select a metric";
  if (!AB_METRIC_NAMES.includes(entry.metric)) return `${entry.metric} is not one number per path`;
  const own = validateMetricCfg(entry);
  if (own) return own;
  if (entry.denominator) {
    if (BINARY_METRICS.includes(entry.metric)) return "A 0/1 metric takes no denominator";
    const den = validateMetricCfg(entry.denominator);
    if (den) return `Denominator: ${den}`;
  }
  if (entry.name && names.filter(n => n === entry.name).length > 1) return `Duplicate name "${entry.name}"`;
  return null;
}

function cleanEntry(entry: any): any {
  const out: any = { ...entry };
  if (!out.name) delete out.name;
  if (out.metric_args === undefined) delete out.metric_args;
  if (!out.denominator) delete out.denominator;
  return out;
}

function ABMetricRow({ entry, events, segmentCols, segmentLevels, customCols, showErrors, error, onChange, onRemove }: {
  entry: any; events: string[]; segmentCols: string[]; segmentLevels: Record<string, string[]>;
  customCols: Record<string, boolean>; showErrors: boolean; error: string | null;
  onChange: (e: any) => void; onRemove: () => void;
}) {
  const binary = BINARY_METRICS.includes(entry.metric);
  const inp: React.CSSProperties = { flex: 1, minWidth: 0, boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 5, padding: "4px 7px", fontSize: 11, outline: "none" };
  return (
    <div data-testid="ab-metric-row" style={{ border: `1px solid ${showErrors && error ? "#fca5a5" : "#e5e7eb"}`, borderRadius: 8, padding: 8, background: "#fff", display: "flex", flexDirection: "column", gap: 6 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <span style={{ fontSize: 10, color: "#6b7280", flexShrink: 0 }}>Name</span>
        <input aria-label="Metric name" value={entry.name ?? ""} placeholder="row label (optional)"
          onChange={e => onChange({ ...entry, name: e.target.value })} style={inp} />
      </div>
      <MetricRow cfg={{ metric: entry.metric, metric_args: entry.metric_args }} events={events}
        segmentCols={segmentCols} segmentLevels={segmentLevels} customCols={customCols}
        showErrors={showErrors} showAgg={false} metricNames={AB_METRIC_NAMES}
        onChange={c => onChange({
          ...entry, metric: c.metric, metric_args: c.metric_args,
          ...(BINARY_METRICS.includes(c.metric) ? { denominator: undefined } : {}),
        })}
        onRemove={onRemove} />
      {!binary && (
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: "#374151" }}>
          <input type="checkbox" checked={!!entry.denominator}
            onChange={e => onChange({ ...entry, denominator: e.target.checked ? { metric: "event_count" } : undefined })} />
          Ratio of sums: divide by
        </label>
      )}
      {!binary && entry.denominator && (
        <div style={{ paddingLeft: 16 }}>
          <MetricRow cfg={entry.denominator} events={events} segmentCols={segmentCols} segmentLevels={segmentLevels}
            customCols={customCols} showErrors={showErrors} showAgg={false} metricNames={DENOMINATOR_NAMES}
            onChange={c => onChange({ ...entry, denominator: { metric: c.metric, metric_args: c.metric_args } })}
            onRemove={() => onChange({ ...entry, denominator: undefined })} />
        </div>
      )}
      {showErrors && error && <div style={{ fontSize: 10, color: "#dc2626" }}>⚠ {error}</div>}
    </div>
  );
}

/** Edits a draft; only Apply writes `metrics`, so one edit session is one recompute. */
function MetricsOverlay({ metrics, events, segmentCols, segmentLevels, customCols, onApply, onClose }: {
  metrics: any[]; events: string[]; segmentCols: string[]; segmentLevels: Record<string, string[]>;
  customCols: Record<string, boolean>; onApply: (m: any[]) => void; onClose: () => void;
}) {
  const [draft, setDraft] = React.useState<any[]>(() => metrics.map(m => ({ ...m })));
  const [submitted, setSubmitted] = React.useState(false);
  React.useEffect(() => {
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", k);
    return () => document.removeEventListener("keydown", k);
  }, [onClose]);
  const names = draft.map(m => m.name).filter(Boolean);
  const errors = draft.map(m => validateAbMetric(m, names));
  const errCount = errors.filter(Boolean).length;
  const apply = () => {
    if (draft.length === 0 || errCount > 0) { setSubmitted(true); return; }
    onApply(draft.map(cleanEntry));
    onClose();
  };
  return (
    <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.3)", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={e => e.target === e.currentTarget && onClose()}>
      <div data-testid="ab-metrics-editor" style={{ background: "#fff", borderRadius: 12, boxShadow: "0 8px 32px rgba(0,0,0,0.18)", padding: 20, width: 540, maxWidth: "92%", maxHeight: "85%", display: "flex", flexDirection: "column" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 6 }}>
          <span style={{ fontSize: 15, fontWeight: 600, color: "#111827" }}>Metrics</span>
          <button onClick={onClose} style={{ background: "none", border: "none", fontSize: 20, cursor: "pointer", color: "#6b7280" }}>×</button>
        </div>
        <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 10, lineHeight: 1.5 }}>
          One number per path. 0/1 metrics are compared as shares (z-test), totals and values as means (Welch),
          ratios of sums by bootstrap. Choose the decision metric before looking at the result.
        </div>
        <div style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: 8 }}>
          {draft.length === 0 && (
            <div style={{ textAlign: "center", padding: "28px 0", color: "#9ca3af", fontSize: 13, border: "1px dashed #e5e7eb", borderRadius: 8 }}>
              No metrics yet. Click "Add metric".
            </div>
          )}
          {draft.map((m, i) => (
            <ABMetricRow key={i} entry={m} events={events} segmentCols={segmentCols} segmentLevels={segmentLevels}
              customCols={customCols} showErrors={submitted} error={errors[i]}
              onChange={e => setDraft(d => d.map((x, j) => (j === i ? e : x)))}
              onRemove={() => setDraft(d => d.filter((_, j) => j !== i))} />
          ))}
        </div>
        <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 8 }}>
          <button onClick={() => setDraft(d => [...d, { metric: "has_event" }])}
            style={{ width: "100%", padding: "7px 0", border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", cursor: "pointer", fontSize: 13, color: "#374151", fontWeight: 500 }}>
            + Add metric
          </button>
          {submitted && errCount > 0 && <div style={{ fontSize: 11, color: "#dc2626", textAlign: "center" }}>⚠ {errCount} metric{errCount > 1 ? "s need" : " needs"} fixing</div>}
          {submitted && draft.length === 0 && <div style={{ fontSize: 11, color: "#dc2626", textAlign: "center" }}>⚠ Add at least one metric</div>}
          <button data-testid="ab-metrics-apply" onClick={apply}
            style={{ width: "100%", padding: "7px 0", background: "var(--retentioneering-yellow)", border: "none", borderRadius: 6, cursor: "pointer", fontSize: 13, fontWeight: 600, color: "#1a1a1a" }}>
            Apply
          </button>
        </div>
      </div>
    </div>
  );
}

// ── sidebar ────────────────────────────────────────────────────────────────

const SIDEBAR_W = 256;
const sel: React.CSSProperties = {
  width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 6, color: "#111827",
  fontSize: 12, padding: "5px 24px 5px 8px", cursor: "pointer", outline: "none", appearance: "none",
  backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%236b7280' stroke-width='2'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E")`,
  backgroundRepeat: "no-repeat", backgroundPosition: "right 6px center", background: "#f9fafb",
};
// Module-level so the sidebar does not remount them (focus loss in inputs).
const SH = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: "#6b7280", margin: "0 0 6px" }}>{children}</div>
);
const Rule = () => <div style={{ height: 1, background: "#e5e7eb", margin: "14px 0" }} />;
const Hint = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 10, color: "#6b7280", marginTop: 4, lineHeight: 1.4 }}>{children}</div>
);

interface SidebarProps {
  host: RenderContext["host"];
  isStatic: boolean;
  isLoading: boolean;
  params: Record<string, any>;
  segLevels: Record<string, unknown[]>;
  segmentCols: string[];
  pathCols: string[];
  assignmentInfo: string;
  metricsCount: number;
  onOpenMetrics: () => void;
}

/** Columns that can name the arms: the stream's segments and, with an assignment
 *  table, its `arm` column (`diff=("arm", ...)`, the table-only form). */
export function armColumnChoices(segmentCols: string[], assignmentInfo: string): string[] {
  return assignmentInfo && !segmentCols.includes("arm") ? ["arm", ...segmentCols] : segmentCols;
}

/** Keep numeric and string levels distinct in HTML select values and round trips. */
export function armLevelOptions(levels: unknown[]): { value: string; label: string }[] {
  return levels.map(level => ({
    value: JSON.stringify(level),
    label: levels.filter(other => String(other) === String(level)).length > 1
      ? `${JSON.stringify(level)} (${typeof level})` : String(level),
  }));
}

function Sidebar({ host, isStatic, isLoading, params, segLevels, segmentCols, pathCols, assignmentInfo, metricsCount, onOpenMetrics }: SidebarProps) {
  const disabled = isLoading || isStatic;
  const diff: unknown[] | null = params.diff;
  const segmentDiff = Array.isArray(diff) && diff.length === 3 ? diff : null;
  const idDiff = Array.isArray(diff) && diff.length === 2;
  const [seg, setSeg] = React.useState(String(segmentDiff?.[0] ?? ""));
  const [v1, setV1] = React.useState(segmentDiff ? JSON.stringify(segmentDiff[1]) : "");
  const [v2, setV2] = React.useState(segmentDiff ? JSON.stringify(segmentDiff[2]) : "");
  React.useEffect(() => {
    setSeg(String(segmentDiff?.[0] ?? "")); setV1(segmentDiff ? JSON.stringify(segmentDiff[1]) : ""); setV2(segmentDiff ? JSON.stringify(segmentDiff[2]) : "");
  }, [JSON.stringify(diff)]);
  const levels = seg ? armLevelOptions(segLevels[seg] ?? []) : [];
  const armsDirty = seg !== (segmentDiff?.[0] ?? "") || v1 !== (segmentDiff ? JSON.stringify(segmentDiff[1]) : "") || v2 !== (segmentDiff ? JSON.stringify(segmentDiff[2]) : "");
  const canApplyArms = !!seg && !!v1 && !!v2 && v1 !== v2;

  const [horizon, setHorizon] = React.useState<string>(params.horizon || "");
  React.useEffect(() => setHorizon(params.horizon || ""), [params.horizon]);
  const horizonValid = horizon.trim() === "" || isValidDuration(horizon);
  const hasAssignedAt = assignmentInfo.includes("assigned_at");
  const commitHorizon = () => {
    if (horizonValid && horizon.trim() !== (params.horizon || "")) host.set("horizon", horizon.trim());
  };

  const set = (key: string) => (e: React.ChangeEvent<HTMLSelectElement | HTMLInputElement>) => host.set(key, e.target.value);
  const options = (list: [string, string][], current: string) =>
    (list.some(([v]) => v === current) ? list : [...list, [current, current] as [string, string]])
      .map(([v, label]) => <option key={v} value={v}>{label}</option>);
  const strataChoices = segmentCols.filter(c => c !== segmentDiff?.[0]);
  const armChoices = armColumnChoices(segmentCols, assignmentInfo);

  return (
    <div data-testid="ab-sidebar" style={{ width: SIDEBAR_W, minWidth: SIDEBAR_W, height: "100%", background: "#fff", borderLeft: "1px solid #e5e7eb", display: "flex", flexDirection: "column", overflow: "hidden", fontFamily: "system-ui, sans-serif" }}>
      <div style={{ padding: "10px 14px", borderBottom: "1px solid #e5e7eb", display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#111827" }}>Settings</span>
        {isLoading && <span style={{ fontSize: 11, color: "#6b7280" }}>Computing…</span>}
      </div>
      <div style={{ flex: 1, overflowY: "auto", padding: 14 }}>
        <SH>Arms (group 1 vs group 2)</SH>
        {idDiff ? (
          <Hint>Two lists of path ids, set in code.</Hint>
        ) : (
          <>
            <select aria-label="Arm column" value={seg} disabled={disabled} style={{ ...sel, marginBottom: 6 }}
              onChange={e => { const c = e.target.value; setSeg(c); const l = armLevelOptions(segLevels[c] ?? []); setV1(l[0]?.value ?? ""); setV2(l[1]?.value ?? ""); }}>
              <option value="">— select —</option>
              {armChoices.map(c => <option key={c} value={c}>{c === "arm" && !segmentCols.includes("arm") ? "arm (assignment table)" : c}</option>)}
            </select>
            {seg && (
              <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                <span style={{ color: "#ef4444", fontSize: 13 }}>●</span>
                <select aria-label="Group 1" value={v1} disabled={disabled} style={{ ...sel, flex: 1, minWidth: 0, width: "auto" }} onChange={e => setV1(e.target.value)}>
                  {levels.map(l => <option key={l.value} value={l.value} disabled={l.value === v2}>{l.label}</option>)}
                </select>
                <span style={{ color: "#6b7280", fontSize: 11 }}>vs</span>
                <span style={{ color: "#3b82f6", fontSize: 13 }}>●</span>
                <select aria-label="Group 2" value={v2} disabled={disabled} style={{ ...sel, flex: 1, minWidth: 0, width: "auto" }} onChange={e => setV2(e.target.value)}>
                  {levels.map(l => <option key={l.value} value={l.value} disabled={l.value === v1}>{l.label}</option>)}
                </select>
              </div>
            )}
            {armsDirty && !isStatic && (
              <button disabled={!canApplyArms || isLoading} onClick={() => host.set("diff", JSON.stringify([seg, JSON.parse(v1), JSON.parse(v2)]))}
                style={{ marginTop: 8, width: "100%", padding: "6px 0", background: canApplyArms ? "var(--retentioneering-yellow)" : "#f3f4f6", border: "none", borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: canApplyArms ? "pointer" : "default", color: canApplyArms ? "#1a1a1a" : "#9ca3af" }}>
                Apply
              </button>
            )}
          </>
        )}

        <Rule />
        <SH>Metrics</SH>
        <button data-testid="ab-metrics-button" onClick={onOpenMetrics} disabled={isStatic}
          style={{ width: "100%", padding: "6px 10px", border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", cursor: isStatic ? "default" : "pointer", display: "flex", justifyContent: "space-between", fontSize: 12, color: "#374151", fontWeight: 500 }}>
          <span>Configure metrics</span>
          <span style={{ fontSize: 10, color: "#9ca3af", background: "#f3f4f6", borderRadius: 10, padding: "1px 6px" }}>{metricsCount}</span>
        </button>

        <Rule />
        <SH>Confidence level</SH>
        <select aria-label="Confidence level" value={String(params.ci_level)} disabled={disabled} style={sel}
          onChange={e => host.set("ci_level", Number(e.target.value))}>
          {(CI_LEVELS.includes(params.ci_level) ? CI_LEVELS : [...CI_LEVELS, params.ci_level].sort((a, b) => a - b))
            .map(v => <option key={v} value={String(v)}>{fmtLevel(v)}</option>)}
        </select>
        <div style={{ marginTop: 10 }}><SH>Correction</SH></div>
        <select aria-label="Correction" value={params.correction} disabled={disabled} style={sel} onChange={set("correction")}>
          {options(CORRECTIONS, params.correction)}
        </select>
        <select aria-label="Corrected together" value={params.correction_scope} disabled={disabled} style={{ ...sel, marginTop: 6 }} onChange={set("correction_scope")}>
          {options(SCOPES, params.correction_scope)}
        </select>
        <Hint>{params.correction_scope === "all"
          ? "One family over every row of every metric: a scorecard read without one decision metric."
          : "Within each metric's rows (its strata, its periods); metrics are separate families."}</Hint>
        <div style={{ marginTop: 10 }}><SH>Test</SH></div>
        <select aria-label="Test" value={params.test} disabled={disabled} style={sel} onChange={set("test")}>
          {options(TESTS, params.test)}
        </select>
        <Hint>Auto uses Fisher exact for binary outcomes and descriptive estimates otherwise. Explicit Welch and bootstrap require nonbinary outcomes; z-test requires adequate expected cells and remains approximate.</Hint>

        <Rule />
        <SH>Strata</SH>
        <select aria-label="Strata column" value={params.strata_col || ""} disabled={disabled} style={sel} onChange={set("strata_col")}>
          <option value="">— None</option>
          {strataChoices.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        {params.strata_col && (
          <>
            <select aria-label="Strata fixed before exposure" disabled={disabled} style={{ ...sel, marginTop: 6 }}
              value={params.strata_pre_exposure === true ? "true" : params.strata_pre_exposure === false ? "false" : ""}
              onChange={e => host.set("strata_pre_exposure", e.target.value === "" ? null : e.target.value === "true")}>
              {PRE_EXPOSURE.map(([v, label]) => <option key={v} value={v}>{label}</option>)}
            </select>
            <Hint>A stratum must be fixed before exposure (a platform at signup); otherwise its rows are exploratory.</Hint>
          </>
        )}

        <Rule />
        <SH>Periods</SH>
        <select aria-label="Period" value={params.period || ""} disabled={disabled} style={sel} onChange={set("period")}>
          {options(PERIODS, params.period || "")}
        </select>
        {params.period && (
          <select aria-label="Period basis" value={params.period_basis} disabled={disabled} style={{ ...sel, marginTop: 6 }} onChange={set("period_basis")}>
            {PERIOD_BASES.map(([v, label]) => (
              <option key={v} value={v} disabled={v === "since_assignment" && !hasAssignedAt}>{label}</option>
            ))}
          </select>
        )}
        {params.period && !hasAssignedAt && <Hint>Since-assignment bins need an assignment table with assigned_at.</Hint>}

        <Rule />
        <SH>Horizon</SH>
        <input aria-label="Complete observation cutoff" value={params.observation_end || ""}
          disabled={disabled} placeholder="Complete logging through (exclusive timestamp)"
          onChange={set("observation_end")}
          style={{ ...sel, width: "100%", marginBottom: 6 }} />
        <Hint>Required for a horizon or since-assignment periods. Declare the logging cutoff; the last event is not evidence of complete observation.</Hint>
        <input aria-label="Horizon" value={horizon} disabled={disabled || !hasAssignedAt}
          placeholder={hasAssignedAt ? "e.g. 28D (empty: whole window)" : "needs assignment with assigned_at"}
          onChange={e => setHorizon(e.target.value)} onBlur={commitHorizon}
          onKeyDown={e => { if (e.key === "Enter") commitHorizon(); }}
          style={{ width: "100%", boxSizing: "border-box", border: `1px solid ${horizonValid ? "#d1d5db" : "#fca5a5"}`, borderRadius: 6, padding: "5px 8px", fontSize: 12, outline: "none", background: disabled || !hasAssignedAt ? "#f9fafb" : "#fff" }} />
        {!horizonValid
          ? <div data-testid="horizon-error" style={{ fontSize: 10, color: "#dc2626", marginTop: 4 }}>A duration with a unit: 14D, 28 days, 12h, W.</div>
          : <Hint>Events in [assigned_at, assigned_at + horizon) per path; paths whose window is not closed are left out.</Hint>}

        {pathCols.length > 1 && (
          <>
            <Rule />
            <SH>Path column (the randomised unit)</SH>
            <select aria-label="Path column" value={params.path_col || pathCols[0]} disabled={disabled} style={sel} onChange={set("path_col")}>
              {pathCols.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </>
        )}
      </div>
    </div>
  );
}

function SidebarToggle({ onClick }: { onClick: () => void }) {
  return (
    <button onClick={onClick} title="Toggle settings" style={{ position: "absolute", top: 10, right: 16, zIndex: 25, display: "flex", alignItems: "center", justifyContent: "center", width: 32, height: 32, borderRadius: 6, cursor: "pointer", background: "#f3f4f6", border: "1px solid #d1d5db", color: "#6b7280" }}>
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <rect x="1" y="1" width="14" height="14" rx="2" stroke="currentColor" strokeWidth="1.5" />
        <line x1="10" y1="1" x2="10" y2="15" stroke="currentColor" strokeWidth="1.5" />
      </svg>
    </button>
  );
}

// ── main render ────────────────────────────────────────────────────────────

const PARAM_KEYS = [
  "diff", "metrics", "strata_col", "strata_pre_exposure", "period", "period_basis", "period_population",
  "horizon", "observation_end", "test", "correction", "correction_scope", "ci_level", "path_col",
];

function readParams(host: RenderContext["host"]): Record<string, any> {
  const out: Record<string, any> = {};
  for (const key of PARAM_KEYS) {
    const v = host.get(key);
    out[key] = key === "diff" || key === "metrics" ? parseJson<any>(v, key === "metrics" ? [] : null) : v;
  }
  out.ci_level = Number(out.ci_level) || 0.95;
  out.correction = out.correction || "holm";
  out.correction_scope = out.correction_scope || "all";
  out.test = out.test || "auto";
  out.period_basis = out.period_basis || "calendar";
  return out;
}

type Tab = "scorecard" | "strata" | "periods";

export function render({ host, el, isStatic = false }: RenderContext) {
  function App() {
    const [params, setParams] = React.useState(() => readParams(host));
    const [result, setResult] = React.useState<ABResult>(() => parseJson(host.get("result"), {}));
    const [isLoading, setIsLoading] = React.useState<boolean>(() => (host.get("is_loading") as boolean) ?? false);
    const [error, setError] = React.useState<string>(() => (host.get("error") as string) || "");
    const [height, setHeight] = React.useState<number>(() => (host.get("height") as number) || 560);
    const [sidebarOpen, setSidebarOpen] = React.useState<boolean>(() => (host.get("sidebar_open") as boolean) ?? true);
    const [metricsOpen, setMetricsOpen] = React.useState(false);
    const [tab, setTab] = React.useState<Tab>("scorecard");

    const events = parseJson<string[]>(host.get("event_list"), []);
    const pathCols = parseJson<string[]>(host.get("path_cols"), []);
    const segmentCols = parseJson<string[]>(host.get("segment_cols"), []);
    const segLevels = parseJson<Record<string, string[]>>(host.get("segment_levels"), {});
    const customCols = parseJson<Record<string, boolean>>(host.get("custom_cols"), {});
    const assignmentInfo = (host.get("assignment_info") as string) || "";

    const refreshParams = () => setParams(readParams(host));
    useHostSubscriptions(host, [
      ["result", () => setResult(parseJson(host.get("result"), {}))],
      ["is_loading", () => setIsLoading((host.get("is_loading") as boolean) ?? false)],
      ["error", () => setError((host.get("error") as string) || "")],
      ["height", () => setHeight((host.get("height") as number) || 560)],
      ["sidebar_open", () => setSidebarOpen((host.get("sidebar_open") as boolean) ?? true)],
      ...PARAM_KEYS.map(k => [k, refreshParams] as [string, () => void]),
    ]);

    // A local echo so a select shows the new value before Python answers.
    const echoHost = React.useMemo(() => ({
      ...host,
      set(key: string, value: unknown) {
        host.set(key, value);
        if (PARAM_KEYS.includes(key)) setParams(p => ({
          ...p, [key]: key === "diff" || key === "metrics" ? parseJson<any>(value, null) : value,
        }));
      },
    }), []);

    const hasStrata = !!(result.strata && result.strata.length);
    const hasPeriods = !!(result.periods && result.periods.length);
    const current: Tab = tab === "strata" && !hasStrata ? "scorecard" : tab === "periods" && !hasPeriods ? "scorecard" : tab;
    const overall = result.overall ?? [];
    const metrics: any[] = Array.isArray(params.metrics) ? params.metrics : [];
    const handleToggle = () => setSidebarOpen(p => { const n = !p; host.set("sidebar_open", n); return n; });

    const tabButton = (id: Tab, label: string) => (
      <button key={id} data-testid={`ab-tab-${id}`} onClick={() => setTab(id)} style={{
        padding: "6px 12px", fontSize: 12, fontWeight: 500, border: "none", background: "none", cursor: "pointer",
        color: current === id ? "#111827" : "#6b7280", borderBottom: `2px solid ${current === id ? "#111827" : "transparent"}`,
      }}>{label}</button>
    );

    return (
      <div style={{ position: "relative", display: "flex", flexDirection: "row", height, background: "#ffffff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
        <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0, display: "flex", flexDirection: "column" }}>
          <SidebarToggle onClick={handleToggle} />
          <div style={{ display: "flex", gap: 2, padding: "8px 56px 0 12px", borderBottom: "1px solid #e5e7eb", flexShrink: 0 }}>
            {tabButton("scorecard", "Scorecard")}
            {hasStrata && tabButton("strata", `Strata (${result.strata_col})`)}
            {hasPeriods && tabButton("periods", `Periods (${result.period})`)}
          </div>
          <div style={{ flex: 1, minHeight: 0, overflowY: "auto", padding: 12 }}>
            {error && !isLoading && (
              <div data-testid="ab-error" style={{ marginBottom: 12, padding: 12, background: "#fff1f2", border: "1px solid #fca5a5", borderRadius: 8, fontSize: 12, color: "#dc2626", whiteSpace: "pre-wrap" }}>{error}</div>
            )}
            {!!result.warnings?.length && (
              <div data-testid="ab-warnings" style={{ marginBottom: 12, padding: "8px 10px", background: "#fffbeb", border: "1px solid #fcd34d", borderRadius: 8, fontSize: 11, color: "#92400e", lineHeight: 1.5 }}>
                {result.warnings.map((w, i) => <div key={i}>⚠ {w}</div>)}
              </div>
            )}
            {overall.length === 0 && !isLoading && !error && (
              <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "80%", color: "#9ca3af", fontSize: 13 }}>
                {!params.diff ? "Choose the arms in the settings panel" : "Add metrics in the settings panel"}
              </div>
            )}
            {overall.length > 0 && current === "scorecard" && <ScorecardView result={result} />}
            {current === "strata" && hasStrata && <StrataView result={result} />}
            {current === "periods" && hasPeriods && <PeriodsView result={result} />}
          </div>
          {isLoading && <ComputingSpinner />}
        </div>
        {sidebarOpen && (
          <Sidebar host={echoHost} isStatic={isStatic} isLoading={isLoading} params={params}
            segLevels={segLevels} segmentCols={segmentCols} pathCols={pathCols} assignmentInfo={assignmentInfo}
            metricsCount={metrics.length} onOpenMetrics={() => setMetricsOpen(true)} />
        )}
        {!isStatic && metricsOpen && (
          <MetricsOverlay metrics={metrics} events={events} segmentCols={segmentCols} segmentLevels={segLevels}
            customCols={customCols} onApply={m => echoHost.set("metrics", JSON.stringify(m))}
            onClose={() => setMetricsOpen(false)} />
        )}
        <RetentioneeringSpinKeyframes />
      </div>
    );
  }

  const root = createRoot(el);
  root.render(<App />);
  return () => root.unmount();
}
