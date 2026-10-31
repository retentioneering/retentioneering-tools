/**
 * A/B compression widget (`ab_compression`): the plan of `get_ab_compression`
 * step by step (through the twin `ab_compression_data`). The path chart, the
 * step slider, the mapping table at the selected step and the numbers before and
 * after; the rules are in `ab_compression_encoding.ts`.
 *
 * Data parameters edited in the sidebar set one traitlet each and Python
 * recomputes; the view (the selected step, the table filter, the segment) lives
 * here and is written back so `state_file` keeps it. The payload holds every
 * step, so a static export keeps the slider.
 */
import * as React from "react";
import { createRoot } from "react-dom/client";
import {
  parseJson, ComputingSpinner, RetentioneeringSpinKeyframes, useHostSubscriptions, type RenderContext,
} from "./widget-utils";
import {
  type CompressionPayload, type Measure, type PathStep, type Segment,
  MEASURES, NEEDS, actionCounts, beforeAfter, fmtMeasure, fmtScale, fmtShare, hasNeeds, mappingAt, needGeometry,
  panelGeometry, parseCompression, stepOf, stepText,
} from "./ab_compression_encoding";

const DATA_KEYS = [
  "screen", "horizon", "path_col", "edge_weight", "max_steps", "edges", "min_path_share", "chain_threshold",
  "synonym_threshold", "bridge_threshold", "max_events", "target", "step", "keep", "other", "by",
  "by_pre_exposure", "correction", "ci_level", "power", "min_support", "target_delta",
];

const SIDEBAR_W = 264;

/** The named targets, as the sidebar and the header say them. */
const TARGET_TEXT: Record<string, string> = {
  untestable_share: "least data too thin to test",
  testable: "most testable cells",
  mde: "lowest median MDE",
};
const INK = "#111827";
const MUTED = "#6b7280";
const LINE = "#374151";
const ARTIFICIAL = "#dc2626";
const SELECTED = "#2563eb";

const sel: React.CSSProperties = {
  width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 6, color: INK,
  fontSize: 12, padding: "5px 8px", cursor: "pointer", outline: "none", background: "#f9fafb",
};
const SH = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: MUTED, margin: "0 0 6px" }}>{children}</div>
);
const Rule = () => <div style={{ height: 1, background: "#e5e7eb", margin: "14px 0" }} />;
const Hint = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 10, color: MUTED, marginTop: 4, lineHeight: 1.4 }}>{children}</div>
);
const Label = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 11, color: "#374151", margin: "8px 0 4px" }}>{children}</div>
);

export interface View {
  selected_step: number;
  changed_only: boolean;
  segment: string;
}

function readView(host: RenderContext["host"]): View {
  const step = Number(host.get("selected_step"));
  const changed = host.get("changed_only");
  return {
    selected_step: Number.isInteger(step) ? step : -1,
    changed_only: typeof changed === "boolean" ? changed : true,
    segment: (host.get("segment") as string) || "",
  };
}

function readParams(host: RenderContext["host"]): Record<string, any> {
  const out: Record<string, any> = {};
  for (const key of DATA_KEYS) out[key] = host.get(key);
  return out;
}

// ── the path chart ─────────────────────────────────────────────────────────

/** The y range of a panel, in the measure's units (the panel has no axis). */
function rangeText(min: number, max: number, measure: Measure, unit: "pp" | "per_path"): string {
  if (measure === "testable") return `${Math.round(min)}–${Math.round(max)}`;
  if (measure === "mass_retained") return `${fmtShare(min, 0)}–${fmtShare(max, 0)}`;
  if (measure === "untestable_share") return `${fmtShare(min, 3)}–${fmtShare(max, 3)}`;
  const f = (v: number) => (unit === "pp" ? (v * 100).toFixed(2) : v.toFixed(3));
  return `${f(min)}–${f(max)}${unit === "pp" ? " pp" : ""}`;
}

const PANEL_W = 300;
const PANEL_H = 64;

/** One panel: the measure along the path, the selected and the chosen step
 *  ringed, a step that creates artificial transitions ringed in red. Clicking a
 *  point selects its step. */
export function PathPanel({ path, measure, title, unit, selected, chosen, onSelect }: {
  path: PathStep[]; measure: Measure; title: string; unit: "pp" | "per_path"; selected: number; chosen: number;
  onSelect: (step: number) => void;
}) {
  const g = panelGeometry(path, measure, PANEL_W, PANEL_H, 8);
  const current = path[Math.min(selected, path.length - 1)];
  return (
    <div data-testid={`abc-panel-${measure}`} style={{ display: "flex", alignItems: "center", gap: 10 }}>
      <svg width={PANEL_W} height={PANEL_H} viewBox={`0 0 ${PANEL_W} ${PANEL_H}`} role="img" aria-label={`${title} along the compression path`}
        style={{ flexShrink: 0, overflow: "visible" }}>
        <rect x={0} y={0} width={PANEL_W} height={PANEL_H} fill="#f9fafb" stroke="#e5e7eb" />
        <path d={g.path} fill="none" stroke={LINE} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
        {g.points.map((p) => p.y == null ? null : (
          <g key={p.step} data-testid="abc-point" data-step={p.step} data-artificial={p.artificial ? "true" : "false"}
            onClick={() => onSelect(p.step)} style={{ cursor: "pointer" }}>
            <title>{`${stepText(path[p.step])} · ${title.toLowerCase()} ${fmtMeasure(path[p.step], measure, unit)}`}</title>
            <circle cx={p.x} cy={p.y} r={9} fill="transparent" />
            {p.artificial && <circle cx={p.x} cy={p.y} r={7} fill="none" stroke={ARTIFICIAL} strokeWidth={2} />}
            <circle cx={p.x} cy={p.y} r={p.step === selected ? 5 : 4} fill={p.step === selected ? SELECTED : LINE} stroke="#f9fafb" strokeWidth={2} />
            {p.step === chosen && <circle data-testid="abc-chosen" cx={p.x} cy={p.y} r={8} fill="none" stroke={INK} strokeWidth={1.25} strokeDasharray="2 2" />}
          </g>
        ))}
      </svg>
      <div style={{ fontSize: 11, color: "#374151", lineHeight: 1.4, minWidth: 0 }}>
        <div style={{ fontWeight: 600, color: INK }}>{title}</div>
        <div data-testid={`abc-value-${measure}`}>{current ? fmtMeasure(current, measure, unit) : "n/a"}</div>
        <div style={{ fontSize: 10, color: MUTED }}>{`path range ${rangeText(g.min, g.max, measure, unit)}`}</div>
      </div>
    </div>
  );
}

/** The users-needed panel (with `target_delta`): two series in one unit, a
 *  multiple of the current users on a log scale; the line at ×1 is the data as
 *  it is. A legend names the series (solid and dashed, not colour alone). */
export function NeedPanelView({ path, targetDelta, unit, selected, chosen, onSelect }: {
  path: PathStep[]; targetDelta: number | null; unit: "pp" | "per_path"; selected: number; chosen: number;
  onSelect: (step: number) => void;
}) {
  const g = needGeometry(path, PANEL_W, PANEL_H, 8);
  const current = path[Math.min(selected, path.length - 1)];
  const lo = Math.log10(g.min), hi = Math.log10(g.max);
  const yOne = PANEL_H - 8 - ((0 - lo) / (hi - lo)) * (PANEL_H - 16);
  const dash: Record<string, string | undefined> = { median_scale_needed: undefined, scale_to_test_90: "5 3" };
  const target = targetDelta == null ? "the target" : unit === "pp" ? `${(targetDelta * 100).toFixed(2)} pp` : String(targetDelta);
  return (
    <div data-testid="abc-panel-needed" style={{ display: "flex", alignItems: "center", gap: 10 }}>
      <svg width={PANEL_W} height={PANEL_H} viewBox={`0 0 ${PANEL_W} ${PANEL_H}`} role="img" aria-label="Users needed along the compression path"
        style={{ flexShrink: 0, overflow: "visible" }}>
        <rect x={0} y={0} width={PANEL_W} height={PANEL_H} fill="#f9fafb" stroke="#e5e7eb" />
        {yOne >= 0 && yOne <= PANEL_H && <line x1={0} x2={PANEL_W} y1={yOne} y2={yOne} stroke="#d1d5db" strokeDasharray="2 3" />}
        {NEEDS.map(({ key }) => (
          <path key={key} data-testid={`abc-need-${key}`} d={g.lines[key]} fill="none" stroke={LINE} strokeWidth={2}
            strokeDasharray={dash[key]} strokeLinejoin="round" strokeLinecap="round" />
        ))}
        {NEEDS.map(({ key, title }) => g.points[key].map((p) => p.y == null ? null : (
          <g key={`${key}-${p.step}`} data-testid="abc-need-point" data-series={key} data-step={p.step}
            onClick={() => onSelect(p.step)} style={{ cursor: "pointer" }}>
            <title>{`${stepText(path[p.step])} · users for ${title}: ${fmtScale(p.value)}`}</title>
            <circle cx={p.x} cy={p.y} r={9} fill="transparent" />
            {p.artificial && <circle cx={p.x} cy={p.y} r={7} fill="none" stroke={ARTIFICIAL} strokeWidth={2} />}
            <circle cx={p.x} cy={p.y} r={p.step === selected ? 5 : 3.5} fill={p.step === selected ? SELECTED : LINE} stroke="#f9fafb" strokeWidth={2} />
            {p.step === chosen && <circle cx={p.x} cy={p.y} r={8} fill="none" stroke={INK} strokeWidth={1.25} strokeDasharray="2 2" />}
          </g>
        )))}
      </svg>
      <div style={{ fontSize: 11, color: "#374151", lineHeight: 1.4, minWidth: 0 }}>
        <div style={{ fontWeight: 600, color: INK }}>Users needed (× now)</div>
        {NEEDS.map(({ key, title }) => (
          <div key={key} data-testid={`abc-value-${key}`} style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <svg width={18} height={6}><line x1={0} x2={18} y1={3} y2={3} stroke={LINE} strokeWidth={2} strokeDasharray={dash[key]} /></svg>
            <span>{current ? fmtScale(current[key]) : "n/a"}: {key === "median_scale_needed" ? `half of the testable cells detect ${target}` : title}</span>
          </div>
        ))}
        <div style={{ fontSize: 10, color: MUTED }}>{`path range ${fmtScale(g.min)}–${fmtScale(g.max)}, log scale; the dotted line is ×1`}</div>
      </div>
    </div>
  );
}

function PathChart({ segment, unit, targetDelta, step, onSelect }: {
  segment: Segment; unit: "pp" | "per_path"; targetDelta: number | null; step: number; onSelect: (s: number) => void;
}) {
  const vocab = segment.path.map((p) => p.vocabulary);
  const lo = Math.min(...vocab), hi = Math.max(...vocab);
  const anyArtificial = segment.path.some((p) => p.artificial_transitions > 0);
  return (
    <div data-testid="abc-chart" style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {MEASURES.map(({ key, title }) => (
        <PathPanel key={key} path={segment.path} measure={key} title={title} unit={unit} selected={step}
          chosen={segment.chosen_step} onSelect={onSelect} />
      ))}
      {hasNeeds(segment.path) && (
        <NeedPanelView path={segment.path} targetDelta={targetDelta} unit={unit} selected={step}
          chosen={segment.chosen_step} onSelect={onSelect} />
      )}
      <div style={{ width: PANEL_W, display: "flex", justifyContent: "space-between", fontSize: 10, color: MUTED }}>
        <span>{lo} events</span><span>vocabulary size →</span><span>{hi} events</span>
      </div>
      <div style={{ fontSize: 10, color: MUTED, display: "flex", flexWrap: "wrap", gap: 12 }}>
        <span><svg width={12} height={12}><circle cx={6} cy={6} r={4} fill={SELECTED} /></svg> selected step</span>
        <span><svg width={18} height={18}><circle cx={9} cy={9} r={7} fill="none" stroke={INK} strokeWidth={1.25} strokeDasharray="2 2" /></svg> chosen step</span>
        <span><svg width={18} height={18}><circle cx={9} cy={9} r={6} fill="none" stroke={ARTIFICIAL} strokeWidth={2} /></svg>
          creates artificial transitions{anyArtificial ? "" : " (none on this path)"}</span>
      </div>
    </div>
  );
}

// ── tables ─────────────────────────────────────────────────────────────────

const ACTION_STYLE: Record<string, { bg: string; fg: string }> = {
  keep: { bg: "#f3f4f6", fg: "#374151" },
  merge: { bg: "#e0e7ff", fg: "#3730a3" },
  drop: { bg: "#fee2e2", fg: "#991b1b" },
  other: { bg: "#fef3c7", fg: "#92400e" },
};

/** A merged label breaks after its joiners (`+`, `/`): a label of many events
 *  wraps in its column instead of pushing the reasons out of view. */
function Breakable({ label }: { label: string }) {
  const parts = label.split(/(?<=[+/])/);
  return <>{parts.map((part, i) => <React.Fragment key={i}>{part}{i < parts.length - 1 && <wbr />}</React.Fragment>)}</>;
}

function MappingTable({ segment, step, other, changedOnly }: { segment: Segment; step: number; other: string; changedOnly: boolean }) {
  const rows = mappingAt(segment, step, other, changedOnly);
  const th: React.CSSProperties = { textAlign: "left", fontWeight: 600, color: MUTED, fontSize: 10, padding: "4px 8px", borderBottom: "1px solid #e5e7eb", position: "sticky", top: 0, background: "#fff" };
  const td: React.CSSProperties = { padding: "4px 8px", borderBottom: "1px solid #f3f4f6", fontSize: 11, color: INK, verticalAlign: "top" };
  if (!rows.length) {
    return <div data-testid="abc-mapping-empty" style={{ padding: 12, fontSize: 12, color: MUTED }}>No event changes at step {step}: the vocabulary is the stream's.</div>;
  }
  return (
    <table data-testid="abc-mapping" style={{ borderCollapse: "collapse", width: "100%", fontFamily: "system-ui,-apple-system,sans-serif" }}>
      <thead>
        <tr>
          <th style={th}>event</th><th style={th}>paths</th><th style={th}>action</th><th style={th}>new label</th>
          <th style={th}>step</th><th style={th}>reason</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.event} data-testid="abc-mapping-row" data-event={r.event} data-action={r.action}>
            <td style={{ ...td, fontWeight: 500, whiteSpace: "nowrap" }}>{r.event}</td>
            <td style={{ ...td, whiteSpace: "nowrap", fontVariantNumeric: "tabular-nums" }}>{fmtShare(r.pathShare)}</td>
            <td style={td}>
              <span style={{ display: "inline-block", padding: "0 6px", borderRadius: 4, fontSize: 10, background: ACTION_STYLE[r.action].bg, color: ACTION_STYLE[r.action].fg }}>{r.action}</span>
            </td>
            <td style={{ ...td, minWidth: 180, maxWidth: 320 }}>{r.label === null ? <span style={{ color: MUTED }}>(dropped)</span> : <Breakable label={r.label} />}</td>
            <td style={{ ...td, fontVariantNumeric: "tabular-nums" }}>{r.since || ""}</td>
            <td style={{ ...td, color: "#374151", lineHeight: 1.4, minWidth: 240, overflowWrap: "anywhere" }}>{r.reason}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function BeforeAfter({ segment, step, unit }: { segment: Segment; step: number; unit: "pp" | "per_path" }) {
  const rows = beforeAfter(segment.path, step, unit);
  const td: React.CSSProperties = { padding: "2px 8px", fontSize: 11, color: INK, fontVariantNumeric: "tabular-nums", textAlign: "right", whiteSpace: "nowrap" };
  return (
    <table data-testid="abc-before-after" style={{ borderCollapse: "collapse" }}>
      <thead>
        <tr>
          <th style={{ ...td, textAlign: "left", color: MUTED, fontWeight: 600, fontSize: 10 }} />
          <th style={{ ...td, color: MUTED, fontWeight: 600, fontSize: 10 }}>before (step 0)</th>
          <th style={{ ...td, color: MUTED, fontWeight: 600, fontSize: 10 }}>{`after (step ${step})`}</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(([name, a, b]) => (
          <tr key={name} data-testid="abc-count" data-name={name}>
            <td style={{ ...td, textAlign: "left", color: "#374151" }}>{name}</td>
            <td style={td}>{a}</td>
            <td style={{ ...td, fontWeight: a !== b ? 600 : 400 }}>{b}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ── header ─────────────────────────────────────────────────────────────────

function homogeneityText(h: Record<string, unknown> | null): string | null {
  if (!h) return null;
  const n = (k: string, d = 2) => (typeof h[k] === "number" ? (h[k] as number).toFixed(d) : "n/a");
  return `effective vocabulary ${n("effective_vocabulary", 1)} of ${h.events ?? "?"} events · transition entropy ${n("transition_entropy")} bits · distinct-path share ${typeof h.distinct_path_share === "number" ? fmtShare(h.distinct_path_share as number) : "n/a"} · ${h.paths ?? "?"} paths`;
}

export function Header({ payload, segment }: { payload: CompressionPayload; segment: Segment }) {
  const screen = payload.screen === "steps"
    ? `step screen, steps 1–${payload.max_steps}`
    : `transition screen, ${payload.edge_weight}`;
  const target = typeof payload.target === "number" ? `median MDE ≤ ${payload.target}` : (TARGET_TEXT[String(payload.target)] ?? String(payload.target));
  const who = payload.population === "assignment" ? "ITT: every path of the assignment table" : "the stream's paths";
  const h = homogeneityText(segment.homogeneity);
  return (
    <div data-testid="abc-header" style={{ fontSize: 12, color: INK, lineHeight: 1.5 }}>
      <div><b>Vocabulary compression</b><span style={{ color: MUTED }}> · {screen} · {who}{payload.horizon ? `, horizon ${payload.horizon}` : ""} · target {target} · chosen step {segment.chosen_step} of {segment.path.length - 1}</span></div>
      <div style={{ fontSize: 11, color: MUTED }}>
        Arm-blind: every number pools both arms ({payload.group1_paths ?? "?"} + {payload.group2_paths ?? "?"} paths); the arm of a path is never read. Testable cells and MDE as the screen counts them (min_support {payload.min_support}, MDE at alpha / m).
      </div>
      {h && <div data-testid="abc-homogeneity" style={{ fontSize: 11, color: "#374151" }}>{payload.by ? `${payload.by} = ${segment.key}: ` : ""}{h}</div>}
    </div>
  );
}

// ── sidebar ────────────────────────────────────────────────────────────────

function NumberField({ label, value, disabled, onCommit, percent = false, integer = false, emptyIsNone = false, min = 0, max = Infinity }: {
  label: string; value: number | null; disabled: boolean; onCommit: (v: number | null) => void;
  percent?: boolean; integer?: boolean; emptyIsNone?: boolean; min?: number; max?: number;
}) {
  const shown = value == null || (emptyIsNone && !value) ? "" : String(+(percent ? value * 100 : value).toPrecision(6));
  const [text, setText] = React.useState(shown);
  React.useEffect(() => setText(shown), [shown]);
  const n = Number(text);
  const empty = text.trim() === "";
  const scaled = percent ? n / 100 : n;
  const valid = (empty && emptyIsNone) || (!empty && Number.isFinite(n) && (!integer || Number.isInteger(n)) && scaled > min && scaled <= max);
  const commit = () => {
    if (!valid) return;
    const next = empty ? null : scaled;
    if (next !== value) onCommit(next);
  };
  return (
    <input aria-label={label} value={text} disabled={disabled} placeholder={emptyIsNone ? "none" : undefined}
      onChange={(e) => setText(e.target.value)} onBlur={commit} onKeyDown={(e) => { if (e.key === "Enter") commit(); }}
      style={{ width: "100%", boxSizing: "border-box", border: `1px solid ${valid ? "#d1d5db" : "#fca5a5"}`, borderRadius: 6, padding: "5px 8px", fontSize: 12, outline: "none", background: disabled ? "#f9fafb" : "#fff" }} />
  );
}

function fixedText(key: string, value: unknown): string {
  if (key === "keep" || key === "edges") {
    const list = parseJson<unknown[]>(value, []);
    return Array.isArray(list) && list.length ? (key === "keep" ? list.join(", ") : `${list.length} declared`) : "none";
  }
  if (key === "horizon") return value ? String(value) : "none (whole window)";
  if (key === "path_col") return value ? String(value) : "schema default";
  if (key === "step") return Number(value) >= 0 ? String(value) : "by target";
  if (key === "max_steps") return Number(value) > 0 ? String(value) : "default";
  if (key === "by_pre_exposure") return value === "true" ? "declared" : value === "false" ? "no" : "from the recipe";
  if (value === null || value === undefined || value === "") return "default";
  return String(value);
}

function Sidebar({ host, isStatic, isLoading, params, view, setParam, setView, payload }: {
  host: RenderContext["host"]; isStatic: boolean; isLoading: boolean; params: Record<string, any>; view: View;
  setParam: (k: string, v: unknown) => void; setView: <K extends keyof View>(k: K, v: View[K]) => void;
  payload: CompressionPayload | null;
}) {
  const disabled = isLoading || isStatic;
  const diff = parseJson<unknown>(host.get("diff"), null);
  const diffText = Array.isArray(diff) && diff.length === 3 ? `${diff[0]}: ${diff[1]} vs ${diff[2]}` : String(diff ?? "");
  const assignment = (host.get("assignment_info") as string) || "";
  const target = String(params.target ?? "untestable_share");
  const named = Object.keys(TARGET_TEXT);
  const targets = [...named, ...(named.includes(target) ? [] : [target])];
  const fixedKeys = [
    "horizon", "path_col", params.screen === "steps" ? "max_steps" : "edge_weight", ...(params.screen === "steps" ? [] : ["edges"]),
    "synonym_threshold", "bridge_threshold", "keep", "other", "by", "by_pre_exposure", "step", "correction", "ci_level", "power", "min_support",
  ];
  const fixed: [string, string][] = [
    ["diff", diffText],
    ["assignment", assignment ? "table (ITT)" : "none: the stream's paths"],
    ...fixedKeys.map((k) => [k, fixedText(k, params[k])] as [string, string]),
  ];
  return (
    <div data-testid="abc-sidebar" style={{ width: SIDEBAR_W, minWidth: SIDEBAR_W, height: "100%", background: "#fff", borderLeft: "1px solid #e5e7eb", display: "flex", flexDirection: "column", overflow: "hidden", fontFamily: "system-ui, sans-serif" }}>
      <div style={{ padding: "10px 14px", borderBottom: "1px solid #e5e7eb", display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: INK }}>Settings</span>
        {isLoading && <span style={{ fontSize: 11, color: MUTED, marginRight: 34 }}>Computing…</span>}
      </div>
      <div style={{ flex: 1, overflowY: "auto", padding: 14 }}>
        <SH>Plan (recomputes)</SH>
        <Label>Screen</Label>
        <select aria-label="Screen" value={params.screen} disabled={disabled} style={sel} onChange={(e) => setParam("screen", e.target.value)}>
          <option value="transitions">Transitions (no artificial transitions)</option>
          <option value="steps">Steps (aligned steps)</option>
        </select>
        <Label>Rare below (% of paths)</Label>
        <NumberField label="Min path share" value={Number(params.min_path_share)} percent disabled={disabled} max={1}
          onCommit={(v) => setParam("min_path_share", v)} />
        <Label>Chain threshold</Label>
        <NumberField label="Chain threshold" value={Number(params.chain_threshold)} disabled={disabled} max={1}
          onCommit={(v) => setParam("chain_threshold", v)} />
        <Label>Max events</Label>
        <NumberField label="Max events" value={Number(params.max_events) || null} integer emptyIsNone disabled={disabled}
          onCommit={(v) => setParam("max_events", v ?? 0)} />
        <Label>Chosen step by</Label>
        <select aria-label="Target" value={target} disabled={disabled} style={sel} onChange={(e) => setParam("target", e.target.value)}>
          {targets.map((t) => (
            <option key={t} value={t}>{TARGET_TEXT[t] ?? `median MDE ≤ ${t}`}</option>
          ))}
        </select>
        <Hint>{target === "untestable_share"
          ? "The earliest step whose observations in cells too thin to test are near their lowest, without artificial transitions: the information, not the number of cells."
          : target === "testable" ? "The step with the most testable cells: a merged chain counts as a lost cell." : "The step with the lowest median MDE of the testable cells."}</Hint>
        <Label>Target effect (users needed)</Label>
        <NumberField label="Target effect" value={Number(params.target_delta) || null} percent={payload?.unit !== "per_path"} emptyIsNone disabled={disabled}
          onCommit={(v) => setParam("target_delta", v ?? 0)} />
        <Hint>{payload?.unit === "per_path" ? "Per path, absolute" : "Percentage points, absolute"}: adds the users-needed panel.</Hint>
        {isStatic && <Hint>A static export has no kernel: these are the values it was computed with.</Hint>}

        <Rule />
        <SH>View</SH>
        {payload && payload.segments.length > 1 && (
          <>
            <Label>Segment ({payload.by})</Label>
            <select aria-label="Segment" value={view.segment || payload.segments[0].key} style={sel} onChange={(e) => setView("segment", e.target.value)}>
              {payload.segments.map((s) => <option key={s.key} value={s.key}>{s.key}</option>)}
            </select>
          </>
        )}
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#374151", marginTop: 8 }}>
          <input type="checkbox" aria-label="Changed events only" checked={view.changed_only} onChange={(e) => setView("changed_only", e.target.checked)} />
          List only the events that change
        </label>

        <Rule />
        <SH>Fixed at construction</SH>
        <div data-testid="abc-fixed" style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          {fixed.map(([k, v]) => (
            <div key={k} style={{ display: "flex", justifyContent: "space-between", gap: 8, fontSize: 11 }}>
              <span style={{ color: MUTED, fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" }}>{k}</span>
              <span style={{ color: INK, textAlign: "right", overflowWrap: "anywhere" }}>{v}</span>
            </div>
          ))}
        </div>
        <Hint>Set in code: pass them again to change them.</Hint>
      </div>
    </div>
  );
}

function SidebarToggle({ onClick }: { onClick: () => void }) {
  return (
    <button onClick={onClick} title="Toggle settings" aria-label="Toggle settings sidebar" style={{ position: "absolute", top: 8, right: 12, zIndex: 25, display: "flex", alignItems: "center", justifyContent: "center", width: 30, height: 30, borderRadius: 6, cursor: "pointer", background: "#f3f4f6", border: "1px solid #d1d5db", color: MUTED }}>
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <rect x="1" y="1" width="14" height="14" rx="2" stroke="currentColor" strokeWidth="1.5" />
        <line x1="10" y1="1" x2="10" y2="15" stroke="currentColor" strokeWidth="1.5" />
      </svg>
    </button>
  );
}

// ── main render ────────────────────────────────────────────────────────────

/** The body for one segment: the chart, the slider, the counts and the table. */
export function CompressionBody({ payload, segment, view, onStep }: {
  payload: CompressionPayload; segment: Segment; view: View; onStep: (s: number) => void;
}) {
  const step = stepOf(segment, view.selected_step);
  const last = segment.path.length - 1;
  const current = segment.path[step];
  const counts = actionCounts(segment, step, payload.other);
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 24, alignItems: "flex-start" }}>
        <PathChart segment={segment} unit={payload.unit} targetDelta={payload.target_delta ?? null} step={step} onSelect={onStep} />
        <div style={{ display: "flex", flexDirection: "column", gap: 8, minWidth: 260 }}>
          <BeforeAfter segment={segment} step={step} unit={payload.unit} />
          <div data-testid="abc-actions" style={{ fontSize: 11, color: "#374151" }}>
            At step {step}: {counts.keep} kept, {counts.merge} merged, {counts.drop} dropped, {counts.other} relabelled {payload.other}.
          </div>
        </div>
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <span style={{ fontSize: 11, color: "#374151", whiteSpace: "nowrap" }}>Step</span>
        <input type="range" aria-label="Step" data-testid="abc-slider" min={0} max={last} step={1} value={step}
          onChange={(e) => onStep(Number(e.target.value))} style={{ flex: 1, maxWidth: 420 }} disabled={last === 0} />
        <span data-testid="abc-step" style={{ fontSize: 12, fontWeight: 600, color: INK, fontVariantNumeric: "tabular-nums" }}>{step} / {last}</span>
        <button data-testid="abc-to-chosen" onClick={() => onStep(-1)} disabled={step === segment.chosen_step && view.selected_step < 0}
          style={{ border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", fontSize: 11, padding: "2px 8px", cursor: "pointer" }}>
          chosen ({segment.chosen_step})
        </button>
      </div>
      <div data-testid="abc-step-text" style={{ fontSize: 11, color: "#374151", lineHeight: 1.45 }}>
        <b>{current ? stepText(current) : ""}</b>{current && current.step > 0 ? <> — {current.reason}</> : null}
        <div style={{ color: MUTED }}>Apply this plan: <code>stream.compress_events(stream.ab_compression_data(..., step={step}))</code></div>
      </div>
      <MappingTable segment={segment} step={step} other={payload.other} changedOnly={view.changed_only} />
    </div>
  );
}

const VIEW_TRAITS: Record<keyof View, string> = {
  selected_step: "selected_step",
  changed_only: "changed_only",
  segment: "segment",
};

export function render({ host, el, isStatic = false }: RenderContext) {
  function App() {
    const [payload, setPayload] = React.useState<CompressionPayload | null>(() => parseCompression(host.get("result")));
    const [isLoading, setIsLoading] = React.useState<boolean>(() => (host.get("is_loading") as boolean) ?? false);
    const [error, setError] = React.useState<string>(() => (host.get("error") as string) || "");
    const [height, setHeight] = React.useState<number>(() => (host.get("height") as number) || 640);
    const [sidebarOpen, setSidebarOpen] = React.useState<boolean>(() => (host.get("sidebar_open") as boolean) ?? true);
    const [params, setParams] = React.useState(() => readParams(host));
    const [view, setViewState] = React.useState<View>(() => readView(host));

    useHostSubscriptions(host, [
      ["result", () => setPayload(parseCompression(host.get("result")))],
      ["is_loading", () => setIsLoading((host.get("is_loading") as boolean) ?? false)],
      ["error", () => setError((host.get("error") as string) || "")],
      ["height", () => setHeight((host.get("height") as number) || 640)],
      ["sidebar_open", () => setSidebarOpen((host.get("sidebar_open") as boolean) ?? true)],
      ...DATA_KEYS.map((k) => [k, () => setParams(readParams(host))] as [string, () => void]),
    ]);

    const setParam = React.useCallback((key: string, value: unknown) => {
      setParams((p) => ({ ...p, [key]: value }));
      host.set(key, value);
      // a new plan: the slider goes back to its chosen step
      setViewState((v) => ({ ...v, selected_step: -1 }));
      host.set("selected_step", -1);
    }, []);
    const setView = React.useCallback(<K extends keyof View>(key: K, value: View[K]) => {
      setViewState((v) => ({ ...v, [key]: value }));
      host.set(VIEW_TRAITS[key], value);
    }, []);

    const segment = payload
      ? payload.segments.find((s) => s.key === view.segment) ?? payload.segments[0]
      : null;
    const handleToggle = () => setSidebarOpen((p) => { const n = !p; host.set("sidebar_open", n); return n; });

    return (
      <div data-testid="ab-compression-widget" style={{ position: "relative", display: "flex", flexDirection: "row", height, background: "#ffffff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
        <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0, display: "flex", flexDirection: "column" }}>
          <SidebarToggle onClick={handleToggle} />
          <div style={{ padding: "10px 52px 8px 12px", borderBottom: "1px solid #e5e7eb", flexShrink: 0 }}>
            {payload && segment && <Header payload={payload} segment={segment} />}
            {error && !isLoading && (
              <div data-testid="abc-error" style={{ marginTop: 6, padding: 10, background: "#fff1f2", border: "1px solid #fca5a5", borderRadius: 8, fontSize: 12, color: "#dc2626", whiteSpace: "pre-wrap" }}>{error}</div>
            )}
            {!!payload?.warnings?.length && (
              <div data-testid="abc-warnings" style={{ marginTop: 6, padding: "6px 8px", background: "#fffbeb", border: "1px solid #fcd34d", borderRadius: 8, fontSize: 11, color: "#92400e", lineHeight: 1.5 }}>
                {payload.warnings.map((w, i) => <div key={i}>⚠ {w}</div>)}
              </div>
            )}
          </div>
          <div data-testid="abc-scroll" style={{ flex: 1, minHeight: 0, overflow: "auto", padding: 12 }}>
            {payload && segment && (
              <CompressionBody payload={payload} segment={segment} view={view} onStep={(s) => setView("selected_step", s)} />
            )}
            {!payload && !isLoading && !error && (
              <div style={{ padding: 24, color: "#9ca3af", fontSize: 13, textAlign: "center" }}>No data</div>
            )}
          </div>
          {isLoading && <ComputingSpinner />}
        </div>
        {sidebarOpen && (
          <Sidebar host={host} isStatic={isStatic} isLoading={isLoading} params={params} view={view}
            setParam={setParam} setView={setView} payload={payload} />
        )}
        <RetentioneeringSpinKeyframes />
      </div>
    );
  }

  const root = createRoot(el);
  root.render(<App />);
  return () => root.unmount();
}
