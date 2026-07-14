import * as React from "react";
import { createRoot } from "react-dom/client";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes, useHostSubscriptions, type RenderContext } from "./widget-utils";

// ── constants ──────────────────────────────────────────────────────────────

const STEP_COLORS = ["#8b5cf6","#10b981","#0ea5e9","#84cc16","#f59e0b","#ef4444","#ec4899"];
const DIFF_A = "#ef4444";
const DIFF_B = "#3b82f6";

/** Diff-mode sentinel meaning "every other value of this segment column". */
const REST_VALUE = "<REST>";
const REST_LABEL = "Rest (everyone else)";

/** get_segment_levels' sentinel for paths with no value assigned for this segment column. */
const MISSING_VALUE = "<MISSING>";
const MISSING_LABEL = "No segment value";

function segmentValueLabel(v: string): string {
  return v === MISSING_VALUE ? MISSING_LABEL : v;
}

// Estimated heights for auto-sizing
const HEADER_H     = 48;  // sidebar header
const SIDEBAR_W    = 252;
const STEP_ROW_H   = 34;  // per step row
const ADD_BTN_H    = 34;
const DIFF_SECTION = 160; // diff selector
const CHART_MIN_H  = 200;
const TABLE_ROW_H  = 30;
const TABLE_HEAD_H = 34;
const PADDING      = 32;

function autoHeight(nSteps: number, nStepRows: number, hasDiff: boolean): number {
  const sidebarContent =
    HEADER_H + PADDING +
    nSteps * STEP_ROW_H + ADD_BTN_H + 20 +
    DIFF_SECTION;

  const chartH = CHART_MIN_H + 60; // chart + labels
  const tableH = TABLE_HEAD_H + nStepRows * TABLE_ROW_H + 16;
  const mainContent = HEADER_H + PADDING + chartH + tableH;

  return Math.max(sidebarContent, mainContent) + 24;
}

// ── colour helpers ─────────────────────────────────────────────────────────

function heatmapColor(v: number, absMax: number): string {
  if (v === 0 || absMax === 0) return "rgba(229,231,235,0.4)";
  const t = Math.min(Math.abs(v) / absMax, 1);
  return v < 0
    ? `rgba(59,130,246,${0.15 + t * 0.65})`
    : `rgba(239,68,68,${0.15 + t * 0.65})`;
}

// ── types ──────────────────────────────────────────────────────────────────

interface FunnelStep {
  step: string;
  unique_paths?: number;
  conversion_rate?: number;
  step_conversion_rate?: number;
  funnel1_unique_paths?: number;
  funnel1_conversion_rate?: number;
  funnel1_step_conversion_rate?: number;
  funnel2_unique_paths?: number;
  funnel2_conversion_rate?: number;
  funnel2_step_conversion_rate?: number;
  delta_unique_paths?: number;
  delta_conversion_rate?: number;
  delta_step_conversion_rate?: number;
}

// ── bar chart ──────────────────────────────────────────────────────────────

function FunnelChart({ steps, hasDiff, label1, label2, chartH }: {
  steps: FunnelStep[];
  hasDiff: boolean;
  label1: string;
  label2: string;
  chartH: number;
}) {
  const BAR_GROUP = hasDiff ? 74 : 48;
  const BAR_W     = hasDiff ? 20 : 34;
  const GAP       = hasDiff ? 10 : 0;
  const LABEL_H   = 52;
  const AXIS_W    = 36;
  const PLOT_H    = chartH - LABEL_H - 24;
  const totalW    = steps.length * BAR_GROUP + AXIS_W + 8;

  // Scale to max rate (not 100%)
  const maxRate = Math.max(
    0.01,
    ...steps.map(s =>
      hasDiff
        ? Math.max(s.funnel1_conversion_rate ?? 0, s.funnel2_conversion_rate ?? 0)
        : (s.conversion_rate ?? 0)
    ),
  ) * 100;

  // Nice ceiling for grid: round up to nearest 10 or 5
  const gridMax = maxRate <= 5  ? 5
    : maxRate <= 10 ? 10
    : maxRate <= 25 ? 25
    : maxRate <= 50 ? 50
    : maxRate <= 75 ? 75 : 100;

  const gridLines = [0, gridMax * 0.25, gridMax * 0.5, gridMax * 0.75, gridMax];

  return (
    <div style={{ overflowX: "auto" }}>
      <svg width={Math.max(totalW, 300)} height={chartH} style={{ display: "block" }}>
        {/* Grid lines */}
        {gridLines.map(v => {
          const y = 8 + PLOT_H * (1 - v / gridMax);
          return (
            <g key={v}>
              <line x1={AXIS_W} x2={totalW} y1={y} y2={y} stroke="#e5e7eb" strokeWidth={1} />
              <text x={AXIS_W - 4} y={y + 4} textAnchor="end" fontSize={9} fill="#9ca3af">
                {v.toFixed(v % 1 === 0 ? 0 : 1)}%
              </text>
            </g>
          );
        })}

        {steps.map((s, i) => {
          const cx = AXIS_W + 4 + i * BAR_GROUP + BAR_GROUP / 2;
          const yBase = 8 + PLOT_H;

          const rateA = hasDiff
            ? (s.funnel1_conversion_rate ?? 0) * 100
            : (s.conversion_rate ?? 0) * 100;
          const rateB = hasDiff ? (s.funnel2_conversion_rate ?? 0) * 100 : 0;
          const hA = Math.max(2, (rateA / gridMax) * PLOT_H);
          const hB = Math.max(2, (rateB / gridMax) * PLOT_H);

          const color = hasDiff ? DIFF_A : STEP_COLORS[i % STEP_COLORS.length];

          return (
            <g key={s.step}>
              {/* Bar A */}
              <rect
                x={cx - (hasDiff ? BAR_W + GAP / 2 : BAR_W / 2)}
                y={yBase - hA} width={BAR_W} height={hA}
                fill={color} rx={3} opacity={0.9}
              />
              {hasDiff && (
                <rect
                  x={cx + GAP / 2} y={yBase - hB}
                  width={BAR_W} height={hB}
                  fill={DIFF_B} rx={3} opacity={0.9}
                />
              )}
              {/* Rate labels */}
              <text
                x={cx - (hasDiff ? BAR_W / 2 + GAP / 2 : 0)}
                y={yBase - hA - 4}
                textAnchor="middle" fontSize={9} fill="#374151" fontWeight={600}
              >
                {rateA.toFixed(1)}%
              </text>
              {hasDiff && (
                <text
                  x={cx + BAR_W / 2 + GAP / 2} y={yBase - hB - 4}
                  textAnchor="middle" fontSize={9} fill="#374151" fontWeight={600}
                >
                  {rateB.toFixed(1)}%
                </text>
              )}
              {/* Step label (rotated) */}
              <text
                x={cx} y={yBase + 14}
                textAnchor="end" fontSize={10} fill="#6b7280"
                transform={`rotate(-40, ${cx}, ${yBase + 14})`}
              >
                {s.step.length > 16 ? s.step.slice(0, 15) + "…" : s.step}
              </text>
            </g>
          );
        })}
      </svg>

      {hasDiff && (
        <div style={{ display: "flex", gap: 16, paddingLeft: AXIS_W, paddingBottom: 4, fontSize: 11, color: "#111827" }}>
          <span>
            <span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: DIFF_A, marginRight: 4 }} />
            {label1}
          </span>
          <span>
            <span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: DIFF_B, marginRight: 4 }} />
            {label2}
          </span>
        </div>
      )}
    </div>
  );
}

// ── small colour dot ──────────────────────────────────────────────────────

const Dot = ({ color }: { color: string }) => (
  <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: color, marginLeft: 4, verticalAlign: "middle" }} />
);

// ── data table ─────────────────────────────────────────────────────────────

function FunnelTable({ steps, hasDiff, label1, label2, result }: {
  steps: FunnelStep[];
  hasDiff: boolean;
  label1: string;
  label2: string;
  result: any;
}) {
  const pctDelta = (s: FunnelStep) => {
    const r1 = s.funnel1_conversion_rate ?? 0;
    const r2 = s.funnel2_conversion_rate ?? 0;
    return r1 === 0 ? 0 : ((r1 - r2) / r1) * 100;
  };
  const pctStepDelta = (s: FunnelStep) => {
    const r1 = s.funnel1_step_conversion_rate ?? 0;
    const r2 = s.funnel2_step_conversion_rate ?? 0;
    return r1 === 0 ? 0 : ((r1 - r2) / r1) * 100;
  };
  const absMaxDelta = hasDiff
    ? Math.max(0.001, ...steps.map(s => Math.abs(pctDelta(s))))
    : 0;
  const absMaxStepDelta = hasDiff
    ? Math.max(0.001, ...steps.map(s => Math.abs(pctStepDelta(s))))
    : 0;

  const th: React.CSSProperties = { padding: "6px 10px", fontSize: 11, fontWeight: 600, color: "#6b7280", textAlign: "right", borderBottom: "1px solid #e5e7eb", whiteSpace: "nowrap" };
  const td: React.CSSProperties = { padding: "5px 10px", fontSize: 12, textAlign: "right", color: "#111827", borderBottom: "1px solid #f3f4f6" };
  const tdl: React.CSSProperties = { ...td, textAlign: "left", fontWeight: 500 };

  const renderDeltaCell = (relPct: number, ppVal: number, absMax: number) => {
    const sign = relPct >= 0 ? "+" : "";
    return (
      <td style={{ ...td, background: heatmapColor(relPct, absMax), fontWeight: 600, color: "#111827" }}>
        {sign}{relPct.toFixed(1)}%{" "}
        <span style={{ fontWeight: 400, fontSize: 10, color: "#374151", opacity: 0.75 }}>
          ({ppVal >= 0 ? "+" : ""}{ppVal.toFixed(1)} pp)
        </span>
      </td>
    );
  };

  return (
    <div style={{ overflowX: "auto", marginTop: 8 }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th style={{ ...th, textAlign: "left" }}>Step</th>
            {hasDiff ? (
              <>
                <th style={th}>Paths <Dot color={DIFF_A} /></th>
                <th style={th}>Paths <Dot color={DIFF_B} /></th>
                <th style={th}>Overall <Dot color={DIFF_A} /></th>
                <th style={th}>Overall <Dot color={DIFF_B} /></th>
                <th style={th}>Step % <Dot color={DIFF_A} /></th>
                <th style={th}>Step % <Dot color={DIFF_B} /></th>
                <th style={th}>Δ Overall (pp)</th>
                <th style={th}>Δ Step (pp)</th>
              </>
            ) : (
              <>
                <th style={th}>Unique Paths</th>
                <th style={th}>Overall %</th>
                <th style={th}>Step %</th>
              </>
            )}
          </tr>
        </thead>
        <tbody>
          {/* Total row */}
          {hasDiff ? (
            <tr style={{ background: "#f9fafb", borderBottom: "2px solid #e5e7eb" }}>
              <td style={{ ...tdl, color: "#6b7280", fontStyle: "italic" }}>total paths</td>
              <td style={{ ...td, color: "#6b7280" }}>{(result?.group1_total ?? 0).toLocaleString()}</td>
              <td style={{ ...td, color: "#6b7280" }}>{(result?.group2_total ?? 0).toLocaleString()}</td>
              <td colSpan={6} />
            </tr>
          ) : (
            <tr style={{ background: "#f9fafb", borderBottom: "2px solid #e5e7eb" }}>
              <td style={{ ...tdl, color: "#6b7280", fontStyle: "italic" }}>total paths</td>
              <td style={{ ...td, color: "#6b7280" }}>{(result?.total_paths ?? 0).toLocaleString()}</td>
              <td colSpan={2} />
            </tr>
          )}
          {steps.map((s, i) => (
            <tr key={s.step} style={{ background: i % 2 === 0 ? "#fff" : "#fafafa" }}>
              <td style={tdl}>{s.step}</td>
              {hasDiff ? (
                <>
                  <td style={td}>{(s.funnel1_unique_paths ?? 0).toLocaleString()}</td>
                  <td style={td}>{(s.funnel2_unique_paths ?? 0).toLocaleString()}</td>
                  <td style={td}>{((s.funnel1_conversion_rate ?? 0) * 100).toFixed(1)}%</td>
                  <td style={td}>{((s.funnel2_conversion_rate ?? 0) * 100).toFixed(1)}%</td>
                  <td style={td}>{((s.funnel1_step_conversion_rate ?? 0) * 100).toFixed(1)}%</td>
                  <td style={td}>{((s.funnel2_step_conversion_rate ?? 0) * 100).toFixed(1)}%</td>
                  {renderDeltaCell(pctDelta(s), (s.delta_conversion_rate ?? 0) * 100, absMaxDelta)}
                  {renderDeltaCell(pctStepDelta(s), (s.delta_step_conversion_rate ?? 0) * 100, absMaxStepDelta)}
                </>
              ) : (
                <>
                  <td style={td}>{(s.unique_paths ?? 0).toLocaleString()}</td>
                  <td style={td}>{((s.conversion_rate ?? 0) * 100).toFixed(1)}%</td>
                  <td style={td}>{((s.step_conversion_rate ?? 0) * 100).toFixed(1)}%</td>
                </>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── step editor (drag-and-drop + popover add) ──────────────────────────────

function StepEditor({ steps, events, onStepsChange }: {
  steps: string[];
  events: string[];
  onStepsChange: (s: string[]) => void;
}) {
  const [addOpen, setAddOpen] = React.useState(false);
  const [query, setQuery]     = React.useState("");
  const [dragIdx, setDragIdx] = React.useState<number | null>(null);
  const [overIdx, setOverIdx] = React.useState<number | null>(null);
  const inputRef = React.useRef<HTMLInputElement>(null);

  const available = events.filter(
    e => !steps.includes(e) && (!query || e.toLowerCase().includes(query.toLowerCase()))
  );

  const addStep = (e: string) => {
    onStepsChange([...steps, e]);
    setQuery("");
    setAddOpen(false);
  };

  // Drag handlers (HTML5 drag API — no external lib needed)
  const onDragStart = (i: number) => setDragIdx(i);
  const onDragOver  = (i: number) => (e: React.DragEvent) => {
    e.preventDefault();
    setOverIdx(i);
  };
  const onDrop = (toIdx: number) => () => {
    if (dragIdx === null || dragIdx === toIdx) { setDragIdx(null); setOverIdx(null); return; }
    const next = [...steps];
    const [moved] = next.splice(dragIdx, 1);
    next.splice(toIdx, 0, moved);
    onStepsChange(next);
    setDragIdx(null);
    setOverIdx(null);
  };
  const onDragEnd = () => { setDragIdx(null); setOverIdx(null); };

  React.useEffect(() => {
    if (addOpen) setTimeout(() => inputRef.current?.focus(), 40);
  }, [addOpen]);

  // Close on outside click
  const popRef = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    if (!addOpen) return;
    const handler = (e: MouseEvent) => {
      if (popRef.current && !popRef.current.contains(e.target as Node)) setAddOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [addOpen]);

  const row: React.CSSProperties = {
    display: "flex", alignItems: "center", gap: 6,
    padding: "5px 8px", border: "1px solid #e5e7eb", borderRadius: 6,
    marginBottom: 4, background: "#fff", cursor: "grab",
    transition: "box-shadow 0.1s",
  };

  return (
    <div>
      {/* Step list */}
      {steps.map((s, i) => (
        <div
          key={s}
          draggable
          onDragStart={() => onDragStart(i)}
          onDragOver={onDragOver(i)}
          onDrop={onDrop(i)}
          onDragEnd={onDragEnd}
          style={{
            ...row,
            opacity: dragIdx === i ? 0.4 : 1,
            boxShadow: overIdx === i && dragIdx !== i ? "0 0 0 2px #8b5cf6" : "none",
          }}
        >
          {/* Grip icon */}
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" style={{ flexShrink: 0, color: "#9ca3af" }}>
            <circle cx="5" cy="4" r="1.2" fill="currentColor"/>
            <circle cx="5" cy="7" r="1.2" fill="currentColor"/>
            <circle cx="5" cy="10" r="1.2" fill="currentColor"/>
            <circle cx="9" cy="4" r="1.2" fill="currentColor"/>
            <circle cx="9" cy="7" r="1.2" fill="currentColor"/>
            <circle cx="9" cy="10" r="1.2" fill="currentColor"/>
          </svg>
          {/* Number badge */}
          <span style={{
            width: 18, height: 18, borderRadius: 4,
            background: "#ede9fe", color: "#7c3aed",
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 9, fontWeight: 700, flexShrink: 0,
          }}>{i + 1}</span>
          {/* Name */}
          <span style={{ flex: 1, fontSize: 12, color: "#111827", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            {s}
          </span>
          {/* Remove */}
          <button
            onClick={() => onStepsChange(steps.filter((_, j) => j !== i))}
            style={{ background: "none", border: "none", cursor: "pointer", color: "#9ca3af", fontSize: 16, padding: "0 2px", lineHeight: 1, flexShrink: 0 }}
            title="Remove"
          >×</button>
        </div>
      ))}

      {/* Add step button + popover */}
      <div style={{ position: "relative" }} ref={popRef}>
        <button
          onClick={() => setAddOpen(v => !v)}
          style={{
            width: "100%", padding: "6px 10px", border: "1px solid #e5e7eb",
            borderRadius: 6, background: "#fff", cursor: "pointer",
            display: "flex", alignItems: "center", gap: 6,
            fontSize: 12, color: "#374151", fontWeight: 500,
          }}
        >
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" style={{ flexShrink: 0 }}>
            <line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
          </svg>
          Add step
        </button>

        {addOpen && (
          <div style={{
            position: "absolute", top: "calc(100% + 4px)", left: 0, right: 0,
            border: "1px solid #e5e7eb", borderRadius: 8, background: "#fff",
            boxShadow: "0 4px 16px rgba(0,0,0,0.10)", zIndex: 50, overflow: "hidden",
          }}>
            <div style={{ padding: "8px 10px", borderBottom: "1px solid #f3f4f6" }}>
              <input
                ref={inputRef}
                value={query}
                onChange={e => setQuery(e.target.value)}
                placeholder="Search events…"
                style={{ width: "100%", boxSizing: "border-box", border: "1px solid #e5e7eb", borderRadius: 6, padding: "5px 8px", fontSize: 12, outline: "none" }}
              />
            </div>
            <div style={{ maxHeight: 200, overflowY: "auto" }}>
              {available.slice(0, 60).map(e => (
                <div
                  key={e}
                  onClick={() => addStep(e)}
                  style={{ padding: "6px 12px", fontSize: 12, cursor: "pointer", color: "#111827" }}
                  onMouseEnter={ev => (ev.currentTarget.style.background = "#f3f4f6")}
                  onMouseLeave={ev => (ev.currentTarget.style.background = "")}
                >{e}</div>
              ))}
              {available.length === 0 && (
                <div style={{ padding: "8px 12px", fontSize: 11, color: "#9ca3af" }}>No events found</div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ── sidebar ────────────────────────────────────────────────────────────────

function Sidebar({ steps, events, segLevels, pathCols, pathIdCol,
  diffSegment, diffValue1, diffValue2, isLoading, isStatic,
  onStepsChange, onPathIdColChange, onDiffChange, onResetFilters }: {
  steps: string[];
  events: string[];
  segLevels: Record<string, string[]>;
  pathCols: string[];
  pathIdCol: string;
  diffSegment: string | null;
  diffValue1: string | null;
  diffValue2: string | null;
  isLoading: boolean;
  isStatic?: boolean;
  onStepsChange: (s: string[]) => void;
  onPathIdColChange: (c: string) => void;
  onDiffChange: (seg: string | null, v1: string | null, v2: string | null) => void;
  onResetFilters: () => void;
}) {
  const segCols = Object.keys(segLevels);
  const [localSeg, setLocalSeg] = React.useState(diffSegment ?? "");
  const [localV1,  setLocalV1]  = React.useState(diffValue1  ?? "");
  const [localV2,  setLocalV2]  = React.useState(diffValue2  ?? "");
  React.useEffect(() => {
    setLocalSeg(diffSegment ?? "");
    setLocalV1(diffValue1  ?? "");
    setLocalV2(diffValue2  ?? "");
  }, [diffSegment, diffValue1, diffValue2]);

  const localLevels = localSeg ? (segLevels[localSeg] ?? []) : [];
  const canApply = localSeg !== "" && localV1 !== "" && localV2 !== "" && localV1 !== localV2;
  const isDirty  = localSeg !== (diffSegment ?? "") || localV1 !== (diffValue1 ?? "") || localV2 !== (diffValue2 ?? "");

  const sel: React.CSSProperties = {
    width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db",
    borderRadius: 6, color: "#111827", fontSize: 12,
    padding: "5px 24px 5px 8px", cursor: "pointer", outline: "none",
    appearance: "none",
    backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%236b7280' stroke-width='2'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E")`,
    backgroundRepeat: "no-repeat", backgroundPosition: "right 6px center", background: "#f9fafb",
  };
  const SH = ({ children }: { children: React.ReactNode }) => (
    <div style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: "#6b7280", marginBottom: 10 }}>{children}</div>
  );
  const FL = ({ children }: { children: React.ReactNode }) => (
    <div style={{ fontSize: 13, fontWeight: 500, color: "#111827", marginBottom: 6 }}>{children}</div>
  );

  return (
    <div style={{ width: SIDEBAR_W, minWidth: SIDEBAR_W, height: "100%", background: "#fff", borderLeft: "1px solid #e5e7eb", display: "flex", flexDirection: "column", overflow: "hidden", fontFamily: "system-ui, sans-serif" }}>
      <div style={{ padding: "10px 14px", borderBottom: "1px solid #e5e7eb", display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#111827" }}>Settings</span>
        {isLoading && <span style={{ fontSize: 11, color: "#6b7280" }}>Computing…</span>}
      </div>
      <div style={{ flex: 1, overflowY: "auto", padding: 14 }}>

        <SH>Funnel Steps</SH>
        <div style={{ marginBottom: 20, opacity: isStatic ? 0.5 : 1, pointerEvents: isStatic ? "none" : undefined }}>
          <StepEditor steps={steps} events={events} onStepsChange={onStepsChange} />
        </div>

        {pathCols.length > 1 && (
          <div style={{ marginBottom: 20 }}>
            <FL>Path Column</FL>
            <select value={pathIdCol} onChange={e => onPathIdColChange(e.target.value)} style={sel} disabled={isLoading || isStatic}>
              {pathCols.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
        )}

        <div style={{ height: 1, background: "#e5e7eb", margin: "0 0 16px" }} />
        <SH>Diff by Segment</SH>

        <select
          value={localSeg}
          onChange={e => {
            const v = e.target.value;
            setLocalSeg(v);
            if (!v) { setLocalV1(""); setLocalV2(""); onDiffChange(null, null, null); }
            else { const lvls = segLevels[v] ?? []; setLocalV1(lvls[0] != null ? String(lvls[0]) : ""); setLocalV2(lvls[1] != null ? String(lvls[1]) : ""); }
          }}
          style={{ ...sel, marginBottom: localSeg ? 8 : 0 }}
          disabled={isLoading || isStatic}
        >
          <option value="">— None</option>
          {segCols.map(c => <option key={c} value={c}>{c}</option>)}
        </select>

        {localSeg && localLevels.length >= 2 && (
          <>
            <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 4, flex: 1, minWidth: 0 }}>
                <span style={{ color: "rgb(239,68,68)", fontSize: 13, flexShrink: 0 }}>●</span>
                <select value={localV1} onChange={e => setLocalV1(e.target.value)} style={{ ...sel, flex: 1, minWidth: 0, width: "auto" }} disabled={isLoading || isStatic}>
                  {localLevels.map(v => <option key={String(v)} value={String(v)} disabled={String(v) === localV2}>{segmentValueLabel(String(v))}</option>)}
                </select>
              </div>
              <span style={{ color: "#6b7280", fontSize: 11, flexShrink: 0 }}>vs</span>
              <div style={{ display: "flex", alignItems: "center", gap: 4, flex: 1, minWidth: 0 }}>
                <span style={{ color: "rgb(59,130,246)", fontSize: 13, flexShrink: 0 }}>●</span>
                <select value={localV2} onChange={e => setLocalV2(e.target.value)} style={{ ...sel, flex: 1, minWidth: 0, width: "auto" }} disabled={isLoading || isStatic}>
                  {localLevels.map(v => <option key={String(v)} value={String(v)} disabled={String(v) === localV1}>{segmentValueLabel(String(v))}</option>)}
                  <option value={REST_VALUE}>{REST_LABEL}</option>
                </select>
              </div>
            </div>
            {isDirty && !isStatic && (
              <button
                onClick={() => { if (canApply) onDiffChange(localSeg, localV1, localV2); }}
                disabled={!canApply || isLoading}
                style={{ width: "100%", padding: "6px 0", background: canApply ? "var(--retentioneering-yellow)" : "#f3f4f6", border: "none", borderRadius: 6, cursor: canApply ? "pointer" : "default", color: canApply ? "#1a1a1a" : "#9ca3af", fontSize: 12, fontWeight: 600 }}
              >Apply</button>
            )}
          </>
        )}

        <div style={{ height: 1, background: "#e5e7eb", margin: "16px 0" }} />
        <button
          onClick={onResetFilters}
          disabled={isStatic}
          style={{ width: "100%", padding: "6px 0", background: "transparent", border: "1px solid #d1d5db", borderRadius: 6, color: "#6b7280", fontSize: 12, cursor: isStatic ? "default" : "pointer" }}
        >Reset</button>
      </div>
    </div>
  );
}

// ── sidebar toggle ─────────────────────────────────────────────────────────

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

export function render({ host, el, isStatic = false }: RenderContext) {
  function App() {
    const [steps,       setStepsState]  = React.useState<string[]>(() => parseJson(host.get("steps"), []));
    const [diffSeg,     setDiffSeg]     = React.useState<string | null>(() => { const d = parseJson<string[]>(host.get("diff") || "[]", []); return d[0] ?? null; });
    const [diffV1,      setDiffV1]      = React.useState<string | null>(() => { const d = parseJson<string[]>(host.get("diff") || "[]", []); return d[1] ?? null; });
    const [diffV2,      setDiffV2]      = React.useState<string | null>(() => { const d = parseJson<string[]>(host.get("diff") || "[]", []); return d[2] ?? null; });
    const [pathIdCol,   setPathIdCol]   = React.useState<string>(() => (host.get("path_col") as string) || "");
    const [result,      setResult]      = React.useState<{ steps: FunnelStep[] }>(() => parseJson(host.get("result"), { steps: [] }));
    const [isLoading,   setIsLoading]   = React.useState<boolean>(() => (host.get("is_loading") as boolean) ?? false);
    const [heightProp,  setHeightProp]  = React.useState<number>(() => (host.get("height") as number) ?? 0);
    const [sidebarOpen, setSidebarOpen] = React.useState<boolean>(() => (host.get("sidebar_open") as boolean) ?? true);

    const events    = parseJson<string[]>(host.get("event_list"), []);
    const pathCols  = parseJson<string[]>(host.get("path_cols"), []);
    const segLevels = parseJson<Record<string, string[]>>(host.get("segment_levels"), {});

    useHostSubscriptions(host, [
      ["result",     () => setResult(parseJson(host.get("result"), { steps: [] }))],
      ["is_loading", () => setIsLoading((host.get("is_loading") as boolean) ?? false)],
      ["height",     () => setHeightProp((host.get("height") as number) ?? 0)],
      ["sidebar_open", () => setSidebarOpen((host.get("sidebar_open") as boolean) ?? true)],
      ["steps",      () => setStepsState(parseJson(host.get("steps"), []))],
      ["diff",       () => { const d = parseJson<string[]>(host.get("diff") || "[]", []); setDiffSeg(d[0] ?? null); setDiffV1(d[1] ?? null); setDiffV2(d[2] ?? null); }],
      ["path_col", () => setPathIdCol((host.get("path_col") as string) || "")],
    ]);

    const setSteps = React.useCallback((s: string[]) => {
      setStepsState(s);
      host.set("steps", JSON.stringify(s));
    }, []);

    const handleDiff = React.useCallback((seg: string | null, v1: string | null, v2: string | null) => {
      setDiffSeg(seg); setDiffV1(v1); setDiffV2(v2);
      host.set("diff", seg && v1 && v2 ? JSON.stringify([seg, v1, v2]) : "");
    }, []);

    const handlePathId = React.useCallback((c: string) => {
      setPathIdCol(c); host.set("path_col", c);
    }, []);

    const handleToggle = React.useCallback(() => {
      setSidebarOpen(p => { const n = !p; host.set("sidebar_open", n); return n; });
    }, []);

    const funnelSteps = result.steps ?? [];
    // Detect diff mode from the result itself — this handles the case where
    // the backend sends result before the diff traitlet syncs to the browser.
    const hasDiff = funnelSteps.length > 0
      ? funnelSteps[0].funnel1_unique_paths !== undefined
      : !!(diffSeg && diffV1 && diffV2);
    // Labels are baked into the result by the backend — always present even if
    // the diff traitlet hasn't synced to the browser yet.
    const label1 = (result as any).group1_label || diffV1 || "Group 1";
    const label2 = (result as any).group2_label || diffV2 || "Group 2";

    // Auto-height: if heightProp == 0 (not set), compute from content
    const computedHeight = heightProp > 0
      ? heightProp
      : autoHeight(steps.length, funnelSteps.length, hasDiff);

    const CHART_H = Math.max(CHART_MIN_H, computedHeight - TABLE_HEAD_H - funnelSteps.length * TABLE_ROW_H - HEADER_H - PADDING - 20);

    return (
      <div style={{ display: "flex", flexDirection: "row", height: computedHeight, background: "#ffffff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
        <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0, display: "flex", flexDirection: "column" }}>
          <SidebarToggle onClick={handleToggle} />
          <div style={{ width: "100%", height: "100%" }}>
            <div style={{ flex: 1, overflowY: "auto", padding: 16 }}>
              {funnelSteps.length === 0 && !isLoading && (
                <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "#9ca3af", fontSize: 13 }}>
                  Add steps in the settings panel to build the funnel
                </div>
              )}
              {funnelSteps.length > 0 && (
                <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
                  <FunnelChart steps={funnelSteps} hasDiff={hasDiff} label1={label1} label2={label2} chartH={CHART_H} />
                  <div style={{ width: "100%" }}>
                    <FunnelTable steps={funnelSteps} hasDiff={hasDiff} label1={label1} label2={label2} result={result} />
                  </div>
                </div>
              )}
            </div>
          </div>
          {isLoading && <ComputingSpinner />}
        </div>
        {sidebarOpen && (
          <Sidebar
            steps={steps} events={events} segLevels={segLevels}
            pathCols={pathCols} pathIdCol={pathIdCol}
            diffSegment={diffSeg} diffValue1={diffV1} diffValue2={diffV2}
            isLoading={isLoading}
            isStatic={isStatic}
            onStepsChange={setSteps}
            onPathIdColChange={handlePathId}
            onDiffChange={handleDiff}
            onResetFilters={() => { setSteps([]); handleDiff(null, null, null); }}
          />
        )}
        <RetentioneeringSpinKeyframes />
      </div>
    );
  }

  const root = createRoot(el);
  root.render(<App />);
  return () => root.unmount();
}
