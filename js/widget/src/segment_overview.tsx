/**
 * Segment Overview Widget
 * Heatmap table: rows = metrics, columns = segment values.
 * Click a cell to see the metric distribution for that segment.
 * Click two cells in the same row to compare their distributions.
 */
import * as React from "react";
import { createRoot } from "react-dom/client";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes } from "./widget-utils";
import { AuthGate, loadSession, clearSession, type AuthSession } from "./AuthGate";
import { MetricRow, validateMetricCfg } from "./metric_config_row";

interface AnyWidgetModel {
  get(key: string): unknown;
  set(key: string, value: unknown): void;
  save_changes(): void;
  on(event: string, cb: () => void): void;
  off(event: string, cb: () => void): void;
}
interface RenderContext { model: AnyWidgetModel; el: HTMLElement; isStatic?: boolean; }

// ── heatmap colour ─────────────────────────────────────────────────────────

/** Blue–white–red diverging heatmap. t ∈ [0,1]. */
function heatmapRgb(t: number): string {
  if (t < 0.5) {
    const u = t / 0.5;
    return `rgb(${Math.round(59 + u * (229-59))}, ${Math.round(130 + u * (231-130))}, ${Math.round(246 + u * (235-246))})`;
  } else {
    const u = (t - 0.5) / 0.5;
    return `rgb(${Math.round(229 + u * (239-229))}, ${Math.round(231 - u * (231-68))}, ${Math.round(235 - u * (235-68))})`;
  }
}

function cellColor(value: number, min: number, max: number): string {
  if (!Number.isFinite(value) || min === max) return "#f9fafb";
  const t = (value - min) / (max - min);
  return heatmapRgb(t);
}

// ── value formatting ────────────────────────────────────────────────────────

function formatCell(metric: string, v: number): string {
  if (!Number.isFinite(v)) return "—";
  if (metric === "segment_share") return (v * 100).toFixed(1) + "%";
  if (metric.startsWith("first_event_time")) return new Date(v * 1000).toISOString().slice(0, 10);
  if (metric === "segment_size" || metric.endsWith("_size") || metric.endsWith("_count"))
    return v.toLocaleString(undefined, { maximumFractionDigits: 0 });
  if (metric.includes("duration") || metric.includes("time"))
    return formatTime(v);
  if (v >= 1000) return v.toLocaleString(undefined, { maximumFractionDigits: 0 });
  if (Number.isInteger(v)) return v.toString();
  return v.toFixed(3).replace(/\.?0+$/, "");
}

function formatTime(seconds: number): string {
  if (seconds < 60)  return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86400).toFixed(1)}d`;
}

// ── types ──────────────────────────────────────────────────────────────────

interface SegmentOverviewData {
  metrics:  string[];
  segments: string[];
  values:   number[][];          // values[metricIdx][segmentIdx]
}

interface DistributionData {
  bins: number[];
  counts: number[];
  counts_normalized: number[];
  kde: [number[], number[]] | null;
  mean: number;
  median: number;
}

interface DistributionResult {
  distribution_1?: DistributionData;
  distribution_2?: DistributionData;
  distribution?:   DistributionData;
  distance?: number;
  log_scale?: boolean;
}

// ── mini histogram (SVG) ───────────────────────────────────────────────────

function MiniHistogram({ dist, color, width = 260, height = 120 }: {
  dist: DistributionData;
  color: string;
  width?: number;
  height?: number;
}) {
  if (!dist.bins.length) return <div style={{ color: "#9ca3af", fontSize: 12 }}>No data</div>;

  const PAD = { l: 28, r: 8, t: 8, b: 28 };
  const W = width - PAD.l - PAD.r;
  const H = height - PAD.t - PAD.b;
  const maxCount = Math.max(...dist.counts_normalized, 0.001);
  const nBins = dist.counts.length;

  const bins  = dist.bins;
  const xMin  = bins[0], xRange = bins[bins.length - 1] - bins[0];

  const toX = (v: number) => PAD.l + (xRange > 0 ? ((v - xMin) / xRange) * W : W / 2);
  const toY = (v: number) => PAD.t + H * (1 - v / maxCount);

  const bars = dist.counts_normalized.map((c, i) => ({
    x: toX(bins[i]),
    w: toX(bins[i + 1]) - toX(bins[i]),
    h: (c / maxCount) * H,
    y: PAD.t + H - (c / maxCount) * H,
  }));

  // KDE path
  let kdePath = "";
  if (dist.kde) {
    const [xs, ys] = dist.kde;
    const maxY = Math.max(...ys, 0.001);
    const pts = xs.map((x, i) => `${toX(x).toFixed(1)},${(PAD.t + H * (1 - ys[i] / maxY)).toFixed(1)}`);
    kdePath = `M${pts[0]} ` + pts.slice(1).map(p => `L${p}`).join(" ");
  }

  // Axis labels
  const xLabels = [xMin, xMin + xRange / 2, xMin + xRange].map((v, i) => ({
    x: PAD.l + (i === 0 ? 0 : i === 1 ? W / 2 : W),
    text: xRange > 1000 ? v.toExponential(1) : v.toFixed(xRange < 1 ? 2 : xRange < 10 ? 1 : 0),
  }));

  return (
    <svg width={width} height={height} style={{ display: "block" }}>
      {/* Bars */}
      {bars.map((b, i) => (
        <rect key={i} x={b.x + 0.5} y={b.y} width={Math.max(b.w - 1, 1)} height={b.h}
          fill={color} opacity={0.7} />
      ))}
      {/* KDE */}
      {kdePath && <path d={kdePath} fill="none" stroke={color} strokeWidth={1.5} opacity={0.9} />}
      {/* Axes */}
      <line x1={PAD.l} y1={PAD.t + H} x2={PAD.l + W} y2={PAD.t + H} stroke="#d1d5db" strokeWidth={1} />
      <line x1={PAD.l} y1={PAD.t} x2={PAD.l} y2={PAD.t + H} stroke="#d1d5db" strokeWidth={1} />
      {xLabels.map((l, i) => (
        <text key={i} x={l.x} y={PAD.t + H + 14} textAnchor="middle" fontSize={9} fill="#6b7280">{l.text}</text>
      ))}
      {/* Mean/median lines */}
      {Number.isFinite(dist.mean) && (
        <line x1={toX(dist.mean)} y1={PAD.t} x2={toX(dist.mean)} y2={PAD.t + H}
          stroke={color} strokeWidth={1} strokeDasharray="3,2" opacity={0.8} />
      )}
    </svg>
  );
}

// ── distribution modal ─────────────────────────────────────────────────────

function DistributionModal({ result, label1, label2, metricName, onClose }: {
  result: DistributionResult;
  label1: string;
  label2: string;
  metricName: string;
  onClose: () => void;
}) {
  const isPair = !!result.distribution_1;
  const dist1  = isPair ? result.distribution_1! : result.distribution!;
  const dist2  = isPair ? result.distribution_2 : undefined;

  React.useEffect(() => {
    const h = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    document.addEventListener("keydown", h);
    return () => document.removeEventListener("keydown", h);
  }, [onClose]);

  return (
    <div
      style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.35)", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={e => e.target === e.currentTarget && onClose()}
    >
      <div style={{ background: "#fff", borderRadius: 12, boxShadow: "0 8px 32px rgba(0,0,0,0.18)", padding: 24, minWidth: 340, maxWidth: 680, width: "90%" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <span style={{ fontSize: 14, fontWeight: 600, color: "#111827" }}>{metricName}</span>
          <button onClick={onClose} style={{ background: "none", border: "none", fontSize: 18, cursor: "pointer", color: "#6b7280", padding: "0 4px" }}>×</button>
        </div>

        {result.log_scale && (
          <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 10, background: "#f9fafb", padding: "4px 8px", borderRadius: 4 }}>
            Log₁₀ scale applied (highly skewed data)
          </div>
        )}

        <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
          <div>
            <div style={{ fontSize: 11, fontWeight: 600, color: "#7c3aed", marginBottom: 6 }}>{label1}</div>
            <MiniHistogram dist={dist1} color="#7c3aed" />
            <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4 }}>
              mean {Number.isFinite(dist1.mean) ? dist1.mean.toFixed(3) : "—"} · median {Number.isFinite(dist1.median) ? dist1.median.toFixed(3) : "—"}
            </div>
          </div>
          {dist2 && (
            <div>
              <div style={{ fontSize: 11, fontWeight: 600, color: "#06b6d4", marginBottom: 6 }}>{label2}</div>
              <MiniHistogram dist={dist2} color="#06b6d4" />
              <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4 }}>
                mean {Number.isFinite(dist2.mean) ? dist2.mean.toFixed(3) : "—"} · median {Number.isFinite(dist2.median) ? dist2.median.toFixed(3) : "—"}
              </div>
            </div>
          )}
        </div>

        {Number.isFinite(result.distance) && (
          <div style={{ marginTop: 12, fontSize: 12, color: "#374151", background: "#f3f4f6", padding: "6px 10px", borderRadius: 6 }}>
            Wasserstein distance: <strong>{result.distance!.toFixed(4)}</strong>
          </div>
        )}
      </div>
    </div>
  );
}

// ── heatmap table ──────────────────────────────────────────────────────────

function HeatmapTable({ data, onCellClick, onCellRightClick, selectedCells }: {
  data: SegmentOverviewData;
  onCellClick: (metricIdx: number, segIdx: number, shift: boolean) => void;
  onCellRightClick: (metricIdx: number, segIdx: number, x: number, y: number) => void;
  selectedCells: Set<string>;
}) {
  const { metrics, segments, values } = data;

  // Per-row bounds for colour scaling
  const rowBounds = metrics.map((_, mi) => {
    const row = values[mi].filter(Number.isFinite);
    return { min: Math.min(...row), max: Math.max(...row) };
  });

  const th: React.CSSProperties = {
    padding: "6px 10px", fontSize: 11, fontWeight: 600, color: "#6b7280",
    background: "#f9fafb", borderBottom: "1px solid #e5e7eb",
    borderRight: "1px solid #e5e7eb", whiteSpace: "nowrap",
    position: "sticky", top: 0, zIndex: 2,
  };
  const thLeft: React.CSSProperties = {
    ...th, textAlign: "left", position: "sticky", left: 0, zIndex: 3,
    minWidth: 160, maxWidth: 220,
  };

  return (
    <div style={{ overflowX: "auto", overflowY: "auto", flex: 1 }}>
      <table style={{ borderCollapse: "collapse", fontSize: 12, width: "max-content", minWidth: "100%" }}>
        <thead>
          <tr>
            <th style={{ ...thLeft, background: "#f9fafb" }}>Metric</th>
            {segments.map(seg => (
              <th key={seg} data-segment={seg} style={{ ...th, textAlign: "right" }}>{seg}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {metrics.map((metric, mi) => {
            const { min, max } = rowBounds[mi];
            return (
              <tr key={metric} data-metric={metric}>
                <td style={{
                  padding: "5px 10px", fontSize: 11, color: "#374151", fontWeight: 500,
                  background: "#fff", borderBottom: "1px solid #f3f4f6",
                  borderRight: "1px solid #e5e7eb",
                  position: "sticky", left: 0, zIndex: 1,
                }}>
                  {metric}
                </td>
                {segments.map((seg, si) => {
                  const v   = values[mi][si];
                  const key = `${mi}:${si}`;
                  const sel = selectedCells.has(key);
                  return (
                    <td
                      key={si}
                      data-segment={seg}
                      onClick={e => onCellClick(mi, si, e.shiftKey)}
                      onContextMenu={e => { e.preventDefault(); onCellRightClick(mi, si, e.clientX, e.clientY); }}
                      style={{
                        padding: "5px 10px", textAlign: "right",
                        borderBottom: "1px solid #f3f4f6",
                        borderRight: "1px solid #f3f4f6",
                        background: sel ? "rgba(124,58,237,0.18)" : cellColor(v, min, max),
                        cursor: "context-menu",
                        outline: sel ? "2px solid #7c3aed" : "none",
                        outlineOffset: "-2px",
                        fontWeight: sel ? 600 : 400,
                        color: "#111827",
                        userSelect: "none",
                      }}
                    >
                      {formatCell(metric, v)}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── sidebar ────────────────────────────────────────────────────────────────

// ── shared select style ────────────────────────────────────────────────────

const mkSel = (fontSize = 12): React.CSSProperties => ({
  width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 6,
  color: "#111827", fontSize, padding: `${fontSize === 12 ? 5 : 4}px 24px ${fontSize === 12 ? 5 : 4}px 8px`,
  cursor: "pointer", outline: "none", appearance: "none",
  backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%236b7280' stroke-width='2'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E")`,
  backgroundRepeat: "no-repeat", backgroundPosition: "right 6px center", background: "#f9fafb",
});


function metricLabel(cfg: any): string {
  const parts = [cfg.metric];
  const a = cfg.metric_args;
  if (a?.event)      parts.push(`(${a.event})`);
  if (a?.events)     parts.push(`(${Array.isArray(a.events) ? a.events.join(", ") : a.events})`);
  if (a?.start_event) parts.push(`(${a.start_event} → ${a.end_event})`);
  return parts.join(" ");
}


// ── context menu ──────────────────────────────────────────────────────────

function ContextMenu({ x, y, canCompare, onShowDist, onClose }: {
  x: number; y: number;
  canCompare: boolean;
  onShowDist: () => void;
  onClose: () => void;
}) {
  const ref = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    const h = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("mousedown", h);
    document.addEventListener("keydown", k);
    return () => { document.removeEventListener("mousedown", h); document.removeEventListener("keydown", k); };
  }, [onClose]);

  const label = canCompare ? "Compare distributions" : "Show distribution";

  return (
    <div ref={ref} style={{
      position: "absolute", left: x, top: y, zIndex: 200,
      background: "#fff", border: "1px solid #e5e7eb", borderRadius: 8,
      boxShadow: "0 4px 16px rgba(0,0,0,0.12)", minWidth: 180, overflow: "hidden",
    }}>
      <button
        onClick={() => { onShowDist(); onClose(); }}
        style={{
          width: "100%", padding: "8px 14px", background: "none", border: "none",
          textAlign: "left", fontSize: 12, color: "#111827", cursor: "pointer",
          display: "flex", alignItems: "center", gap: 8,
        }}
        onMouseEnter={e => (e.currentTarget.style.background = "#f3f4f6")}
        onMouseLeave={e => (e.currentTarget.style.background = "")}
      >
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <rect x="2" y="7" width="8" height="14"/><rect x="14" y="3" width="8" height="18"/>
        </svg>
        {label}
      </button>
    </div>
  );
}

// MetricsModal is split: the trigger button stays in the sidebar,
// the overlay is rendered at the App root level (position: absolute)
// so it escapes VS Code's iframe transform constraints.

function MetricsTriggerButton({ count, onClick, disabled }: { count: number; onClick: () => void; disabled?: boolean }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        width: "100%", padding: "6px 10px", border: "1px solid #e5e7eb", borderRadius: 6,
        background: "#fff", cursor: disabled ? "default" : "pointer", display: "flex", alignItems: "center", justifyContent: "space-between",
        fontSize: 12, color: "#374151", fontWeight: 500,
      }}
    >
      <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l-.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>
        </svg>
        Configure Metrics
      </span>
      {count > 0 && (
        <span style={{ fontSize: 10, color: "#9ca3af", background: "#f3f4f6", borderRadius: 10, padding: "1px 6px" }}>
          {count}
        </span>
      )}
    </button>
  );
}

// Overlay rendered at App root — uses position:absolute to avoid
// VS Code iframe transform issues with position:fixed
function MetricsOverlay({ metrics, events, segmentCols, segmentLevels, onMetricsChange, onClose }: {
  metrics: any[];
  events: string[];
  segmentCols: string[];
  segmentLevels: Record<string,string[]>;
  onMetricsChange: (m: any[]) => void;
  onClose: () => void;
}) {
  const modalRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", k);
    return () => document.removeEventListener("keydown", k);
  }, [onClose]);

  const [submitted, setSubmitted] = React.useState(false);
  const listRef = React.useRef<HTMLDivElement>(null);
  const addMetric    = () => onMetricsChange([...metrics, { metric: "event_count", agg: "mean", metric_args: { event: events } }]);
  const updateMetric = (i: number, cfg: any) => onMetricsChange(metrics.map((m, j) => j === i ? cfg : m));
  const removeMetric = (i: number) => onMetricsChange(metrics.filter((_, j) => j !== i));
  const errCount = metrics.filter(m => validateMetricCfg(m) !== null).length;
  const hasErrors = metrics.length === 0 || errCount > 0;
  const handleDone = () => { if (hasErrors) { setSubmitted(true); } else { onClose(); } };

  const prevLen = React.useRef(metrics.length);
  React.useEffect(() => {
    if (metrics.length > prevLen.current && listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight;
    }
    prevLen.current = metrics.length;
  }, [metrics.length]);

  return (
    <div
      style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.3)", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={e => e.target === e.currentTarget && onClose()}
    >
      <div ref={modalRef} style={{ background: "#fff", borderRadius: 12, boxShadow: "0 8px 32px rgba(0,0,0,0.18)", padding: 24, width: 520, maxWidth: "90%", maxHeight: "80%", display: "flex", flexDirection: "column" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
          <span style={{ fontSize: 15, fontWeight: 600, color: "#111827" }}>Configure Metrics</span>
          <button onClick={onClose} style={{ background: "none", border: "none", fontSize: 20, cursor: "pointer", color: "#6b7280", padding: "0 4px" }}>×</button>
        </div>
        <div ref={listRef} style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: 8 }}>
          {metrics.length === 0 ? (
            <div style={{ textAlign: "center", padding: "32px 0", color: "#9ca3af", fontSize: 13, border: "1px dashed #e5e7eb", borderRadius: 8 }}>
              No metrics added yet. Click "Add Metric" to start.
            </div>
          ) : metrics.map((m, i) => (
            <MetricRow key={i} cfg={m} events={events} segmentCols={segmentCols} segmentLevels={segmentLevels} showErrors={submitted} showAgg={true} onChange={cfg => updateMetric(i, cfg)} onRemove={() => removeMetric(i)} />
          ))}
        </div>
        <div style={{ marginTop: 16, display: "flex", flexDirection: "column", gap: 8 }}>
          <button onClick={addMetric} style={{ width: "100%", padding: "7px 0", border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6, fontSize: 13, color: "#374151", fontWeight: 500 }}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
              <line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>
            </svg>
            Add Metric
          </button>
          {submitted && errCount > 0 && <div style={{ fontSize: 11, color: "#dc2626", textAlign: "center" }}>⚠ {errCount} row{errCount > 1 ? "s have" : " has"} incomplete fields</div>}
          {submitted && metrics.length === 0 && <div style={{ fontSize: 11, color: "#dc2626", textAlign: "center" }}>⚠ Add at least one metric</div>}
          <button onClick={handleDone}
            style={{ width: "100%", padding: "7px 0", background: "var(--retentioneering-yellow)", border: "none", borderRadius: 6, cursor: "pointer", fontSize: 13, fontWeight: 600, color: "#1a1a1a" }}>
            Done
          </button>
        </div>
      </div>
    </div>
  );
}


// Module-level — must NOT be defined inside Sidebar (causes remounting and focus loss)
const SidebarSH = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: "#6b7280", marginBottom: 6 }}>{children}</div>
);

// ── sidebar ────────────────────────────────────────────────────────────────

function Sidebar({ segCols, segCol, pathCols, pathIdCol, metrics, events, isLoading, isDirty, isStatic,
  onSegColChange, onPathIdColChange, onMetricsChange, onOpenMetrics, onApply }: {
  segCols: string[];
  segCol: string;
  pathCols: string[];
  pathIdCol: string;
  metrics: any[];
  events: string[];
  isLoading: boolean;
  isDirty: boolean;
  isStatic?: boolean;
  onSegColChange: (c: string) => void;
  onPathIdColChange: (c: string) => void;
  onMetricsChange: (m: any[]) => void;
  onOpenMetrics: () => void;
  onApply: () => void;
}) {
  const sel = mkSel(12);

  return (
    <div style={{ width: 248, minWidth: 248, height: "100%", background: "#fff", borderLeft: "1px solid #e5e7eb", display: "flex", flexDirection: "column", overflow: "hidden", fontFamily: "system-ui, sans-serif" }}>
      <div style={{ padding: "10px 14px", borderBottom: "1px solid #e5e7eb", display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#111827" }}>Settings</span>
        {isLoading && <span style={{ fontSize: 11, color: "#6b7280" }}>Computing…</span>}
      </div>
      <div style={{ flex: 1, overflowY: "auto", padding: 14 }}>

        <SidebarSH>Segment Column</SidebarSH>
        <div style={{ marginBottom: 16 }}>
          <select value={segCol} onChange={e => onSegColChange(e.target.value)} style={sel} disabled={isLoading || isStatic}>
            <option value="">— select —</option>
            {segCols.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>

        {pathCols.length > 1 && (
          <div style={{ marginBottom: 16 }}>
            <SidebarSH>Path Column</SidebarSH>
            <select value={pathIdCol} onChange={e => onPathIdColChange(e.target.value)} style={sel} disabled={isLoading || isStatic}>
              {pathCols.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
        )}

        <div style={{ height: 1, background: "#e5e7eb", margin: "0 0 14px" }} />
        <SidebarSH>Metrics</SidebarSH>
        <div style={{ marginBottom: 16 }}>
          <MetricsTriggerButton count={metrics.length} onClick={isStatic ? () => {} : onOpenMetrics} disabled={isStatic} />
        </div>

        {!isStatic && isDirty && segCol && (
          <>
            <div style={{ height: 1, background: "#e5e7eb", margin: "0 0 14px" }} />
            <button
              onClick={onApply}
              disabled={isLoading}
              style={{ width: "100%", padding: "7px 0", background: "var(--retentioneering-yellow)", border: "none", borderRadius: 6, cursor: "pointer", color: "#1a1a1a", fontSize: 12, fontWeight: 600 }}
            >
              Apply
            </button>
          </>
        )}
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

export function render({ model, el, isStatic = false }: RenderContext) {
  function App() {
    const [segCol,     setSegColState]    = React.useState<string>(() => (model.get("segment_col") as string) || "");
    const [pathIdCol,  setPathIdColState] = React.useState<string>(() => (model.get("path_col") as string) || "");
    const [metrics,    setMetricsState]   = React.useState<any[]>(() => parseJson(model.get("metrics"), []));
    const [result,     setResult]         = React.useState<SegmentOverviewData | null>(() => {
      const r = parseJson<any>(model.get("result"), null);
      return r?.metrics ? r : null;
    });
    const [isLoading,  setIsLoading]      = React.useState<boolean>(() => (model.get("is_loading") as boolean) ?? false);
    const [height,     setHeight]         = React.useState<number>(() => (model.get("height") as number) ?? 480);
    const [sidebarOpen, setSidebarOpen]   = React.useState<boolean>(() => (model.get("sidebar_open") as boolean) ?? true);
    const [session,    setSession]        = React.useState(() => loadSession());

    // Cell selection: max 2 per row. key = "metricIdx:segIdx"
    const [selected, setSelected] = React.useState<Set<string>>(new Set());
    // Modals and context menu — rendered at root level with position:absolute to work in VS Code
    const [metricsOpen, setMetricsOpen] = React.useState(false);
    const [distModal, setDistModal] = React.useState<{ result: DistributionResult; label1: string; label2: string; metric: string } | null>(null);
    const [distLoading, setDistLoading] = React.useState(false);
    const [ctxMenu, setCtxMenu] = React.useState<{ x: number; y: number; mi: number; si: number } | null>(null);
    const rootRef = React.useRef<HTMLDivElement>(null);

    const segCols   = parseJson<string[]>(model.get("segment_cols"), []);
    const pathCols  = parseJson<string[]>(model.get("path_cols"),    []);
    const events    = parseJson<string[]>(model.get("event_list"),   []);
    const segLevels = parseJson<Record<string,string[]>>(model.get("segment_levels"), {});

    React.useEffect(() => {
      const subs: Array<[string, () => void]> = [
        ["result",     () => { const r = parseJson<any>(model.get("result"), null); setResult(r?.metrics ? r : null); }],
        ["is_loading", () => setIsLoading((model.get("is_loading") as boolean) ?? false)],
        ["height",     () => setHeight((model.get("height") as number) ?? 480)],
        ["sidebar_open", () => setSidebarOpen((model.get("sidebar_open") as boolean) ?? true)],
        ["segment_col", () => setSegColState((model.get("segment_col") as string) || "")],
        ["path_col", () => setPathIdColState((model.get("path_col") as string) || "")],
        ["metrics", () => setMetricsState(parseJson(model.get("metrics"), []))],
      ];
      subs.forEach(([k, cb]) => model.on(`change:${k}`, cb));
      return () => subs.forEach(([k, cb]) => model.off(`change:${k}`, cb));
    }, []);

    const setSegCol = (c: string) => { setSegColState(c); model.set("segment_col", c); model.save_changes(); };
    const setPathId = (c: string) => { setPathIdColState(c); model.set("path_col", c); model.save_changes(); };
    const setMetrics = (m: any[]) => { setMetricsState(m); model.set("metrics", JSON.stringify(m)); model.save_changes(); };

    // Track the last *applied* state to show Apply only when something changed
    const [lastApplied, setLastApplied] = React.useState(() => ({
      segCol:    (model.get("segment_col") as string) || "",
      pathIdCol: (model.get("path_col") as string) || "",
      metrics:   parseJson<any[]>(model.get("metrics"), []),
    }));

    const isDirty = segCol !== lastApplied.segCol ||
                    pathIdCol !== lastApplied.pathIdCol ||
                    JSON.stringify(metrics) !== JSON.stringify(lastApplied.metrics);

    const handleApply = () => {
      model.set("segment_col", segCol);
      model.set("path_col", pathIdCol);
      model.set("metrics", JSON.stringify(metrics));
      model.set("apply_trigger", (Date.now()).toString());
      model.save_changes();
      setLastApplied({ segCol, pathIdCol, metrics });
    };

    const handleToggle = () => {
      setSidebarOpen(p => { const n = !p; model.set("sidebar_open", n); model.save_changes(); return n; });
    };

    // Left click: select/deselect (shift = add second cell in same row, max 2)
    const handleCellClick = React.useCallback((mi: number, si: number, shift: boolean) => {
      if (!result) return;
      const key = `${mi}:${si}`;
      setCtxMenu(null);
      setSelected(prev => {
        if (prev.has(key)) {
          const next = new Set(prev); next.delete(key); return next;
        }
        const next = new Set<string>();
        if (shift) {
          // Add second cell in same row (keep only cells from same row)
          const sameRow = [...prev].filter(k => k.startsWith(`${mi}:`));
          if (sameRow.length < 2) sameRow.forEach(k => next.add(k));
        }
        next.add(key);
        return next;
      });
    }, [result]);

    // Right click: show context menu at position relative to widget root
    const handleCellRightClick = React.useCallback((mi: number, si: number, clientX: number, clientY: number) => {
      if (!result || !rootRef.current) return;
      const rect = rootRef.current.getBoundingClientRect();
      setCtxMenu({ x: clientX - rect.left, y: clientY - rect.top, mi, si });
      // Also select the right-clicked cell if not already selected
      const key = `${mi}:${si}`;
      setSelected(prev => {
        if (prev.has(key)) return prev;
        // Keep existing selection in same row, add this cell
        const sameRow = [...prev].filter(k => k.startsWith(`${mi}:`));
        const next = new Set<string>();
        if (sameRow.length < 2) sameRow.forEach(k => next.add(k));
        next.add(key);
        return next;
      });
    }, [result]);

    // Show distribution for currently selected cells in the right-clicked row
    const handleShowDist = React.useCallback(() => {
      if (!result || !ctxMenu) return;
      const { mi } = ctxMenu;
      const metricName = result.metrics[mi];
      const rowCells   = [...selected].filter(k => k.startsWith(`${mi}:`));

      const metric = { metric: _metricBaseName(metricName), metric_args: _metricArgs(metricName, events) };
      setDistLoading(true);

      if (rowCells.length >= 2) {
        const si1 = parseInt(rowCells[0].split(":")[1]);
        const si2 = parseInt(rowCells[1].split(":")[1]);
        model.set("dist_request", JSON.stringify({
          segment_col: segCol, path_col: pathIdCol || null,
          segment_value: [result.segments[si1], result.segments[si2]], metric,
        }));
      } else {
        const si  = parseInt(rowCells[0].split(":")[1]);
        model.set("dist_request", JSON.stringify({
          segment_col: segCol, path_col: pathIdCol || null,
          segment_value: result.segments[si], metric, complement: true,
        }));
      }
      model.save_changes();
    }, [result, ctxMenu, selected, segCol, pathIdCol, events]);

    // Listen for distribution result
    React.useEffect(() => {
      const cb = () => {
        const raw = model.get("dist_result") as string;
        if (!raw || raw === "{}") return;
        const r = parseJson<DistributionResult>(raw, {});
        if (!r || (!r.distribution && !r.distribution_1)) return;

        // Find selected cells to determine labels
        const cells = [...selected];
        const segs  = result ? cells.map(k => result.segments[parseInt(k.split(":")[1])]) : [];
        const mi    = cells.length > 0 ? parseInt(cells[0].split(":")[0]) : 0;
        const metricName = result ? result.metrics[mi] : "";

        setDistModal({
          result: r,
          label1: segs[0] ?? "Segment",
          label2: segs[1] ?? "Complement",
          metric: metricName,
        });
        setDistLoading(false);
      };
      model.on("change:dist_result", cb);
      return () => model.off("change:dist_result", cb);
    }, [selected, result]);

    // Expose external navigation API for static HTML report links
    React.useEffect(() => {
      // Match by exact metric name OR by prefix (e.g. "has_purchase" → "has_purchase_mean")
      const findMetricRow = (root: HTMLElement, name: string): HTMLElement | null => {
        const exact = root.querySelector(`tr[data-metric="${name}"]`) as HTMLElement | null;
        if (exact) return exact;
        // prefix match: "has_purchase" → "has_purchase_mean"
        const prefix = name + "_";
        for (const r of root.querySelectorAll("tr[data-metric]")) {
          const m = (r as HTMLElement).dataset.metric || "";
          if (m.startsWith(prefix)) return r as HTMLElement;
        }
        // substring match: "purchase" → "has_purchase_mean"
        for (const r of root.querySelectorAll("tr[data-metric]")) {
          const m = (r as HTMLElement).dataset.metric || "";
          if (m.includes(name)) return r as HTMLElement;
        }
        return null;
      };
      const flashBg = (node: HTMLElement, delay = 0) => {
        setTimeout(() => {
          const prev = node.style.background;
          node.style.background = "#fef3c7";
          setTimeout(() => { node.style.background = prev; }, 1000);
        }, delay);
      };
      // Scroll after two animation frames so the panel is fully visible
      const raf2 = (fn: () => void) => requestAnimationFrame(() => requestAnimationFrame(fn));

      (el as any).__segmentApi = {
        focusCell: (metric: string, segment: string) => {
          raf2(() => {
            const row = findMetricRow(el, metric);
            if (!row) return;
            const cell = row.querySelector(`td[data-segment="${segment}"]`) as HTMLElement | null;
            if (cell) {
              cell.scrollIntoView({ block: "nearest", inline: "center", behavior: "smooth" });
              flashBg(cell, 200);
            }
          });
        },
        focusAny: (name: string) => {
          raf2(() => {
            // Try segment column — flash all cells + header simultaneously for a column effect
            const colCells = Array.from(el.querySelectorAll(`td[data-segment="${name}"]`)) as HTMLElement[];
            if (colCells.length) {
              const th = el.querySelector(`th[data-segment="${name}"]`) as HTMLElement | null;
              if (th) th.scrollIntoView({ behavior: "smooth", inline: "center" });
              colCells.forEach(c => flashBg(c, 200));
              if (th) flashBg(th, 200);
              return;
            }
            // Fall back to metric row (exact match or prefix, e.g. "has_purchase" → "has_purchase_mean")
            const row = findMetricRow(el, name);
            if (row) {
              row.scrollIntoView({ block: "nearest", behavior: "smooth" });
              (Array.from(row.querySelectorAll("td")) as HTMLElement[]).forEach(c => flashBg(c, 200));
            }
          });
        },
      };
    }, []);

    return (
      <div ref={rootRef} style={{ position: "relative", display: "flex", flexDirection: "row", height, background: "#fff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
        <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0, display: "flex", flexDirection: "column" }}>
          <SidebarToggle onClick={handleToggle} />
          <AuthGate session={session} onLogin={setSession} disabled={true} style={{ width: "100%", height: "100%", display: "flex", flexDirection: "column" }}>
            {!result && !isLoading && (
              <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: "#9ca3af", fontSize: 13 }}>
                {segCol ? "No data — computation may have failed" : "Select a segment column and click Apply"}
              </div>
            )}
            {result && (
              <div style={{ flex: 1, overflow: "hidden", display: "flex", flexDirection: "column" }}>
                {!isStatic && (
                  <div style={{ padding: "8px 12px 0", fontSize: 11, color: "#6b7280" }}>
                    Click to select · Shift+click for pair · Right-click → Show distribution
                  </div>
                )}
                <HeatmapTable data={result} onCellClick={handleCellClick} onCellRightClick={isStatic ? () => {} : handleCellRightClick} selectedCells={selected} />
              </div>
            )}
          </AuthGate>

          {(isLoading || distLoading) && <ComputingSpinner />}
        </div>

        {sidebarOpen && (
          <Sidebar
            segCols={segCols} segCol={segCol}
            pathCols={pathCols} pathIdCol={pathIdCol}
            metrics={metrics} events={events}
            isLoading={isLoading} isDirty={isDirty}
            isStatic={isStatic}
            onSegColChange={setSegCol}
            onPathIdColChange={setPathId}
            onMetricsChange={setMetrics}
            onOpenMetrics={() => setMetricsOpen(true)}
            onApply={handleApply}
          />
        )}

        {/* Context menu — disabled in static mode (no backend for dist_request) */}
        {!isStatic && ctxMenu && (
          <ContextMenu
            x={ctxMenu.x} y={ctxMenu.y}
            canCompare={[...selected].filter(k => k.startsWith(`${ctxMenu.mi}:`)).length >= 2}
            onShowDist={handleShowDist}
            onClose={() => setCtxMenu(null)}
          />
        )}

        {/* Modals at root level — position:absolute avoids VS Code transform issues */}
        {!isStatic && metricsOpen && (
          <MetricsOverlay
            metrics={metrics} events={events} segmentCols={segCols} segmentLevels={segLevels}
            onMetricsChange={setMetrics}
            onClose={() => setMetricsOpen(false)}
          />
        )}

        {!isStatic && distModal && (
          <DistributionModal
            result={distModal.result}
            label1={distModal.label1}
            label2={distModal.label2}
            metricName={distModal.metric}
            onClose={() => { setDistModal(null); setSelected(new Set()); }}
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

// ── helpers ────────────────────────────────────────────────────────────────

function _metricBaseName(metricName: string): string {
  // "event_count_checkout_mean" → "event_count"
  // "length_mean" → "length"
  const known = ["event_count", "has_event", "time_between", "active_days", "in_segment", "matches_pattern", "first_event_time", "duration", "length"];
  for (const k of known) if (metricName.startsWith(k)) return k;
  return metricName.split("_")[0];
}

function _metricArgs(metricName: string, _events: string[]): any {
  // Best-effort extraction — backend will validate
  const m = _metricBaseName(metricName);
  if (m === "event_count") {
    const rest = metricName.replace(/^event_count_/, "").replace(/_[a-z\d]+$/, "");
    return rest ? { event: rest } : undefined;
  }
  if (m === "has_event") {
    const rest = metricName.replace(/^has_event_/, "").replace(/_[a-z\d]+$/, "");
    return rest ? { events: rest } : undefined;
  }
  return undefined;
}
