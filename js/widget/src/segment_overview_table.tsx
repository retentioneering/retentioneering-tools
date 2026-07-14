/**
 * Shared heatmap table + distribution-comparison UI for Segment Overview and
 * Cluster Analysis. Both widgets show the same shape of data (metrics ×
 * segment/cluster values) and both let the user click a cell to inspect the
 * underlying metric distribution for one or two groups — this module is the
 * single implementation both `segment_overview.tsx` and `cluster_analysis.tsx`
 * render.
 */
import * as React from "react";
import type { WidgetHost } from "@retentioneering/viz-core";
import { parseJson } from "./widget-utils";
import { AGG_OPTIONS } from "./metric_config_row";

// ── heatmap colour ─────────────────────────────────────────────────────────

/** Blue–white–red diverging heatmap. t ∈ [0,1]. */
export function heatmapRgb(t: number): string {
  if (t < 0.5) {
    const u = t / 0.5;
    return `rgb(${Math.round(59 + u * (229-59))}, ${Math.round(130 + u * (231-130))}, ${Math.round(246 + u * (235-246))})`;
  } else {
    const u = (t - 0.5) / 0.5;
    return `rgb(${Math.round(229 + u * (239-229))}, ${Math.round(231 - u * (231-68))}, ${Math.round(235 - u * (235-68))})`;
  }
}

export function cellColor(value: number, min: number, max: number): string {
  if (!Number.isFinite(value) || min === max) return "#f9fafb";
  const t = (value - min) / (max - min);
  return heatmapRgb(t);
}

// ── value / label formatting ────────────────────────────────────────────────

function formatTime(seconds: number): string {
  if (seconds < 60)  return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86400).toFixed(1)}d`;
}

/** Merged cell formatter — superset of the two widgets' previous formatters. */
export function formatCellValue(metric: string, v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v as number)) return "—";
  const n = v as number;
  if (metric === "segment_share") return (n * 100).toFixed(1) + "%";
  if (metric.startsWith("first_event_time")) return new Date(n * 1000).toISOString().slice(0, 10);
  if (metric === "segment_size" || metric.endsWith("_size") || metric.endsWith("_count"))
    return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  if (metric.includes("duration") || metric.includes("time"))
    return formatTime(n);
  if (n >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  if (Number.isInteger(n)) return n.toString();
  return n.toFixed(3).replace(/\.?0+$/, "");
}

// event_count_purchase_mean / has_event_add_to_cart_median → "purchase · event_count · mean"
// (the event name is the interesting part — leading it keeps it from getting lost
// between the metric prefix and the aggregation suffix on a narrow column).
// The bulk variants must be checked before the plain ones: "event_count_bulk_x"
// also starts with "event_count_", so the more specific prefix has to win.
export function formatMetricLabel(metric: string): string {
  for (const base of ["event_count_bulk", "has_event_bulk", "has_all_events", "has_any_event", "event_count", "has_event"]) {
    if (!metric.startsWith(base + "_")) continue;
    const rest = metric.slice(base.length + 1);
    for (const agg of AGG_OPTIONS) {
      const suffix = "_" + agg;
      if (rest.endsWith(suffix) && rest.length > suffix.length) {
        const eventName = rest.slice(0, rest.length - suffix.length);
        return `${eventName} · ${base} · ${agg}`;
      }
    }
  }
  return metric;
}

// ── types ──────────────────────────────────────────────────────────────────

export interface SegmentOverviewData {
  metrics:  string[];
  segments: (string | null)[];   // null = paths with no value for this segment column
  values:   (number | null)[][];   // values[metricIdx][segmentIdx]
}

/** Stand-in for a null segment column (get_segment_levels' "<MISSING>" group,
 *  which segment_overview_data surfaces as a real None column label). Used
 *  both as the display label and, since data-segment/key need a string, as
 *  the DOM sentinel for that column. */
const NO_SEGMENT_LABEL = "(no segment)";
const NO_SEGMENT_KEY = "__none__";

export interface DistributionData {
  bins: number[];
  counts: number[];
  counts_normalized: number[];
  kde: [number[], number[]] | null;
  mean: number;
  median: number;
}

export interface DistributionResult {
  distribution_1?: DistributionData;
  distribution_2?: DistributionData;
  distribution?:   DistributionData;
  distance?: number;
  log_scale?: boolean;
}

// ── mini histogram (SVG) ───────────────────────────────────────────────────

export function MiniHistogram({ dist, color, width = 260, height = 120 }: {
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

  const bins  = dist.bins;
  const xMin  = bins[0], xRange = bins[bins.length - 1] - bins[0];

  const toX = (v: number) => PAD.l + (xRange > 0 ? ((v - xMin) / xRange) * W : W / 2);

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

export function DistributionModal({ result, label1, label2, metricName, onClose }: {
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

// ── context menu ──────────────────────────────────────────────────────────

export function ContextMenu({ x, y, canCompare, onShowDist, onClose }: {
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

// ── editable header label (rename affordance) ───────────────────────────────

function EditableHeaderLabel({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const [editing, setEditing] = React.useState(false);
  const [draft, setDraft] = React.useState(value);
  React.useEffect(() => { if (!editing) setDraft(value); }, [value, editing]);

  if (editing) {
    return (
      <input
        autoFocus
        value={draft}
        onChange={e => setDraft(e.target.value)}
        onClick={e => e.stopPropagation()}
        onBlur={() => { setEditing(false); const v = draft.trim(); if (v && v !== value) onChange(v); }}
        onKeyDown={e => {
          if (e.key === "Enter") (e.target as HTMLInputElement).blur();
          if (e.key === "Escape") { setDraft(value); setEditing(false); }
        }}
        style={{ width: "100%", boxSizing: "border-box", fontSize: 11, fontWeight: 600, color: "#111827", border: "1px solid var(--retentioneering-yellow)", borderRadius: 4, padding: "1px 4px", outline: "none", textAlign: "right" }}
      />
    );
  }
  return (
    <span onClick={e => { e.stopPropagation(); setEditing(true); }} title={`${value} — click to rename`}
      style={{ cursor: "pointer", borderBottom: "1px dashed #9ca3af" }}>
      {value}
    </span>
  );
}

// ── heatmap table ──────────────────────────────────────────────────────────

const LABEL_COL_MIN_W = 40;
const LABEL_COL_DEFAULT_W = 200;

export interface SegmentOverviewTableProps {
  data: SegmentOverviewData;
  /** Optional inline header-rename affordance (Cluster Analysis only). */
  renameMap?: Record<string, string>;
  onRename?: (orig: string, next: string) => void;
  /** Optional cell click/right-click/selection (distribution comparison). */
  onCellClick?: (metricIdx: number, segIdx: number, shift: boolean) => void;
  onCellRightClick?: (metricIdx: number, segIdx: number, x: number, y: number) => void;
  selectedCells?: Set<string>;
  /** Cluster Analysis: drag-resizable label column, capped/truncated value columns.
   *  Segment Overview (default): static label column range, unconstrained value columns. */
  resizableLabelColumn?: boolean;
  labelColumnRange?: { min: number; max: number };
  valueColumnMaxWidth?: number;
}

export function SegmentOverviewTable({
  data, renameMap, onRename,
  onCellClick, onCellRightClick, selectedCells,
  resizableLabelColumn = false,
  labelColumnRange = { min: 160, max: 220 },
  valueColumnMaxWidth,
}: SegmentOverviewTableProps) {
  const { metrics, segments, values } = data;

  const rowBounds = metrics.map((_, mi) => {
    const row = values[mi].filter(v => v !== null && Number.isFinite(v as number)) as number[];
    return { min: Math.min(...row), max: Math.max(...row) };
  });

  // ── resizable label column — drag the right edge to show/hide long names ──
  const [labelWidth, setLabelWidth] = React.useState(LABEL_COL_DEFAULT_W);
  const resizing  = React.useRef(false);
  const startX    = React.useRef(0);
  const startW    = React.useRef(0);
  const handleRef = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    if (!resizableLabelColumn) return;
    const onMove = (e: MouseEvent) => { if (!resizing.current) return; setLabelWidth(Math.max(LABEL_COL_MIN_W, startW.current + e.clientX - startX.current)); };
    const onUp   = () => { resizing.current = false; document.body.style.cursor = document.body.style.userSelect = ""; if (handleRef.current) handleRef.current.style.background = "transparent"; };
    document.addEventListener("mousemove", onMove); document.addEventListener("mouseup", onUp);
    return () => { document.removeEventListener("mousemove", onMove); document.removeEventListener("mouseup", onUp); };
  }, [resizableLabelColumn]);

  const th: React.CSSProperties = {
    padding: "5px 10px", fontSize: 11, fontWeight: 600, color: "#6b7280",
    background: "#f9fafb", borderBottom: "1px solid #e5e7eb", borderRight: "1px solid #e5e7eb",
    whiteSpace: "nowrap", position: "sticky", top: 0, zIndex: 2,
  };

  const thL: React.CSSProperties = resizableLabelColumn
    ? { ...th, textAlign: "left", position: "sticky", left: 0, zIndex: 3, boxSizing: "border-box", width: labelWidth, minWidth: labelWidth, maxWidth: labelWidth, overflow: "hidden", textOverflow: "ellipsis" }
    : { ...th, textAlign: "left", position: "sticky", left: 0, zIndex: 3, minWidth: labelColumnRange.min, maxWidth: labelColumnRange.max, overflow: "hidden", textOverflow: "ellipsis" };

  const tdL: React.CSSProperties = resizableLabelColumn
    ? { padding: "5px 10px", fontSize: 11, color: "#374151", fontWeight: 500, background: "#fff", borderBottom: "1px solid #f3f4f6", borderRight: "1px solid #e5e7eb", position: "sticky", left: 0, zIndex: 1, boxSizing: "border-box", width: labelWidth, minWidth: labelWidth, maxWidth: labelWidth, overflow: "hidden", whiteSpace: "nowrap", textOverflow: "ellipsis" }
    : { padding: "5px 10px", fontSize: 11, color: "#374151", fontWeight: 500, background: "#fff", borderBottom: "1px solid #f3f4f6", borderRight: "1px solid #e5e7eb", position: "sticky", left: 0, zIndex: 1, minWidth: labelColumnRange.min, maxWidth: labelColumnRange.max, overflow: "hidden", whiteSpace: "nowrap", textOverflow: "ellipsis" };

  // Value columns are optionally capped so a long (renamed) label can't blow up the
  // whole table — better to truncate it (full value on hover) than widen every column.
  // Width/minWidth/maxWidth must all agree — max-width alone is only a hint to table
  // auto-layout and gets ignored once content is wider than it.
  const thV: React.CSSProperties = valueColumnMaxWidth
    ? { ...th, padding: "5px 6px", textAlign: "right", boxSizing: "border-box", width: valueColumnMaxWidth, minWidth: valueColumnMaxWidth, maxWidth: valueColumnMaxWidth, overflow: "hidden", textOverflow: "ellipsis" }
    : { ...th, textAlign: "right" };
  const tdV: React.CSSProperties = valueColumnMaxWidth
    ? { padding: "5px 6px", textAlign: "right", borderBottom: "1px solid #f3f4f6", borderRight: "1px solid #f3f4f6", boxSizing: "border-box", width: valueColumnMaxWidth, minWidth: valueColumnMaxWidth, maxWidth: valueColumnMaxWidth, overflow: "hidden", whiteSpace: "nowrap", textOverflow: "ellipsis" }
    : { padding: "5px 10px", textAlign: "right", borderBottom: "1px solid #f3f4f6", borderRight: "1px solid #f3f4f6" };

  const interactive = !!(onCellClick || onCellRightClick);

  return (
    <div style={{ position: "relative", height: "100%", overflow: "hidden" }}>
      {resizableLabelColumn && (
        <div ref={handleRef}
          title="Drag to resize"
          style={{ position: "absolute", left: labelWidth - 1, top: 0, bottom: 0, width: 3, cursor: "col-resize", zIndex: 20, background: "transparent", transition: "background 0.12s" }}
          onMouseEnter={() => { if (handleRef.current) handleRef.current.style.background = "var(--retentioneering-yellow)"; }}
          onMouseLeave={() => { if (!resizing.current && handleRef.current) handleRef.current.style.background = "transparent"; }}
          onMouseDown={e => { e.preventDefault(); resizing.current = true; startX.current = e.clientX; startW.current = labelWidth; document.body.style.cursor = "col-resize"; document.body.style.userSelect = "none"; }}
        />
      )}
      <div style={{ position: "absolute", inset: 0, overflowX: "auto", overflowY: "auto" }}>
        <table style={{ borderCollapse: "collapse", tableLayout: resizableLabelColumn ? "fixed" : undefined, fontSize: 12, width: resizableLabelColumn ? "auto" : "max-content", minWidth: resizableLabelColumn ? undefined : "100%" }}>
          <thead>
            <tr>
              <th style={{ ...thL, background: "#f9fafb" }}>Metric</th>
              {segments.map(seg => {
                const segKey = seg ?? NO_SEGMENT_KEY;
                const label = seg ?? NO_SEGMENT_LABEL;
                return (
                  <th key={segKey} data-segment={segKey} style={thV}>
                    {onRename ? (
                      <EditableHeaderLabel value={renameMap?.[segKey] ?? label} onChange={v => onRename(segKey, v)} />
                    ) : (
                      <span title={label}>{label}</span>
                    )}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {metrics.map((metric, mi) => {
              const { min, max } = rowBounds[mi];
              return (
                <tr key={metric} data-metric={metric}>
                  <td title={formatMetricLabel(metric)} style={tdL}>
                    {formatMetricLabel(metric)}
                  </td>
                  {segments.map((seg, si) => {
                    const v = values[mi][si];
                    const key = `${mi}:${si}`;
                    const sel = !!selectedCells?.has(key);
                    return (
                      <td
                        key={si}
                        data-segment={seg ?? NO_SEGMENT_KEY}
                        title={formatCellValue(metric, v)}
                        onClick={onCellClick ? (e => onCellClick(mi, si, e.shiftKey)) : undefined}
                        onContextMenu={onCellRightClick ? (e => { e.preventDefault(); onCellRightClick(mi, si, e.clientX, e.clientY); }) : undefined}
                        style={{
                          ...tdV,
                          background: sel ? "rgba(124,58,237,0.18)" : (v !== null ? cellColor(v, min, max) : "#f9fafb"),
                          cursor: interactive ? "context-menu" : "default",
                          outline: sel ? "2px solid #7c3aed" : "none",
                          outlineOffset: "-2px",
                          fontWeight: sel ? 600 : 400,
                          color: "#111827",
                          userSelect: interactive ? "none" : undefined,
                        }}
                      >
                        {formatCellValue(metric, v)}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── metric-name reverse-parsing (for building a distribution request) ──────

export function metricBaseName(metricName: string): string {
  // "event_count_checkout_mean" → "event_count"
  // "length_mean" → "length"
  // Bulk variants are checked before the plain ones: "event_count_bulk_x" also
  // starts with "event_count", so the more specific prefix has to win.
  const known = ["event_count_bulk", "has_event_bulk", "has_all_events", "has_any_event", "event_count", "has_event", "time_between", "active_days", "in_segment", "matches_pattern", "first_event_time", "duration", "length"];
  for (const k of known) if (metricName.startsWith(k)) return k;
  return metricName.split("_")[0];
}

export function metricArgsFromName(metricName: string, _events: string[]): any {
  // Best-effort extraction — backend will validate. A single cell's
  // distribution is inherently single-valued, so both the plain and bulk
  // variants reconstruct into the strict single-event 'event_count'/'has_event'
  // config (never '*_bulk', which can't be used for a single-column request).
  const m = metricBaseName(metricName);
  if (m === "event_count_bulk") {
    const rest = metricName.replace(/^event_count_bulk_/, "").replace(/_[a-z\d]+$/, "");
    return rest ? { event: rest } : undefined;
  }
  if (m === "has_event_bulk") {
    const rest = metricName.replace(/^has_event_bulk_/, "").replace(/_[a-z\d]+$/, "");
    return rest ? { event: rest } : undefined;
  }
  if (m === "event_count") {
    const rest = metricName.replace(/^event_count_/, "").replace(/_[a-z\d]+$/, "");
    return rest ? { event: rest } : undefined;
  }
  if (m === "has_event") {
    const rest = metricName.replace(/^has_event_/, "").replace(/_[a-z\d]+$/, "");
    return rest ? { event: rest } : undefined;
  }
  // has_all_events/has_any_event: the joined events list can't be
  // unambiguously split back out if an event name itself contains "_and_"/
  // "_or_", so we deliberately don't reconstruct metric_args for these -
  // callers should treat a missing metric_args as "can't drill in further".
  return undefined;
}

// ── shared selection / context-menu / distribution-request state machine ───

export interface DistributionSelectionOptions {
  host: WidgetHost;
  result: SegmentOverviewData | null;
  rootRef: React.RefObject<HTMLElement | null>;
  segmentCol: string;
  pathIdCol: string;
  events: string[];
}

export function useDistributionSelection({ host, result, rootRef, segmentCol, pathIdCol, events }: DistributionSelectionOptions) {
  // Cell selection: max 2 per row. key = "metricIdx:segIdx"
  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [distModal, setDistModal] = React.useState<{ result: DistributionResult; label1: string; label2: string; metric: string } | null>(null);
  const [distLoading, setDistLoading] = React.useState(false);
  const [ctxMenu, setCtxMenu] = React.useState<{ x: number; y: number; mi: number; si: number } | null>(null);

  // has_all_events/has_any_event columns can't be drilled into: the joined
  // events list in the column name can't be unambiguously reconstructed into
  // metric_args (see metricArgsFromName), so their cells aren't selectable.
  const isDrillable = React.useCallback((mi: number) => {
    if (!result) return false;
    const base = metricBaseName(result.metrics[mi]);
    return base !== "has_all_events" && base !== "has_any_event";
  }, [result]);

  // Left click: select/deselect (shift = add second cell in same row, max 2)
  const handleCellClick = React.useCallback((mi: number, si: number, shift: boolean) => {
    if (!result || !isDrillable(mi)) return;
    const key = `${mi}:${si}`;
    setCtxMenu(null);
    setSelected(prev => {
      if (prev.has(key)) {
        const next = new Set(prev); next.delete(key); return next;
      }
      const next = new Set<string>();
      if (shift) {
        const sameRow = [...prev].filter(k => k.startsWith(`${mi}:`));
        if (sameRow.length < 2) sameRow.forEach(k => next.add(k));
      }
      next.add(key);
      return next;
    });
  }, [result, isDrillable]);

  // Right click: show context menu at position relative to widget root
  const handleCellRightClick = React.useCallback((mi: number, si: number, clientX: number, clientY: number) => {
    if (!result || !rootRef.current || !isDrillable(mi)) return;
    const rect = rootRef.current.getBoundingClientRect();
    setCtxMenu({ x: clientX - rect.left, y: clientY - rect.top, mi, si });
    const key = `${mi}:${si}`;
    setSelected(prev => {
      if (prev.has(key)) return prev;
      const sameRow = [...prev].filter(k => k.startsWith(`${mi}:`));
      const next = new Set<string>();
      if (sameRow.length < 2) sameRow.forEach(k => next.add(k));
      next.add(key);
      return next;
    });
  }, [result, rootRef, isDrillable]);

  // Show distribution for currently selected cells in the right-clicked row
  const handleShowDist = React.useCallback(() => {
    if (!result || !ctxMenu) return;
    const { mi } = ctxMenu;
    const metricName = result.metrics[mi];
    const rowCells   = [...selected].filter(k => k.startsWith(`${mi}:`));

    const metric = { metric: metricBaseName(metricName), metric_args: metricArgsFromName(metricName, events) };
    setDistLoading(true);

    if (rowCells.length >= 2) {
      const si1 = parseInt(rowCells[0].split(":")[1]);
      const si2 = parseInt(rowCells[1].split(":")[1]);
      host.set("dist_request", JSON.stringify({
        segment_col: segmentCol, path_col: pathIdCol || null,
        segment_value: [result.segments[si1], result.segments[si2]], metric,
      }));
    } else {
      const si  = parseInt(rowCells[0].split(":")[1]);
      host.set("dist_request", JSON.stringify({
        segment_col: segmentCol, path_col: pathIdCol || null,
        segment_value: result.segments[si], metric, complement: true,
      }));
    }
  }, [result, ctxMenu, selected, segmentCol, pathIdCol, events, host]);

  // Listen for distribution result
  React.useEffect(() => {
    const cb = () => {
      const raw = host.get("dist_result") as string;
      if (!raw || raw === "{}") return;
      const r = parseJson<DistributionResult>(raw, {});
      if (!r || (!r.distribution && !r.distribution_1)) { setDistLoading(false); return; }

      const cells = [...selected];
      const segs  = result ? cells.map(k => result.segments[parseInt(k.split(":")[1])]) : [];
      const mi    = cells.length > 0 ? parseInt(cells[0].split(":")[0]) : 0;
      const metricName = result ? result.metrics[mi] : "";

      setDistModal({
        result: r,
        // segs[i] is `null` (a real "no segment value" group, not a missing
        // selection) whenever that cell's column is the None segment — only
        // fall back to the generic label when no cell was selected at all.
        label1: segs[0] !== undefined ? (segs[0] ?? NO_SEGMENT_LABEL) : "Segment",
        label2: segs[1] !== undefined ? (segs[1] ?? NO_SEGMENT_LABEL) : "Complement",
        metric: metricName,
      });
      setDistLoading(false);
    };
    return host.onChange("dist_result", cb);
  }, [selected, result, host]);

  const closeCtxMenu   = React.useCallback(() => setCtxMenu(null), []);
  const closeDistModal = React.useCallback(() => { setDistModal(null); setSelected(new Set()); }, []);

  const ctxMenuCanCompare = ctxMenu
    ? [...selected].filter(k => k.startsWith(`${ctxMenu.mi}:`)).length >= 2
    : false;

  return {
    selected, ctxMenu, ctxMenuCanCompare, distModal, distLoading,
    handleCellClick, handleCellRightClick, handleShowDist,
    closeCtxMenu, closeDistModal,
  };
}
