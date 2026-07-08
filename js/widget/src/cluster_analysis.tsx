import * as React from "react";
import { createRoot } from "react-dom/client";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes } from "./widget-utils";
import { MetricRow, validateMetricCfg, InfoTip } from "./metric_config_row";
import {
  AnyWidgetModel, SegmentOverviewData, SegmentOverviewTable,
  ContextMenu, DistributionModal, useDistributionSelection,
} from "./segment_overview_table";

interface RenderContext { model: AnyWidgetModel; el: HTMLElement; isStatic?: boolean; }

// The cluster label column injected into the temp eventstream backing the
// heatmap — mirrors `SEGMENT_COL` in tools/cluster_analysis.py. Cluster
// Analysis has no user-selectable segment column (unlike Segment Overview),
// so this is a fixed constant used only to shape the dist_request payload.
const CLUSTER_SEGMENT_COL = "__cluster__";

// ── types ──────────────────────────────────────────────────────────────────

interface SilhouetteData { params: Record<string,any>[]; silhouette: (number|null)[]; }
interface NmfData { H_matrix: number[][]; features: string[]; W_cluster_means: Record<string,number[]>; }
interface ClusterResult { overview?: SegmentOverviewData; silhouette?: SilhouetteData; nmf?: NmfData; }

// ── silhouette chart ───────────────────────────────────────────────────────

function SilhouetteChart({ data }: { data: SilhouetteData }) {
  const { params, silhouette } = data;
  const scores = silhouette.map(s => s ?? 0);
  const best   = scores.indexOf(Math.max(...scores));
  const maxS   = Math.max(...scores, 0.01);

  const W = 60; const H = 180; const PAD = { l: 32, r: 8, t: 12, b: 72 };
  const plotH = H - PAD.t - PAD.b;
  const barW  = 28; const gap = 8;
  const totalW = params.length * (barW + gap) + PAD.l + PAD.r;

  const scrollRef = React.useRef<HTMLDivElement>(null);

  // Scroll so the best bar is visible (centred) when chart mounts or data changes
  React.useEffect(() => {
    const el = scrollRef.current;
    if (!el || best < 0) return;
    const bestX = PAD.l + best * (barW + gap) + barW / 2;
    const target = bestX - el.clientWidth / 2;
    el.scrollLeft = Math.max(0, target);
  }, [best, totalW]);

  const paramLabel = (p: Record<string,any>) =>
    Object.entries(p).map(([k, v]) => `${k.replace("n_clusters","k").replace("min_cluster_size","mcs").replace("cluster_selection_epsilon","ε").replace("nmf_components","nmf")}=${v}`).join(", ");

  return (
    <div ref={scrollRef} style={{ overflowX: "auto", padding: "8px 0" }}>
      <svg width={Math.max(totalW, 300)} height={H} style={{ display: "block" }}>
        {[0, 0.25, 0.5, 0.75, 1.0].filter(v => v <= maxS * 1.05).map(v => {
          const y = PAD.t + plotH * (1 - v / maxS);
          return (
            <g key={v}>
              <line x1={PAD.l} x2={totalW - PAD.r} y1={y} y2={y} stroke="#e5e7eb" strokeWidth={1} />
              <text x={PAD.l - 3} y={y + 3} textAnchor="end" fontSize={8} fill="#9ca3af">{v.toFixed(2)}</text>
            </g>
          );
        })}
        {params.map((p, i) => {
          const s = scores[i];
          const bH = Math.max(2, (s / maxS) * plotH);
          const x  = PAD.l + i * (barW + gap);
          const isB = i === best;
          return (
            <g key={i}>
              <rect x={x} y={PAD.t + plotH - bH} width={barW} height={bH} fill={isB ? "var(--retentioneering-yellow)" : "#d1d5db"} rx={3} />
              {isB && <text x={x + barW/2} y={PAD.t + plotH - bH - 3} textAnchor="middle" fontSize={8} fill="#92400e" fontWeight={700}>{s.toFixed(3)}</text>}
              <text x={x + barW/2} y={PAD.t + plotH + 12} textAnchor="end" fontSize={8} fill="#6b7280" transform={`rotate(-35,${x + barW/2},${PAD.t + plotH + 12})`}>
                {paramLabel(p).slice(0, 18)}
              </text>
            </g>
          );
        })}
        <line x1={PAD.l} y1={PAD.t} x2={PAD.l} y2={PAD.t + plotH} stroke="#d1d5db" strokeWidth={1} />
        <line x1={PAD.l} y1={PAD.t + plotH} x2={totalW - PAD.r} y2={PAD.t + plotH} stroke="#d1d5db" strokeWidth={1} />
      </svg>
      <div style={{ fontSize: 11, color: "#6b7280", paddingLeft: PAD.l }}>Silhouette score (higher = better separation)</div>
    </div>
  );
}

// ── NMF helpers ────────────────────────────────────────────────────────────

function nmfHeatmapColor(value: number, min: number, max: number): string {
  if (min === max) return "#f9fafb";
  const n = (value - min) / (max - min);
  if (n < 0.5) { const i = 1 - n * 2; return `rgba(59,130,246,${i.toFixed(2)})`; }
  const i = (n - 0.5) * 2; return `rgba(239,68,68,${i.toFixed(2)})`;
}

function fmtFeature(name: string): string {
  return name.startsWith("event_count_") ? `#${name.slice("event_count_".length)}` : name;
}

// ── NMF H-matrix (Component × Feature) — vertical headers, resizable header height ──

function NmfHMatrix({ nmf }: { nmf: NmfData }) {
  const { H_matrix, features } = nmf;
  const [headerH, setHeaderH] = React.useState(80);
  const resizing = React.useRef(false);
  const startY   = React.useRef(0);
  const startH   = React.useRef(0);

  const onMove = React.useCallback((e: MouseEvent) => {
    if (!resizing.current) return;
    setHeaderH(Math.max(32, Math.min(200, startH.current + e.clientY - startY.current)));
  }, []);

  const onUp = React.useCallback(() => {
    resizing.current = false;
    document.removeEventListener("mousemove", onMove);
    document.body.style.cursor = document.body.style.userSelect = "";
  }, [onMove]);

  const onDown = (e: React.MouseEvent) => {
    e.preventDefault();
    resizing.current = true; startY.current = e.clientY; startH.current = headerH;
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp, { once: true });
    document.body.style.cursor = "row-resize"; document.body.style.userSelect = "none";
  };

  const cellH: React.CSSProperties = { padding: "3px 6px", textAlign: "right", fontSize: 10, color: "#111827", borderBottom: "1px solid #f3f4f6", borderRight: "1px solid #f3f4f6" };
  const rowLabel: React.CSSProperties = { padding: "3px 10px", fontSize: 10, fontWeight: 500, color: "#374151", background: "#f9fafb", borderBottom: "1px solid #f3f4f6", borderRight: "1px solid #e5e7eb", whiteSpace: "nowrap" };

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ borderCollapse: "collapse", fontSize: 11 }}>
        <thead>
          <tr>
            {/* Corner: label at bottom + drag handle at very bottom */}
            <th style={{ background: "#f9fafb", borderRight: "1px solid #e5e7eb", padding: 0, minWidth: 90, verticalAlign: "bottom", position: "relative" }}>
              <div style={{ height: headerH, display: "flex", alignItems: "flex-end", padding: "4px 10px", fontSize: 10, fontWeight: 600, color: "#6b7280", boxSizing: "border-box" }}>
                Component
              </div>
              {/* Drag handle — thin strip at the bottom of the header row */}
              <div onMouseDown={onDown}
                style={{ height: 5, cursor: "row-resize", background: "transparent", borderTop: "2px solid #e5e7eb", borderBottom: "1px solid #e5e7eb" }}
                title="Drag to resize header" />
            </th>
            {features.map(f => (
              <th key={f} style={{ background: "#f9fafb", borderRight: "1px solid #f3f4f6", padding: 0, minWidth: 36, verticalAlign: "bottom" }}>
                <div style={{ height: headerH, display: "flex", alignItems: "flex-end", justifyContent: "center", overflow: "hidden", padding: "4px 2px", boxSizing: "border-box" }}>
                  <span style={{ writingMode: "vertical-rl", transform: "rotate(180deg)", fontSize: 10, fontWeight: 500, color: "#374151", overflow: "hidden", whiteSpace: "nowrap", maxHeight: headerH - 8 }} title={fmtFeature(f)}>
                    {fmtFeature(f)}
                  </span>
                </div>
                <div style={{ height: 5, borderTop: "2px solid #e5e7eb", borderBottom: "1px solid #e5e7eb" }} />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {H_matrix.map((row, ci) => {
            const min = Math.min(...row), max = Math.max(...row);
            return (
              <tr key={ci}>
                <td style={rowLabel}>Component {ci + 1}</td>
                {row.map((v, fi) => (
                  <td key={fi} style={{ ...cellH, background: nmfHeatmapColor(v, min, max) }} title={`${fmtFeature(features[fi])}: ${v.toFixed(4)}`}>
                    {v.toFixed(3)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── NMF W-matrix (Cluster × Component) ────────────────────────────────────

function NmfWMatrix({ nmf }: { nmf: NmfData }) {
  const { W_cluster_means } = nmf;
  const entries = Object.entries(W_cluster_means);
  if (!entries.length) return null;
  const nComp = entries[0][1].length;

  const thBase: React.CSSProperties = { background: "#f9fafb", borderBottom: "1px solid #e5e7eb", borderRight: "1px solid #f3f4f6", padding: "5px 8px", fontSize: 10, fontWeight: 600, color: "#6b7280", whiteSpace: "nowrap" };
  const tdRow: React.CSSProperties  = { padding: "3px 8px", fontSize: 10, fontWeight: 500, color: "#374151", borderBottom: "1px solid #f3f4f6", borderRight: "1px solid #e5e7eb", whiteSpace: "nowrap", background: "#f9fafb" };

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ borderCollapse: "collapse", fontSize: 11, width: "max-content" }}>
        <thead>
          <tr>
            <th style={{ ...thBase, textAlign: "left" }}>Cluster</th>
            {Array.from({ length: nComp }).map((_, i) => (
              <th key={i} style={{ ...thBase, textAlign: "right", minWidth: 70 }}>Component {i + 1}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {entries.map(([clusterName, row]) => {
            const min = Math.min(...row), max = Math.max(...row);
            return (
              <tr key={clusterName}>
                <td style={tdRow}>{clusterName}</td>
                {row.map((v, ci) => (
                  <td key={ci} style={{ padding: "3px 8px", textAlign: "right", fontSize: 10, color: "#111827", borderBottom: "1px solid #f3f4f6", borderRight: "1px solid #f3f4f6", background: nmfHeatmapColor(v, min, max) }} title={`Component ${ci+1}: ${v.toFixed(4)}`}>
                    {v.toFixed(3)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}


// ── features modal overlay (rendered at App root for VS Code compat) ────────

function FeaturesOverlay({ features, events, segmentCols, segmentLevels, onFeaturesChange, onClose }: {
  features: any[]; events: string[]; segmentCols: string[]; segmentLevels: Record<string,string[]>;
  onFeaturesChange: (f: any[]) => void; onClose: () => void;
}) {
  const ref = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", k);
    return () => document.removeEventListener("keydown", k);
  }, [onClose]);

  const [submitted, setSubmitted] = React.useState(false);
  const listRef = React.useRef<HTMLDivElement>(null);
  const add    = () => onFeaturesChange([...features, { metric: "event_count", metric_args: undefined }]);
  const update = (i: number, cfg: any) => onFeaturesChange(features.map((x, j) => j === i ? cfg : x));
  const remove = (i: number) => onFeaturesChange(features.filter((_, j) => j !== i));
  const errCount = features.filter(f => validateMetricCfg(f) !== null).length;
  const hasErrors = features.length === 0 || errCount > 0;
  const handleDone = () => { if (hasErrors) { setSubmitted(true); } else { onClose(); } };

  const prevLen = React.useRef(features.length);
  React.useEffect(() => {
    if (features.length > prevLen.current && listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight;
    }
    prevLen.current = features.length;
  }, [features.length]);

  return (
    <div
      style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.3)", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={e => e.target === e.currentTarget && onClose()}
    >
      <div ref={ref} style={{ background: "#fff", borderRadius: 12, boxShadow: "0 8px 32px rgba(0,0,0,0.18)", padding: 24, width: 520, maxWidth: "90%", maxHeight: "80%", display: "flex", flexDirection: "column" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
          <span style={{ fontSize: 15, fontWeight: 600, color: "#111827" }}>Configure Features</span>
          <button onClick={onClose} style={{ background: "none", border: "none", fontSize: 20, cursor: "pointer", color: "#6b7280", padding: "0 4px" }}>×</button>
        </div>
        <div ref={listRef} style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: 8 }}>
          {features.length === 0 ? (
            <div style={{ textAlign: "center", padding: "24px 0", color: "#9ca3af", fontSize: 13, border: "1px dashed #e5e7eb", borderRadius: 8 }}>
              No features added yet. Click "Add Feature" to start.
            </div>
          ) : features.map((f, i) => (
            <MetricRow key={i} cfg={f} events={events} segmentCols={segmentCols} segmentLevels={segmentLevels}
              showErrors={submitted} showAgg={false}
              onChange={cfg => update(i, cfg)}
              onRemove={() => remove(i)}
            />
          ))}
        </div>
        <div style={{ marginTop: 16, display: "flex", flexDirection: "column", gap: 8 }}>
          <button onClick={add} style={{ width: "100%", padding: "7px 0", border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6, fontSize: 13, color: "#374151", fontWeight: 500 }}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            Add Feature
          </button>
          {submitted && errCount > 0 && <div style={{ fontSize: 11, color: "#dc2626", textAlign: "center" }}>⚠ {errCount} row{errCount > 1 ? "s have" : " has"} incomplete fields</div>}
          {submitted && features.length === 0 && <div style={{ fontSize: 11, color: "#dc2626", textAlign: "center" }}>⚠ Add at least one feature</div>}
          <button onClick={handleDone}
            style={{ width: "100%", padding: "7px 0", background: "var(--retentioneering-yellow)", border: "none", borderRadius: 6, cursor: "pointer", fontSize: 13, fontWeight: 600, color: "#1a1a1a" }}>
            Done
          </button>
        </div>
      </div>
    </div>
  );
}

function FeaturesTriggerButton({ count, onClick, disabled }: { count: number; onClick: () => void; disabled?: boolean }) {
  return (
    <button onClick={onClick} disabled={disabled} style={{ width: "100%", padding: "6px 10px", border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", cursor: disabled ? "default" : "pointer", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 12, color: "#374151", fontWeight: 500 }}>
      <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/>
          <line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/>
        </svg>
        Configure Features
      </span>
      {count > 0 && <span style={{ fontSize: 10, color: "#9ca3af", background: "#f3f4f6", borderRadius: 10, padding: "1px 6px" }}>{count}</span>}
    </button>
  );
}

function MetricsOverlay({ metrics, events, segmentCols, segmentLevels, onMetricsChange, onClose }: {
  metrics: any[]; events: string[]; segmentCols: string[]; segmentLevels: Record<string,string[]>;
  onMetricsChange: (m: any[]) => void; onClose: () => void;
}) {
  React.useEffect(() => {
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", k);
    return () => document.removeEventListener("keydown", k);
  }, [onClose]);

  const [submitted, setSubmitted] = React.useState(false);
  const listRef = React.useRef<HTMLDivElement>(null);
  const add    = () => onMetricsChange([...metrics, { metric: "event_count", agg: "mean", metric_args: undefined }]);
  const update = (i: number, cfg: any) => onMetricsChange(metrics.map((m, j) => j === i ? cfg : m));
  const remove = (i: number) => onMetricsChange(metrics.filter((_, j) => j !== i));
  const errCount = metrics.filter(m => validateMetricCfg(m) !== null).length;
  const hasErrors = metrics.length === 0 || errCount > 0;
  const handleDone = () => { if (hasErrors) { setSubmitted(true); } else { onClose(); } };

  // Scroll to bottom when a new metric is added
  const prevLen = React.useRef(metrics.length);
  React.useEffect(() => {
    if (metrics.length > prevLen.current && listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight;
    }
    prevLen.current = metrics.length;
  }, [metrics.length]);

  return (
    <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.3)", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={e => e.target === e.currentTarget && onClose()}>
      <div style={{ background: "#fff", borderRadius: 12, boxShadow: "0 8px 32px rgba(0,0,0,0.18)", padding: 24, width: 520, maxWidth: "90%", maxHeight: "80%", display: "flex", flexDirection: "column" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
          <span style={{ fontSize: 15, fontWeight: 600, color: "#111827" }}>Configure Metrics</span>
          <button onClick={onClose} style={{ background: "none", border: "none", fontSize: 20, cursor: "pointer", color: "#6b7280", padding: "0 4px" }}>×</button>
        </div>
        <div ref={listRef} style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: 4 }}>
          {metrics.length === 0 ? (
            <div style={{ textAlign: "center", padding: "24px 0", color: "#9ca3af", fontSize: 13, border: "1px dashed #e5e7eb", borderRadius: 8 }}>
              No metrics added yet.
            </div>
          ) : metrics.map((m, i) => (
            <MetricRow key={i} cfg={m} events={events} segmentCols={segmentCols} segmentLevels={segmentLevels} showErrors={submitted} showAgg={true} onChange={cfg => update(i, cfg)} onRemove={() => remove(i)} />
          ))}
        </div>
        <div style={{ marginTop: 16, display: "flex", flexDirection: "column", gap: 8 }}>
          <button onClick={add} style={{ width: "100%", padding: "7px 0", border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6, fontSize: 13, color: "#374151", fontWeight: 500 }}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
            Add Metric
          </button>
          {submitted && errCount > 0 && <div style={{ fontSize: 11, color: "#dc2626", textAlign: "center" }}>⚠ {errCount} row{errCount > 1 ? "s have" : " has"} incomplete fields</div>}
          {submitted && metrics.length === 0 && <div style={{ fontSize: 11, color: "#dc2626", textAlign: "center" }}>⚠ Add at least one metric</div>}
          <button onClick={handleDone}
            style={{ width: "100%", padding: "7px 0", background: "var(--retentioneering-yellow)", border: "none", borderRadius: 6, cursor: "pointer", fontSize: 13, fontWeight: 600, color: "#1a1a1a" }}>Done</button>
        </div>
      </div>
    </div>
  );
}

interface SaveResult { ok?: boolean; segment_name?: string; error?: string; }

// ── client-side add_clusters(...) code preview ──────────────────────────────
// Pure templating from state already available in the browser — kept in sync
// with the form on every render instead of round-tripping through Python.

function pyRepr(v: any): string {
  if (v === null || v === undefined) return "None";
  if (typeof v === "boolean") return v ? "True" : "False";
  if (typeof v === "number") return String(v);
  if (typeof v === "string") return `'${v.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
  if (Array.isArray(v)) return `[${v.map(pyRepr).join(", ")}]`;
  if (typeof v === "object") return `{${Object.entries(v).map(([k, val]) => `${pyRepr(k)}: ${pyRepr(val)}`).join(", ")}}`;
  return JSON.stringify(v);
}

function buildAddClustersCode(streamVarName: string, name: string, features: any[], method: string, scaler: string, params: Record<string, any>, pathCol: string, rename: Record<string, string>): string {
  const lines = [`    name=${pyRepr(name)},`, `    features=${pyRepr(features)},`, `    method=${pyRepr(method)},`];
  if (scaler) lines.push(`    scaler=${pyRepr(scaler)},`);
  for (const key of ["n_clusters", "min_cluster_size", "cluster_selection_epsilon", "nmf_components"]) {
    if (params[key] !== undefined && params[key] !== null) lines.push(`    ${key}=${pyRepr(params[key])},`);
  }
  if (pathCol) lines.push(`    path_col=${pyRepr(pathCol)},`);

  let code = `${streamVarName}_new = ${streamVarName}.add_clusters(\n${lines.join("\n")}\n)`;
  if (Object.keys(rename).length > 0) {
    code += `.rename_segment_values(\n    ${pyRepr(name)},\n    ${pyRepr(rename)},\n)`;
  }
  return code;
}

function SaveClustersOverlay({ segments, initialRename, streamVarName, features, method, scaler, chosenParams, pathCol, onApplyInplace, onClose, saveResult }: {
  segments: string[];
  initialRename: Record<string, string>;
  streamVarName: string;
  features: any[]; method: string; scaler: string; chosenParams: Record<string, any>; pathCol: string;
  onApplyInplace: (name: string, rename: Record<string, string>) => void;
  onClose: () => void;
  saveResult: SaveResult | null;
}) {
  const clusterNames = segments.filter(s => s !== "noise");
  const [name, setName] = React.useState("cluster");
  const [renameMap, setRenameMap] = React.useState<Record<string, string>>(() =>
    Object.fromEntries(clusterNames.map(s => [s, initialRename[s] ?? s]))
  );
  const [copied, setCopied] = React.useState(false);

  React.useEffect(() => {
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", k);
    return () => document.removeEventListener("keydown", k);
  }, [onClose]);

  const nameValid = name.trim().length > 0;

  const computeRename = (): Record<string, string> => {
    const rename: Record<string, string> = {};
    for (const orig of clusterNames) {
      const v = (renameMap[orig] ?? "").trim();
      if (v && v !== orig) rename[orig] = v;
    }
    return rename;
  };

  // Always current — recomputed every render straight from live form/widget state.
  const currentCode = buildAddClustersCode(streamVarName, name.trim() || "cluster", features, method, scaler, chosenParams, pathCol, computeRename());

  const copyCode = async (code: string) => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch { /* clipboard unavailable — user can still select the textarea manually */ }
  };

  const handleCopyCode = () => { if (nameValid) copyCode(currentCode); };
  const handleApplyInplace = () => { if (nameValid) onApplyInplace(name.trim(), computeRename()); };

  return (
    <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.3)", zIndex: 100, display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={e => e.target === e.currentTarget && onClose()}>
      <div style={{ background: "#fff", borderRadius: 12, boxShadow: "0 8px 32px rgba(0,0,0,0.18)", padding: 24, width: 480, maxWidth: "90%", maxHeight: "85%", overflowY: "auto", display: "flex", flexDirection: "column" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
          <span style={{ fontSize: 15, fontWeight: 600, color: "#111827" }}>Save Clusters to Eventstream</span>
          <button onClick={onClose} style={{ background: "none", border: "none", fontSize: 20, cursor: "pointer", color: "#6b7280", padding: "0 4px" }}>×</button>
        </div>

        <div style={{ marginBottom: 14 }}>
          <SidebarFL>Segment column name</SidebarFL>
          <input value={name} onChange={e => setName(e.target.value)} placeholder="cluster"
            style={{ width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 6, padding: "6px 8px", fontSize: 12, outline: "none" }} />
        </div>

        {clusterNames.length > 0 && (
          <div style={{ marginBottom: 14 }}>
            <SidebarFL>Rename clusters (optional)</SidebarFL>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {clusterNames.map(orig => (
                <div key={orig} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ fontSize: 11, color: "#6b7280", width: 110, flexShrink: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{orig}</span>
                  <span style={{ color: "#9ca3af", fontSize: 11 }}>→</span>
                  <input value={renameMap[orig] ?? orig} onChange={e => setRenameMap({ ...renameMap, [orig]: e.target.value })}
                    style={{ flex: 1, minWidth: 0, boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 6, padding: "5px 8px", fontSize: 12, outline: "none" }} />
                </div>
              ))}
            </div>
          </div>
        )}

        {saveResult && saveResult.ok === false && (
          <div style={{ marginBottom: 14, padding: 10, background: "#fff1f2", border: "1px solid #fca5a5", borderRadius: 6, fontSize: 11, color: "#dc2626", fontFamily: "monospace", whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
            {saveResult.error}
          </div>
        )}

        {saveResult && saveResult.ok && (
          <div style={{ marginBottom: 14, padding: 10, background: "#f0fdf4", border: "1px solid #86efac", borderRadius: 6, fontSize: 11, color: "#15803d" }}>
            Saved as segment "{saveResult.segment_name}" — `{streamVarName}` now has this column.
          </div>
        )}

        {nameValid && (
          <div style={{ marginBottom: 14 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 5 }}>
              <SidebarFL>Code preview</SidebarFL>
              <InfoTip text="Copy this into a new cell to get the same clustering without touching the current stream." />
            </div>
            <div style={{ position: "relative" }}>
              <textarea readOnly value={currentCode} rows={8}
                style={{ width: "100%", boxSizing: "border-box", fontFamily: "monospace", fontSize: 11, padding: "8px 34px 8px 8px", border: "1px solid #d1d5db", borderRadius: 6, resize: "vertical" }} />
              <button onClick={handleCopyCode} title={copied ? "Copied!" : "Copy code"}
                style={{ position: "absolute", top: 6, right: 6, width: 24, height: 24, display: "flex", alignItems: "center", justifyContent: "center", border: "1px solid #e5e7eb", borderRadius: 5, background: "#fff", cursor: "pointer", color: copied ? "#15803d" : "#6b7280" }}>
                {copied ? (
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="20 6 9 17 4 12"/></svg>
                ) : (
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
                )}
              </button>
            </div>
          </div>
        )}

        <button onClick={handleApplyInplace} disabled={!nameValid}
          style={{ width: "100%", display: "flex", alignItems: "center", justifyContent: "center", gap: 6, padding: "8px 0", background: nameValid ? "var(--retentioneering-yellow)" : "#f3f4f6", border: "none", borderRadius: 6, cursor: nameValid ? "pointer" : "default", fontSize: 13, fontWeight: 600, color: nameValid ? "#1a1a1a" : "#9ca3af" }}>
          Apply in-place
          <span onClick={e => e.stopPropagation()}><InfoTip text={`Adds the column to \`${streamVarName}\` immediately.`} /></span>
        </button>
      </div>
    </div>
  );
}

function SaveClustersTriggerButton({ onClick }: { onClick: () => void }) {
  return (
    <button onClick={onClick} style={{ width: "100%", padding: "6px 10px", border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6, fontSize: 12, color: "#374151", fontWeight: 500 }}>
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/>
      </svg>
      Save Clusters…
    </button>
  );
}

function NCInput({ value, onChange, placeholder, disabled }: { value: string; onChange: (v: string) => void; placeholder?: string; disabled?: boolean }) {
  return (
    <input value={value} onChange={e => onChange(e.target.value)}
      placeholder={placeholder} disabled={disabled}
      style={{ width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 6, padding: "5px 8px", fontSize: 12, outline: "none" }} />
  );
}

// Module-level components — must NOT be defined inside Sidebar or App,
// as inline component definitions cause remounting on every render (focus loss).
const SidebarFL = ({ children }: { children: React.ReactNode }) =>
  <div style={{ fontSize: 12, fontWeight: 500, color: "#111827", marginBottom: 5 }}>{children}</div>;

const SidebarSection = ({ title, children }: { title: string; children: React.ReactNode }) => (
  <div style={{ border: "1px solid #e5e7eb", borderRadius: 8, padding: "10px 12px", marginBottom: 14 }}>
    <div style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: "#6b7280", marginBottom: 10 }}>{title}</div>
    {children}
  </div>
);

// ── sidebar ────────────────────────────────────────────────────────────────

function Sidebar({ features, method, scaler, nClustersRaw, nmfEnabled, nmfKRaw,
  metricsConfig, events, pathIdCol, pathCols, isLoading, isStatic,
  onOpenFeatures, onOpenMetrics, onMethodChange, onScalerChange, onNClustersChange,
  onNmfToggle, onNmfKChange, onMetricsChange,
  onPathIdColChange, onApply, isDirty, hasResult, onOpenSave }: {
  features: any[]; method: string; scaler: string; nClustersRaw: string;
  nmfEnabled: boolean; nmfKRaw: string;
  metricsConfig: any[]; events: string[];
  pathIdCol: string; pathCols: string[]; isLoading: boolean; isStatic?: boolean;
  onOpenFeatures: () => void; onOpenMetrics: () => void; onMethodChange: (m: string) => void;
  onScalerChange: (s: string) => void; onNClustersChange: (n: string) => void;
  onNmfToggle: (v: boolean) => void; onNmfKChange: (v: string) => void;
  onMetricsChange: (m: any[]) => void;
  onPathIdColChange: (c: string) => void; onApply: () => void; hasResult: boolean;
  isDirty: boolean; onOpenSave: () => void;
}) {
  const showApply = isDirty || !hasResult;

  const sel: React.CSSProperties = { width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 6, color: "#111827", fontSize: 12, padding: "5px 24px 5px 8px", cursor: "pointer", outline: "none", appearance: "none", backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%236b7280' stroke-width='2'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E")`, backgroundRepeat: "no-repeat", backgroundPosition: "right 6px center", background: "#f9fafb" };

  return (
    <div style={{ width: 260, minWidth: 260, height: "100%", background: "#fff", borderLeft: "1px solid #e5e7eb", display: "flex", flexDirection: "column", overflow: "hidden", fontFamily: "system-ui, sans-serif" }}>
      <div style={{ padding: "10px 14px", borderBottom: "1px solid #e5e7eb", display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#111827" }}>Settings</span>
        {isLoading && <span style={{ fontSize: 11, color: "#6b7280" }}>Computing…</span>}
      </div>
      <div style={{ flex: 1, overflowY: "auto", padding: 12 }}>

        {/* Path Column — top */}
        {pathCols.length > 1 && (
          <div style={{ marginBottom: 14 }}>
            <SidebarFL>Path Column</SidebarFL>
            <select value={pathIdCol} onChange={e => onPathIdColChange(e.target.value)} style={sel} disabled={isStatic}>
              {pathCols.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
        )}

        {/* Clustering section */}
        <SidebarSection title="Clustering">
          <div style={{ marginBottom: 10 }}>
            <SidebarFL>Features</SidebarFL>
            <FeaturesTriggerButton count={features.length} onClick={onOpenFeatures} disabled={isStatic} />
          </div>

          <div style={{ marginBottom: 10 }}>
            <SidebarFL>Feature Scaling</SidebarFL>
            <select value={scaler} onChange={e => onScalerChange(e.target.value)} style={sel} disabled={isStatic}>
              <option value="minmax">MinMax</option>
              <option value="std">Standard</option>
              <option value="">None</option>
            </select>
          </div>

          <div style={{ marginBottom: 10 }}>
            <SidebarFL>N Clusters</SidebarFL>
            <NCInput value={nClustersRaw} onChange={onNClustersChange}
              placeholder="e.g. 3-8 or 4 or 3,5,7" disabled={isStatic} />
            <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 3 }}>Single → fixed · 3-8 or 3,5,7 → silhouette grid</div>
          </div>

          {/* NMF toggle */}
          <div>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: nmfEnabled ? 8 : 0 }}>
              <SidebarFL>NMF Decomposition</SidebarFL>
              <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: isStatic ? "default" : "pointer" }}>
                <div
                  onClick={() => !isStatic && onNmfToggle(!nmfEnabled)}
                  style={{ width: 32, height: 18, borderRadius: 9, background: nmfEnabled ? "var(--retentioneering-yellow)" : "#d1d5db", position: "relative", cursor: isStatic ? "default" : "pointer", transition: "background 0.2s" }}
                >
                  <div style={{ position: "absolute", top: 2, left: nmfEnabled ? 16 : 2, width: 14, height: 14, borderRadius: "50%", background: "#fff", transition: "left 0.2s" }} />
                </div>
              </label>
            </div>
            {nmfEnabled && (
              <NCInput value={nmfKRaw} onChange={onNmfKChange}
                placeholder="e.g. 3-7 or 3,5,7" disabled={isStatic} />
            )}
          </div>
        </SidebarSection>

        {/* Cluster Analysis section */}
        <SidebarSection title="Cluster Analysis">
          <div style={{ marginBottom: 10 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 5 }}>
              <SidebarFL>Metrics</SidebarFL>
              <InfoTip text={"Overview metrics are computed after clustering and shown in the heatmap table.\nThey describe each cluster — e.g. average session length, event counts, conversion rates.\nChoose metrics that help you interpret and compare the resulting clusters."} />
            </div>
            <MetricsTriggerButton count={metricsConfig.length} onClick={onOpenMetrics} disabled={isStatic} />
          </div>
        </SidebarSection>

        {!isStatic && hasResult && (
          <SidebarSection title="Save Clusters">
            <SaveClustersTriggerButton onClick={onOpenSave} />
          </SidebarSection>
        )}

      </div>

      {/* Apply — shown when settings changed, or nothing has been computed yet (e.g. defaults) */}
      {!isStatic && showApply && features.length > 0 && (
        <div style={{ padding: "10px 12px", borderTop: "1px solid #e5e7eb", flexShrink: 0 }}>
          <button onClick={onApply} disabled={isLoading}
            style={{ width: "100%", padding: "8px 0", background: "var(--retentioneering-yellow)", border: "none", borderRadius: 6, cursor: "pointer", color: "#1a1a1a", fontSize: 12, fontWeight: 600 }}>
            Apply
          </button>
        </div>
      )}

    </div>
  );
}

// Reuse MetricsTriggerButton pattern from segment_overview
function MetricsTriggerButton({ count, onClick, disabled }: { count: number; onClick: () => void; disabled?: boolean }) {
  return (
    <button onClick={onClick} disabled={disabled} style={{ width: "100%", padding: "6px 10px", border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", cursor: disabled ? "default" : "pointer", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 12, color: "#374151", fontWeight: 500 }}>
      <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>
        </svg>
        Configure Metrics
      </span>
      {count > 0 && <span style={{ fontSize: 10, color: "#9ca3af", background: "#f3f4f6", borderRadius: 10, padding: "1px 6px" }}>{count}</span>}
    </button>
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

// ── tab bar ────────────────────────────────────────────────────────────────

function Tabs({ tabs, active, onChange }: { tabs: string[]; active: string; onChange: (t: string) => void }) {
  return (
    <div style={{ padding: "6px 12px", flexShrink: 0 }}>
      <div style={{ display: "inline-flex", alignItems: "center", background: "#f3f4f6", borderRadius: 8, padding: 3, gap: 2 }}>
        {tabs.map(t => (
          <button key={t} onClick={() => onChange(t)} style={{
            padding: "4px 12px", fontSize: 12, border: "none", cursor: "pointer", borderRadius: 6,
            background: active === t ? "#fff" : "transparent",
            color: active === t ? "#111827" : "#6b7280",
            fontWeight: active === t ? 500 : 400,
            boxShadow: active === t ? "0 1px 3px rgba(0,0,0,0.08)" : "none",
            transition: "all 0.1s",
          }}>{t}</button>
        ))}
      </div>
    </div>
  );
}

// ── main render ────────────────────────────────────────────────────────────

export function render({ model, el, isStatic = false }: RenderContext) {
  function App() {
    const [features,      setFeaturesState]  = React.useState<any[]>(() => parseJson(model.get("features"), []));
    const [method,        setMethodState]    = React.useState<string>(() => (model.get("method") as string) || "kmeans");
    const [scaler,        setScalerState]    = React.useState<string>(() => (model.get("scaler") as string) || "minmax");
    const [nClusters,     setNClusters]      = React.useState<string>(() => (model.get("n_clusters") as string) || "");
    const [nmfEnabled,    setNmfEnabled]     = React.useState<boolean>(() => (model.get("nmf_enabled") as boolean) ?? false);
    const [nmfK,          setNmfK]           = React.useState<string>(() => (model.get("nmf_components") as string) || "");
    const [metricsConfig, setMetricsConfig]  = React.useState<any[]>(() => parseJson(model.get("overview_metrics"), []));
    const [aggregation,   setAggregation]    = React.useState<string>(() => (model.get("aggregation") as string) || "mean");
    const [pathIdCol,     setPathIdColState] = React.useState<string>(() => (model.get("path_col") as string) || "");
    const [result,        setResult]         = React.useState<ClusterResult>(() => parseJson(model.get("result"), {}));
    const [isLoading,     setIsLoading]      = React.useState<boolean>(() => (model.get("is_loading") as boolean) ?? false);
    const [error,         setError]          = React.useState<string>(() => (model.get("error") as string) || "");
    const [height,        setHeight]         = React.useState<number>(() => (model.get("height") as number) ?? 520);
    const [sidebarOpen,   setSidebarOpen]    = React.useState<boolean>(() => (model.get("sidebar_open") as boolean) ?? true);
    const [featuresOpen,  setFeaturesOpen]   = React.useState(false);
    const [metricsOpen,   setMetricsOpen]    = React.useState(false);
    const [saveOpen,      setSaveOpen]       = React.useState(false);
    const [saveResult,    setSaveResult]     = React.useState<SaveResult | null>(() => {
      const r = parseJson<SaveResult>(model.get("save_result"), {});
      return r && Object.keys(r).length > 0 ? r : null;
    });
    const [chosenParams,  setChosenParams]   = React.useState<Record<string, any>>(() => parseJson(model.get("chosen_params"), {}));
    // Cluster renames typed directly into the heatmap header — cleared on every new
    // result since "cluster_0" may refer to a different cluster after re-clustering.
    const [headerRename,  setHeaderRename]   = React.useState<Record<string, string>>({});
    const rootRef = React.useRef<HTMLDivElement>(null);
    const [activeTab,     setActiveTab]      = React.useState("Overview");

    const events        = parseJson<string[]>(model.get("event_list"),     []);
    const pathCols      = parseJson<string[]>(model.get("path_cols"),      []);
    const segmentCols   = parseJson<string[]>(model.get("segment_cols"),   []);
    const segmentLevels = parseJson<Record<string,string[]>>(model.get("segment_levels"), {});
    const streamVarName = (model.get("stream_var_name") as string) || "stream";

    const {
      selected, ctxMenu, ctxMenuCanCompare, distModal, distLoading,
      handleCellClick, handleCellRightClick, handleShowDist,
      closeCtxMenu, closeDistModal,
    } = useDistributionSelection({
      model, result: result.overview ?? null, rootRef,
      segmentCol: CLUSTER_SEGMENT_COL, pathIdCol, events,
    });

    React.useEffect(() => {
      const subs: Array<[string, () => void]> = [
        ["result",      () => { setResult(parseJson(model.get("result"), {})); setHeaderRename({}); }],
        ["is_loading",  () => setIsLoading((model.get("is_loading") as boolean) ?? false)],
        ["error",       () => setError((model.get("error") as string) || "")],
        ["height",      () => setHeight((model.get("height") as number) ?? 520)],
        ["sidebar_open",() => setSidebarOpen((model.get("sidebar_open") as boolean) ?? true)],
        ["features",    () => setFeaturesState(parseJson(model.get("features"), []))],
        ["method",      () => setMethodState((model.get("method") as string) || "kmeans")],
        ["scaler",      () => setScalerState((model.get("scaler") as string) || "minmax")],
        ["n_clusters",     () => setNClusters((model.get("n_clusters") as string) || "")],
        ["nmf_enabled",    () => setNmfEnabled((model.get("nmf_enabled") as boolean) ?? false)],
        ["nmf_components",          () => setNmfK((model.get("nmf_components") as string) || "")],
        ["overview_metrics", () => setMetricsConfig(parseJson(model.get("overview_metrics"), []))],
        ["aggregation",    () => setAggregation((model.get("aggregation") as string) || "mean")],
        ["path_col",    () => setPathIdColState((model.get("path_col") as string) || "")],
        ["save_result", () => {
          const r = parseJson<SaveResult>(model.get("save_result"), {});
          setSaveResult(r && Object.keys(r).length > 0 ? r : null);
        }],
        ["chosen_params", () => setChosenParams(parseJson(model.get("chosen_params"), {}))],
      ];
      subs.forEach(([k, cb]) => model.on(`change:${k}`, cb));
      return () => subs.forEach(([k, cb]) => model.off(`change:${k}`, cb));
    }, []);

    const setFeatures   = (f: any[]) => { setFeaturesState(f); model.set("features", JSON.stringify(f)); model.save_changes(); };
    const setMethod     = (m: string) => { setMethodState(m); model.set("method", m); model.save_changes(); };
    const setScaler     = (s: string) => { setScalerState(s); model.set("scaler", s); model.save_changes(); };
    // Text fields: only update local React state, no model.set() on keystroke.
    // handleApply syncs everything to Python when the user clicks Apply.
    const setNC         = (n: string) => setNClusters(n);
    const setNmf        = (v: boolean) => { setNmfEnabled(v); model.set("nmf_enabled", v); model.save_changes(); };
    const setNmfKVal    = (v: string) => setNmfK(v);
    const setMetrics    = (m: any[]) => { setMetricsConfig(m); model.set("overview_metrics", JSON.stringify(m)); model.save_changes(); };
    const setAgg        = (a: string) => { setAggregation(a); model.set("aggregation", a); model.save_changes(); };
    const setPathId     = (c: string) => { setPathIdColState(c); model.set("path_col", c); model.save_changes(); };
    const handleToggle  = () => { setSidebarOpen(p => { const n = !p; model.set("sidebar_open", n); model.save_changes(); return n; }); };

    const handleApplyInplace = (name: string, rename: Record<string, string>) => {
      model.set("save_segment_name", name);
      model.set("save_rename", JSON.stringify(rename));
      model.set("save_trigger", Date.now().toString());
      model.save_changes();
    };

    const handleApply = () => {
      model.set("features",       JSON.stringify(features));
      model.set("method",         method);
      model.set("scaler",         scaler);
      model.set("n_clusters",     nClusters);
      model.set("nmf_enabled",    nmfEnabled);
      model.set("nmf_components",          nmfK);
      model.set("overview_metrics", JSON.stringify(metricsConfig));
      model.set("aggregation",    aggregation);
      model.set("path_col",    pathIdCol);
      model.set("apply_trigger",  Date.now().toString());
      model.save_changes();
      setLastApplied({ features, method, scaler, nClusters, nmfEnabled, nmfK, pathIdCol, metricsConfig });
    };

    const [lastApplied, setLastApplied] = React.useState(() => ({
      features:   parseJson<any[]>(model.get("features"), []),
      method:     (model.get("method") as string) || "kmeans",
      scaler:     (model.get("scaler") as string) || "standard",
      nClusters:  (model.get("n_clusters") as string) || "3-8",
      nmfEnabled: (model.get("nmf_enabled") as boolean) ?? false,
      nmfK:       (model.get("nmf_components") as string) || "",
      pathIdCol:  (model.get("path_col") as string) || "",
      metricsConfig: parseJson<any[]>(model.get("overview_metrics"), []),
    }));

    const isDirty = JSON.stringify(features) !== JSON.stringify(lastApplied.features)
                 || method     !== lastApplied.method
                 || scaler     !== lastApplied.scaler
                 || nClusters  !== lastApplied.nClusters
                 || nmfEnabled !== lastApplied.nmfEnabled
                 || nmfK       !== lastApplied.nmfK
                 || pathIdCol  !== lastApplied.pathIdCol
                 || JSON.stringify(metricsConfig) !== JSON.stringify(lastApplied.metricsConfig);

    // Available tabs based on result
    const tabs: string[] = [];
    if (result.overview)    tabs.push("Overview");
    if (result.silhouette)  tabs.push("Silhouette");
    if (result.nmf)         tabs.push("H-matrix");
    if (result.nmf?.W_cluster_means && Object.keys(result.nmf.W_cluster_means).length > 0) tabs.push("W Cluster Means");
    const tab = tabs.includes(activeTab) ? activeTab : (tabs[0] || "Overview");
    const hasResult = !!(result.overview || result.silhouette || result.nmf);

    return (
      <div ref={rootRef} style={{ position: "relative", display: "flex", flexDirection: "row", height, background: "#fff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
        <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0, display: "flex", flexDirection: "column" }}>
          <SidebarToggle onClick={handleToggle} />
          <div style={{ width: "100%", height: "100%" }}>
            <div style={{ display: "flex", flexDirection: "column", width: "100%", height: "100%", overflow: "hidden" }}>
            {tabs.length > 1 && <Tabs tabs={tabs} active={tab} onChange={setActiveTab} />}
            <div style={{ flex: 1, overflow: "auto", padding: "8px 0" }}>
              {error && !isLoading && (
                <div style={{ margin: 16, padding: 12, background: "#fff1f2", border: "1px solid #fca5a5", borderRadius: 8, fontSize: 12, color: "#dc2626", fontFamily: "monospace", whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
                  {error}
                </div>
              )}
              {!result.overview && !result.silhouette && !isLoading && !error && (
                <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "#9ca3af", fontSize: 13 }}>
                  Add features and click Apply to run clustering
                </div>
              )}
              {tab === "Overview"        && result.overview   && (
                <SegmentOverviewTable
                  data={result.overview}
                  renameMap={headerRename}
                  onRename={(orig, next) => setHeaderRename(prev => ({ ...prev, [orig]: next }))}
                  onCellClick={handleCellClick}
                  onCellRightClick={isStatic ? () => {} : handleCellRightClick}
                  selectedCells={selected}
                  resizableLabelColumn
                  valueColumnMaxWidth={100}
                />
              )}
              {tab === "Silhouette"      && result.silhouette  && <div style={{ padding: "0 16px" }}><SilhouetteChart data={result.silhouette} /></div>}
              {tab === "H-matrix"        && result.nmf         && <div style={{ padding: "0 16px" }}><NmfHMatrix nmf={result.nmf} /></div>}
              {tab === "W Cluster Means" && result.nmf         && <div style={{ padding: "0 16px" }}><NmfWMatrix nmf={result.nmf} /></div>}
            </div>
            </div>
          </div>
          {(isLoading || distLoading) && <ComputingSpinner label={isLoading ? "Clustering…" : undefined} />}
        </div>
        {sidebarOpen && (
          <Sidebar
            features={features} method={method} scaler={scaler}
            nClustersRaw={nClusters} nmfEnabled={nmfEnabled} nmfKRaw={nmfK}
            metricsConfig={metricsConfig} events={events}
            pathIdCol={pathIdCol} pathCols={pathCols} isLoading={isLoading}
            isStatic={isStatic}
            onOpenFeatures={() => setFeaturesOpen(true)}
            onOpenMetrics={() => setMetricsOpen(true)}
            onMethodChange={setMethod} onScalerChange={setScaler}
            onNClustersChange={setNC} onNmfToggle={setNmf} onNmfKChange={setNmfKVal}
            onMetricsChange={setMetrics}
            onPathIdColChange={setPathId} onApply={handleApply} isDirty={isDirty} hasResult={hasResult}
            onOpenSave={() => setSaveOpen(true)}
          />
        )}

        {!isStatic && saveOpen && (
          <SaveClustersOverlay
            segments={result.overview?.segments ?? []}
            initialRename={headerRename}
            streamVarName={streamVarName}
            features={features} method={method} scaler={scaler}
            chosenParams={chosenParams} pathCol={pathIdCol}
            onApplyInplace={handleApplyInplace}
            onClose={() => setSaveOpen(false)}
            saveResult={saveResult}
          />
        )}

        {/* Features modal — position:absolute avoids VS Code transform issues */}
        {!isStatic && featuresOpen && (
          <FeaturesOverlay
            features={features} events={events} segmentCols={segmentCols} segmentLevels={segmentLevels}
            onFeaturesChange={setFeatures}
            onClose={() => setFeaturesOpen(false)}
          />
        )}

        {!isStatic && metricsOpen && (
          <MetricsOverlay
            metrics={metricsConfig} events={events} segmentCols={segmentCols} segmentLevels={segmentLevels}
            onMetricsChange={setMetrics}
            onClose={() => setMetricsOpen(false)}
          />
        )}

        {/* Context menu — disabled in static mode (no backend for dist_request) */}
        {!isStatic && ctxMenu && (
          <ContextMenu
            x={ctxMenu.x} y={ctxMenu.y}
            canCompare={ctxMenuCanCompare}
            onShowDist={handleShowDist}
            onClose={closeCtxMenu}
          />
        )}

        {!isStatic && distModal && (
          <DistributionModal
            result={distModal.result}
            label1={distModal.label1}
            label2={distModal.label2}
            metricName={distModal.metric}
            onClose={closeDistModal}
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
