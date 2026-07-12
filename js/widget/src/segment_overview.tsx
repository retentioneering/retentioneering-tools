/**
 * Segment Overview Widget
 * Heatmap table: rows = metrics, columns = segment values.
 * Click a cell to see the metric distribution for that segment.
 * Click two cells in the same row to compare their distributions.
 */
import * as React from "react";
import { createRoot } from "react-dom/client";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes } from "./widget-utils";
import { MetricRow, validateMetricCfg } from "./metric_config_row";
import {
  AnyWidgetModel, SegmentOverviewData, SegmentOverviewTable,
  ContextMenu, DistributionModal, useDistributionSelection,
} from "./segment_overview_table";

interface RenderContext { model: AnyWidgetModel; el: HTMLElement; isStatic?: boolean; }

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
  const addMetric    = () => onMetricsChange([...metrics, { metric: "event_count_bulk", agg: "mean", metric_args: undefined }]);
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
    const [error,      setError]          = React.useState<string>(() => (model.get("error") as string) || "");
    const [height,     setHeight]         = React.useState<number>(() => (model.get("height") as number) ?? 480);
    const [sidebarOpen, setSidebarOpen]   = React.useState<boolean>(() => (model.get("sidebar_open") as boolean) ?? true);

    // Modals — rendered at root level with position:absolute to work in VS Code
    const [metricsOpen, setMetricsOpen] = React.useState(false);
    const rootRef = React.useRef<HTMLDivElement>(null);

    const segCols   = parseJson<string[]>(model.get("segment_cols"), []);
    const pathCols  = parseJson<string[]>(model.get("path_cols"),    []);
    const events    = parseJson<string[]>(model.get("event_list"),   []);
    const segLevels = parseJson<Record<string,string[]>>(model.get("segment_levels"), {});

    React.useEffect(() => {
      const subs: Array<[string, () => void]> = [
        ["result",     () => { const r = parseJson<any>(model.get("result"), null); setResult(r?.metrics ? r : null); }],
        ["is_loading", () => setIsLoading((model.get("is_loading") as boolean) ?? false)],
        ["error",      () => setError((model.get("error") as string) || "")],
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

    const {
      selected, ctxMenu, ctxMenuCanCompare, distModal, distLoading,
      handleCellClick, handleCellRightClick, handleShowDist,
      closeCtxMenu, closeDistModal,
    } = useDistributionSelection({ model, result, rootRef, segmentCol: segCol, pathIdCol, events });

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
          <div style={{ width: "100%", height: "100%", display: "flex", flexDirection: "column" }}>
            {error && !isLoading && (
              <div style={{ margin: 16, padding: 12, background: "#fff1f2", border: "1px solid #fca5a5", borderRadius: 8, fontSize: 12, color: "#dc2626", fontFamily: "monospace", whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
                {error}
              </div>
            )}
            {!result && !isLoading && !error && (
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
                <SegmentOverviewTable
                  data={result}
                  onCellClick={handleCellClick}
                  onCellRightClick={isStatic ? () => {} : handleCellRightClick}
                  selectedCells={selected}
                  resizableLabelColumn
                  valueColumnMaxWidth={100}
                />
              </div>
            )}
          </div>

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
            canCompare={ctxMenuCanCompare}
            onShowDist={handleShowDist}
            onClose={closeCtxMenu}
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
