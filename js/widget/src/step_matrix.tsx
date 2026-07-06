import * as React from "react";
import { createRoot } from "react-dom/client";
import { createPortal } from "react-dom";
import { parseJson, ComputingSpinner, RetentioneeringSpinKeyframes } from "./widget-utils";

interface AnyWidgetModel {
  get(key: string): unknown;
  set(key: string, value: unknown): void;
  save_changes(): void;
  on(event: string, cb: () => void): void;
  off(event: string, cb: () => void): void;
}
interface RenderContext { model: AnyWidgetModel; el: HTMLElement; isStatic?: boolean; }

// ── types ──────────────────────────────────────────────────────────────────

interface MatrixBlock {
  events: string[];
  values: number[][];
  columns: number[];
  group1: MatrixBlock | null;
  group2: MatrixBlock | null;
}
interface MatrixResult {
  matrices: MatrixBlock[];
  event_counts: Record<string, number>;
  event_counts_g1?: Record<string, number>;
  event_counts_g2?: Record<string, number>;
}

// ── colour ─────────────────────────────────────────────────────────────────

function cellBg(v: number, min: number, max: number, isDiff: boolean, heatmapType: "overall"|"row"|"col"): string {
  if (isDiff) {
    if (Math.abs(v) < 0.005) return "transparent";
    if (v < 0) { const n = Math.min(1, Math.abs(v / (min || -1))); return `hsla(217,91%,60%,${n.toFixed(2)})`; }
    const n = Math.min(1, v / (max || 1)); return `hsla(0,84%,60%,${n.toFixed(2)})`;
  }
  if (Math.abs(v) < 0.005) return "transparent";
  const n = Math.max(0, Math.min(1, (v - min) / ((max - min) || 1)));
  return `hsla(25,95%,53%,${n.toFixed(2)})`;
}

function fmtVal(v: number): string {
  if (Math.abs(v) < 0.005) return "-";
  return v.toFixed(2);
}

function truncateMid(s: string, maxLen = 22): string {
  if (s.length <= maxLen) return s;
  const half = Math.floor((maxLen - 1) / 2);
  return s.slice(0, half) + "…" + s.slice(-half);
}

function fmtCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

function EyeIcon({ hidden }: { hidden: boolean }) {
  return hidden ? (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9.88 9.88a3 3 0 1 0 4.24 4.24"/>
      <path d="M10.73 5.08A10.43 10.43 0 0 1 12 5c7 0 10 7 10 7a13.16 13.16 0 0 1-1.67 2.68"/>
      <path d="M6.61 6.61A13.526 13.526 0 0 0 2 12s3 7 10 7a9.74 9.74 0 0 0 5.39-1.61"/>
      <line x1="2" y1="2" x2="22" y2="22"/>
    </svg>
  ) : (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7Z"/>
      <circle cx="12" cy="12" r="3"/>
    </svg>
  );
}

function PinIcon({ pinned }: { pinned: boolean }) {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill={pinned ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="17" x2="12" y2="22"/>
      <path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6h1a2 2 0 0 0 0-4H8a2 2 0 0 0 0 4h1v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z"/>
    </svg>
  );
}

function SortIcon() {
  return (
    <svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="5" x2="12" y2="19"/>
      <polyline points="19 12 12 19 5 12"/>
    </svg>
  );
}

function LexSortIcon({ dir }: { dir: "asc" | "desc" | null }) {
  if (dir === "asc") return (
    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/>
    </svg>
  );
  if (dir === "desc") return (
    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/>
    </svg>
  );
  return (
    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="m3 16 4 4 4-4"/><path d="M7 20V4"/><path d="m21 8-4-4-4 4"/><path d="M17 4v16"/>
    </svg>
  );
}

const iconBtnStyle = (active: boolean, activeColor: string): React.CSSProperties => ({
  display: "flex", alignItems: "center", justifyContent: "center",
  width: 18, height: 20, borderRadius: 3,
  background: "transparent", border: "none", cursor: "pointer",
  color: active ? activeColor : "#9ca3af",
  flexShrink: 0, padding: 0,
});

// ── sliders (matching TransitionGraph / SettingsSidebar style) ─────────────

const THUMB: React.CSSProperties = { position: "absolute", width: 16, height: 16, borderRadius: "50%", background: "#475569", border: "1px solid #94a3b8", cursor: "ew-resize", transform: "translateX(-50%)", top: "50%", marginTop: -8, zIndex: 1 };

function RangeSlider({ min, max, value, onChange, scale = "linear", formatValue, compact = false }: {
  min: number; max: number; value: [number, number]; onChange: (v: [number, number]) => void;
  scale?: "linear" | "log"; formatValue?: (v: number) => string; compact?: boolean;
}) {
  const trackRef = React.useRef<HTMLDivElement>(null);
  const fmt = formatValue ?? ((v: number) => (v * 100).toFixed(0) + "%");

  const toPos = (v: number): number => {
    if (v <= min) return 0;
    if (v >= max) return 1;
    if (scale === "log" && min > 0 && max > 0)
      return (Math.log(v) - Math.log(min)) / (Math.log(max) - Math.log(min));
    return (v - min) / ((max - min) || 1);
  };
  const fromPos = (pos: number): number => {
    if (scale === "log" && min > 0 && max > 0)
      return Math.exp(Math.log(min) + pos * (Math.log(max) - Math.log(min)));
    return min + pos * (max - min);
  };

  const lp = toPos(value[0]) * 100;
  const rp = toPos(value[1]) * 100;

  const drag = (thumb: "lo" | "hi") => (e: React.MouseEvent) => {
    e.preventDefault(); e.stopPropagation();
    const move = (ev: MouseEvent) => {
      const rect = trackRef.current?.getBoundingClientRect(); if (!rect) return;
      const pos = Math.max(0, Math.min(1, (ev.clientX - rect.left) / rect.width));
      const v = fromPos(pos);
      if (thumb === "lo") onChange([Math.min(v, value[1]), value[1]]);
      else onChange([value[0], Math.max(v, value[0])]);
    };
    const up = () => { document.removeEventListener("mousemove", move); document.removeEventListener("mouseup", up); };
    document.addEventListener("mousemove", move); document.addEventListener("mouseup", up);
    move(e.nativeEvent as MouseEvent);
  };

  return (
    <div>
      {!compact && (
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4, fontSize: 12, color: "#6b7280" }}>
          <span>{fmt(value[0])}</span><span>–</span><span>{fmt(value[1])}</span>
        </div>
      )}
      <div ref={trackRef} style={{ position: "relative", height: 20, display: "flex", alignItems: "center", userSelect: "none" }}>
        <div style={{ width: "100%", height: 4, background: "#e2e8f0", borderRadius: 9999, position: "relative" }}>
          <div style={{ position: "absolute", left: `${lp}%`, right: `${100 - rp}%`, height: "100%", background: "#94a3b8", borderRadius: 9999 }} />
        </div>
        <div style={{ ...THUMB, left: `${lp}%` }} onMouseDown={drag("lo")} />
        <div style={{ ...THUMB, left: `${rp}%` }} onMouseDown={drag("hi")} />
      </div>
    </div>
  );
}

// ── matrix table ──────────────────────────────────────────────────────────

const CELL_W = 34;
const SORT_BTN_W = 14;
const MIN_LABEL_W = 80;
const HEADER_H = 30;
const SEP_W = 28;

function serratedEdges(h: number, w = SEP_W, serH = 8, serD = 5): { right: string; left: string } {
  let right = `M ${w} 0`;
  let left  = `M 0 0`;
  for (let y = 0; y < h; y += serH) {
    const ny = Math.min(y + serH, h);
    const mid = y + (ny - y) / 2;
    right += ` L ${w - serD} ${mid} L ${w} ${ny}`;
    left  += ` L ${serD} ${mid} L 0 ${ny}`;
  }
  return { right, left };
}

const SEP_CELL: React.CSSProperties = {
  width: SEP_W, minWidth: SEP_W, padding: 0,
  borderLeft: "none", borderRight: "none",
  borderTopStyle: "hidden", borderBottomStyle: "hidden",
  background: "#e5e7eb",
};

function SepSvg({ height }: { height: number }) {
  const { right, left } = serratedEdges(height);
  return (
    <svg width={SEP_W} height={height} viewBox={`0 0 ${SEP_W} ${height}`} style={{ display: "block" }}>
      <path d={right} fill="none" stroke="#d1d5db" strokeWidth="1" strokeLinejoin="round" />
      <path d={left}  fill="none" stroke="#d1d5db" strokeWidth="1" strokeLinejoin="round" />
    </svg>
  );
}

function SepTh({ height }: { height: number }) {
  return <th style={{ ...SEP_CELL, position: "sticky", top: 0, zIndex: 2, height }}><SepSvg height={height} /></th>;
}
function SepTd() {
  return <td style={{ ...SEP_CELL, height: 20 }}><SepSvg height={18} /></td>;
}

// ── Edge serrations (open ends of a pattern) ──────────────────────────────
const EDGE_W = 14;
const EDGE_CELL: React.CSSProperties = {
  width: EDGE_W, minWidth: EDGE_W, padding: 0,
  borderLeft: "none", borderRight: "none",
  borderTopStyle: "hidden", borderBottomStyle: "hidden",
  background: "#e5e7eb",
};

function EdgeSvg({ height, side }: { height: number; side: "left" | "right" }) {
  // side="right": strip after last block, zigzag on its LEFT edge (x=0)
  // side="left":  strip before first block, zigzag on its RIGHT edge (x=EDGE_W)
  const serH = 8, serD = 5;
  const x = side === "right" ? 0 : EDGE_W;
  const xi = side === "right" ? serD : EDGE_W - serD;
  let d = `M ${x} 0`;
  for (let y = 0; y < height; y += serH) {
    const ny = Math.min(y + serH, height), mid = y + (ny - y) / 2;
    d += ` L ${xi} ${mid} L ${x} ${ny}`;
  }
  return (
    <svg width={EDGE_W} height={height} viewBox={`0 0 ${EDGE_W} ${height}`} style={{ display: "block" }}>
      <path d={d} fill="none" stroke="#d1d5db" strokeWidth="1" strokeLinejoin="round" />
    </svg>
  );
}

function EdgeTh({ height, side }: { height: number; side: "left" | "right" }) {
  return <th style={{ ...EDGE_CELL, position: "sticky", top: 0, zIndex: 2, height }}><EdgeSvg height={height} side={side} /></th>;
}
function EdgeTd({ side }: { side: "left" | "right" }) {
  return <td style={{ ...EDGE_CELL, height: 20 }}><EdgeSvg height={18} side={side} /></td>;
}

interface RowDragState {
  isDraggedRow: boolean; isDragOver: boolean; dragOverPos: "above" | "below";
  onDragStart: (e: React.DragEvent) => void;
  onDragOver:  (e: React.DragEvent) => void;
  onDragLeave: (e: React.DragEvent) => void;
  onDrop:      (e: React.DragEvent) => void;
  onDragEnd:   () => void;
}

// Multi-block row: label cell + all blocks' cells side by side
function EventRow({ ev, blocks, allColIndices, blockEvMin, blockEvMax, blockHasBothSigns, isDiff, labelWidth, heatmapType, isHidden, isPinned, onToggleHidden, onTogglePin, eventCount, eventCountG1, eventCountG2, showLeftEdge, showRightEdge, onCellHover, dimmed, dragState }: {
  ev: string;
  blocks: MatrixBlock[]; allColIndices: number[][];
  blockEvMin: Array<(ev: string, ci: number) => number>;
  blockEvMax: Array<(ev: string, ci: number) => number>;
  blockHasBothSigns: boolean[];
  isDiff: boolean; labelWidth: number; heatmapType: "overall"|"row"|"col";
  isHidden: boolean; isPinned: boolean;
  onToggleHidden: (ev: string) => void; onTogglePin: (ev: string) => void;
  eventCount?: number;
  eventCountG1?: number; eventCountG2?: number;
  showLeftEdge?: boolean; showRightEdge?: boolean;
  onCellHover?: (tip: { x: number; y: number; ev: string; col: number; v: number; g1: number | null; g2: number | null } | null) => void;
  dimmed?: boolean; dragState?: RowDragState;
}) {
  const trStyle: React.CSSProperties = {
    ...(dimmed ? { opacity: 0.45 } : {}),
    ...(dragState?.isDraggedRow ? { opacity: 0.35 } : {}),
    ...(dragState?.isDragOver ? {
      boxShadow: dragState.dragOverPos === "above" ? "inset 0 2px 0 #f59e0b" : "inset 0 -2px 0 #f59e0b",
    } : {}),
  };

  return (
    <tr data-event={ev} style={trStyle}
      draggable={!!dragState}
      onDragStart={dragState?.onDragStart}
      onDragOver={dragState?.onDragOver}
      onDragLeave={dragState?.onDragLeave}
      onDrop={dragState?.onDrop}
      onDragEnd={dragState?.onDragEnd}>
      {/* Sticky label cell */}
      <td className="event-label-cell"
        style={{ position: "sticky", left: 0, background: "#fff", zIndex: 1, height: 20, borderBottom: "1px solid #f3f4f6", borderRight: "2px solid #e5e7eb", color: "#374151", fontWeight: 500, width: labelWidth, minWidth: labelWidth, maxWidth: labelWidth, overflow: "hidden", whiteSpace: "nowrap", padding: 0, fontSize: 11,
          boxShadow: dragState?.isDragOver ? (dragState.dragOverPos === "above" ? "inset 0 2px 0 #f59e0b" : "inset 0 -2px 0 #f59e0b") : undefined }}
        onMouseEnter={e => { const b = e.currentTarget.querySelector(".row-icons") as HTMLElement; if (b) b.style.opacity = "1"; }}
        onMouseLeave={e => { const b = e.currentTarget.querySelector(".row-icons") as HTMLElement; if (b) b.style.opacity = "0"; }}>
        <div style={{ position: "relative", height: "100%", display: "flex", alignItems: "center", padding: "0 8px", cursor: dragState ? "grab" : undefined }}>
          {dragState && (
            <svg width="8" height="12" viewBox="0 0 8 12" style={{ flexShrink: 0, marginRight: 4, opacity: 0.3 }}>
              <circle cx="2" cy="2" r="1.5" fill="currentColor"/><circle cx="6" cy="2" r="1.5" fill="currentColor"/>
              <circle cx="2" cy="6" r="1.5" fill="currentColor"/><circle cx="6" cy="6" r="1.5" fill="currentColor"/>
              <circle cx="2" cy="10" r="1.5" fill="currentColor"/><circle cx="6" cy="10" r="1.5" fill="currentColor"/>
            </svg>
          )}
          <span title={ev} style={{ flex: 1, minWidth: 0, overflow: "hidden", whiteSpace: "nowrap", textAlign: "left" }}>
            {truncateMid(ev, Math.max(4, Math.floor((labelWidth - 16) / 6.0)))}
          </span>
          <span className="row-icons" style={{ position: "absolute", right: 0, top: 0, bottom: 0, display: "flex", alignItems: "center", gap: 4, paddingRight: 6, paddingLeft: 24, background: "linear-gradient(to right, transparent, #fff 22px)", opacity: 0, transition: "opacity 0.1s" }}
            onClick={e => e.stopPropagation()}>
            {isDiff && eventCountG1 !== undefined && eventCountG2 !== undefined ? (
              <span style={{ display: "flex", alignItems: "center", gap: 2, flexShrink: 0, fontVariantNumeric: "tabular-nums" }}>
                <span style={{ fontSize: 10, color: "#3b82f6" }}>{fmtCount(eventCountG1)}</span>
                <span style={{ fontSize: 9, color: "#9ca3af" }}>/</span>
                <span style={{ fontSize: 10, color: "#ef4444" }}>{fmtCount(eventCountG2)}</span>
              </span>
            ) : (
              <span style={{ fontSize: 10, color: "#9ca3af", fontVariantNumeric: "tabular-nums", width: 28, textAlign: "right", flexShrink: 0, visibility: eventCount !== undefined ? "visible" : "hidden" }}>
                {fmtCount(eventCount ?? 0)}
              </span>
            )}
            <button title={isHidden ? "Show event" : "Hide event"} onClick={() => onToggleHidden(ev)} style={iconBtnStyle(isHidden, "#ef4444")}><EyeIcon hidden={isHidden} /></button>
            <button title={isPinned ? "Unpin event" : "Pin event"}  onClick={() => onTogglePin(ev)}    style={iconBtnStyle(isPinned, "#f59e0b")}><PinIcon pinned={isPinned} /></button>
          </span>
        </div>
      </td>

      {showLeftEdge && <EdgeTd side="left" />}
      {/* Per-block cells */}
      {blocks.map((block, bi) => {
        const colIndices = allColIndices[bi];
        const ri = block.events.indexOf(ev);
        return (
          <React.Fragment key={bi}>
            {bi > 0 && <SepTd />}
            {colIndices.map(ci => {
              const col = block.columns[ci];
              const isAnchor = col === 0 && blockHasBothSigns[bi];
              const v = ri >= 0 ? (block.values[ri]?.[ci] ?? 0) : 0;
              const anchorBorder = "2px solid #e5e7eb";
              const thinBorder   = "1px solid #f3f4f6";
              const g1 = block.group1 ? (block.group1.events.indexOf(ev) >= 0 ? (block.group1.values[block.group1.events.indexOf(ev)]?.[ci] ?? null) : null) : null;
              const g2 = block.group2 ? (block.group2.events.indexOf(ev) >= 0 ? (block.group2.values[block.group2.events.indexOf(ev)]?.[ci] ?? null) : null) : null;
              const hoverHandlers = onCellHover ? {
                onMouseEnter: (e: React.MouseEvent) => onCellHover({ x: e.clientX, y: e.clientY, ev, col, v, g1, g2 }),
                onMouseLeave: () => onCellHover(null),
              } : {};
              const bg = cellBg(v, blockEvMin[bi](ev, ci), blockEvMax[bi](ev, ci), isDiff, heatmapType);
              return (
                <td key={`${bi}-${ci}`} data-step={String(col)} {...hoverHandlers}
                  style={{ width: CELL_W, height: 20, background: bg, textAlign: "center", borderBottom: thinBorder, borderRight: isAnchor ? anchorBorder : thinBorder, borderLeft: isAnchor ? anchorBorder : undefined, fontSize: 10, color: Math.abs(v) < 0.005 ? "#d1d5db" : "#111827", cursor: isDiff ? "default" : "default", padding: "0 2px" }}>
                  {fmtVal(v)}
                </td>
              );
            })}
          </React.Fragment>
        );
      })}
      {showRightEdge && <EdgeTd side="right" />}
    </tr>
  );
}

// Retentioneering-style sort: for each column in order, pick the remaining row with max value.
// anchorEv (if provided) is pinned to the top; the rest are sorted normally.
function sortByDirection(block: MatrixBlock, direction: "left" | "right", anchorEv: string | null): string[] {
  const colIndices = block.columns
    .map((c, i) => ({ c, i }))
    .filter(({ c }) => direction === "left" ? c < 0 : c > 0)
    .sort((a, b) => direction === "left" ? b.c - a.c : a.c - b.c)
    .map(({ i }) => i);
  if (colIndices.length === 0) return block.events;

  const remaining = new Set(anchorEv ? block.events.filter(e => e !== anchorEv) : block.events);
  const order: string[] = [];
  for (const ci of colIndices) {
    if (remaining.size === 0) break;
    let maxVal = -Infinity, maxEv: string | null = null;
    for (const ev of remaining) {
      const ri = block.events.indexOf(ev);
      const v = block.values[ri]?.[ci] ?? 0;
      if (maxEv === null || v > maxVal) { maxVal = v; maxEv = ev; }
    }
    if (maxEv !== null) { order.push(maxEv); remaining.delete(maxEv); }
  }
  order.push(...remaining);
  return anchorEv ? [anchorEv, ...order] : order;
}

function MatrixView({ blocks, stepWindow, isDiff, labelWidth, onLabelResize, hiddenEvents, filteredOut, pinnedEvents, onToggleHidden, onTogglePin, heatmapType, globalHeatmap, eventCounts, eventCountsG1, eventCountsG2, pathPattern, diffSeg, diffV1, diffV2 }: {
  blocks: MatrixBlock[]; stepWindow: number; isDiff: boolean; labelWidth: number;
  onLabelResize: (w: number) => void;
  hiddenEvents: Set<string>; filteredOut?: Set<string>; pinnedEvents: Set<string>;
  onToggleHidden: (ev: string) => void; onTogglePin: (ev: string) => void;
  heatmapType: "overall"|"row"|"col"; globalHeatmap?: boolean;
  eventCounts?: Record<string, number>;
  eventCountsG1?: Record<string, number>; eventCountsG2?: Record<string, number>;
  pathPattern?: string;
  diffSeg?: string | null; diffV1?: string | null; diffV2?: string | null;
}) {
  // ── Go To ─────────────────────────────────────────────────────────────────
  const [goToOpen,  setGoToOpen]  = React.useState(false);
  const [goToQuery, setGoToQuery] = React.useState("");
  const [goToRect,  setGoToRect]  = React.useState<DOMRect | null>(null);
  const scrollRef    = React.useRef<HTMLDivElement>(null);
  const goToInputRef = React.useRef<HTMLInputElement>(null);
  const dropdownRef  = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => { if (goToOpen) setTimeout(() => goToInputRef.current?.focus(), 0); }, [goToOpen]);
  React.useEffect(() => {
    if (!goToOpen) return;
    const close = (e: MouseEvent) => {
      if (dropdownRef.current?.contains(e.target as Node)) return;
      setGoToOpen(false); setGoToQuery("");
    };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, [goToOpen]);

  const allEvents = React.useMemo(() => {
    const seen = new Set<string>();
    blocks.forEach(b => b.events.forEach(e => seen.add(e)));
    return [...seen];
  }, [blocks]);

  const openGoTo = (e: React.MouseEvent<HTMLButtonElement>) => {
    setGoToRect((e.currentTarget.closest("th") as HTMLElement).getBoundingClientRect());
    setGoToOpen(v => !v);
  };
  const scrollToEvent = (ev: string) => {
    const container = scrollRef.current; if (!container) return;
    for (const row of container.querySelectorAll("tr[data-event]")) {
      if ((row as HTMLElement).dataset.event === ev) {
        (row as HTMLElement).scrollIntoView({ block: "nearest", behavior: "smooth" });
        const td = (row as HTMLElement).querySelector("td") as HTMLElement;
        if (td) { td.style.background = "#fef3c7"; setTimeout(() => { td.style.background = ""; }, 700); }
        break;
      }
    }
  };
  const goToFiltered = goToQuery
    ? allEvents.filter(e => e.toLowerCase().includes(goToQuery.toLowerCase()))
    : allEvents;

  // ── Resize handle ─────────────────────────────────────────────────────────
  const resizing = React.useRef(false);
  const startX   = React.useRef(0);
  const startW   = React.useRef(0);
  const handleRef = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    const onMove = (e: MouseEvent) => { if (!resizing.current) return; onLabelResize(Math.max(MIN_LABEL_W, startW.current + e.clientX - startX.current)); };
    const onUp   = () => { resizing.current = false; document.body.style.cursor = document.body.style.userSelect = ""; if (handleRef.current) handleRef.current.style.background = "transparent"; };
    document.addEventListener("mousemove", onMove); document.addEventListener("mouseup", onUp);
    return () => { document.removeEventListener("mousemove", onMove); document.removeEventListener("mouseup", onUp); };
  }, [onLabelResize]);

  // ── Pre-compute per-block column indices & bounds ─────────────────────────
  const allColIndices = React.useMemo(() =>
    blocks.map(block => {
      const zeroIdx = block.columns.findIndex(c => c === 0);
      if (zeroIdx >= 0 && stepWindow > 0) {
        const s = Math.max(0, zeroIdx - stepWindow);
        const e = Math.min(block.columns.length - 1, zeroIdx + stepWindow);
        return Array.from({ length: e - s + 1 }, (_, i) => s + i);
      }
      return block.columns.map((_, i) => i);
    }), [blocks, stepWindow]);

  const blockHasBothSigns = React.useMemo(() =>
    blocks.map(b => b.columns.some(c => c < 0) && b.columns.some(c => c > 0)),
    [blocks]);

  const blockHasNeg = React.useMemo(() => blocks.map(b => b.columns.some(c => c < 0)), [blocks]);
  const blockHasPos = React.useMemo(() => blocks.map(b => b.columns.some(c => c > 0)), [blocks]);

  const handleSort = React.useCallback((bi: number, direction: "left" | "right") => {
    const block = blocks[bi]; if (!block) return;
    // Use original (non-diff) values to find anchor: in diff mode block.values are differences,
    // so the centering event has value 0 there; group1 always has the real frequencies.
    const src = block.group1 ?? block;
    const col0ci = src.columns.indexOf(0);
    let anchorEv: string | null = null;
    if (col0ci >= 0) {
      let best = -Infinity;
      for (let ri = 0; ri < src.events.length; ri++) {
        const v = src.values[ri]?.[col0ci] ?? 0;
        if (v > best) { best = v; anchorEv = src.events[ri]; }
      }
    }
    setCustomOrder(sortByDirection(block, direction, anchorEv));
  }, [blocks]);


  const { blockEvMin, blockEvMax } = React.useMemo(() => {
    const blockEvMin: Array<(ev: string, ci: number) => number> = [];
    const blockEvMax: Array<(ev: string, ci: number) => number> = [];

    // Global bounds across all blocks (for Overall and By row when globalHeatmap=true)
    const useGlobal = globalHeatmap && (heatmapType === "overall" || heatmapType === "row");
    let gOverall = { min: 0, max: 1 };
    let gRowBnds: Record<string, { min: number; max: number }> = {};
    if (useGlobal) {
      const allVals = blocks.flatMap((block, bi) =>
        block.events.flatMap((_, ri) => allColIndices[bi].map(ci => block.values[ri]?.[ci] ?? 0))
      );
      gOverall = { min: Math.min(...allVals), max: Math.max(...allVals) };
      if (heatmapType === "row") {
        (blocks[0]?.events ?? []).forEach(ev => {
          const vals = blocks.flatMap((block, bi) => {
            const ri = block.events.indexOf(ev);
            return ri >= 0 ? allColIndices[bi].map(ci => block.values[ri]?.[ci] ?? 0) : [];
          });
          gRowBnds[ev] = { min: Math.min(...vals), max: Math.max(...vals) };
        });
      }
    }

    blocks.forEach((block, bi) => {
      const colIndices = allColIndices[bi];
      const colBnds = colIndices.map(ci => { const v = block.events.map((_, ri) => block.values[ri]?.[ci] ?? 0); return { min: Math.min(...v), max: Math.max(...v) }; });
      const rowBnds = block.events.map((_, ri) => { const v = colIndices.map(ci => block.values[ri]?.[ci] ?? 0); return { min: Math.min(...v), max: Math.max(...v) }; });
      const all = block.events.flatMap((_, ri) => colIndices.map(ci => block.values[ri]?.[ci] ?? 0));
      const overall = { min: Math.min(...all), max: Math.max(...all) };
      blockEvMin.push((ev, ci) => {
        const ri = block.events.indexOf(ev); const idx = colIndices.indexOf(ci);
        if (heatmapType === "row" && ri >= 0) return useGlobal ? (gRowBnds[ev]?.min ?? 0) : (rowBnds[ri]?.min ?? 0);
        if (heatmapType === "col" && idx >= 0) return colBnds[idx]?.min ?? 0;
        return useGlobal ? gOverall.min : overall.min;
      });
      blockEvMax.push((ev, ci) => {
        const ri = block.events.indexOf(ev); const idx = colIndices.indexOf(ci);
        if (heatmapType === "row" && ri >= 0) return useGlobal ? (gRowBnds[ev]?.max ?? 1) : (rowBnds[ri]?.max ?? 1);
        if (heatmapType === "col" && idx >= 0) return colBnds[idx]?.max ?? 1;
        return useGlobal ? gOverall.max : overall.max;
      });
    });
    return { blockEvMin, blockEvMax };
  }, [blocks, allColIndices, heatmapType, globalHeatmap]);

  const blockEvents = blocks[0]?.events ?? [];

  // Custom drag order
  const [customOrder, setCustomOrder] = React.useState<string[]>([]);
  React.useEffect(() => { setCustomOrder(blocks[0]?.events ?? []); }, [blocks]);

  const [lexSortDir, setLexSortDir] = React.useState<"asc" | "desc" | null>(null);
  const handleLexSort = React.useCallback(() => {
    const nextDir = lexSortDir === "asc" ? "desc" : "asc";
    setLexSortDir(nextDir);
    setCustomOrder(prev => {
      const evs = prev.length > 0 ? [...prev] : [...(blocks[0]?.events ?? [])];
      return evs.sort((a, b) => nextDir === "asc" ? a.localeCompare(b) : b.localeCompare(a));
    });
  }, [lexSortDir, blocks]);
  const orderedEvents = customOrder.length > 0 ? customOrder.filter(e => blockEvents.includes(e)) : blockEvents;

  const visibleEvents = orderedEvents.filter(e => !hiddenEvents.has(e) && !(filteredOut?.has(e)));
  const hiddenSectionEvents = orderedEvents.filter(e => hiddenEvents.has(e) || (filteredOut?.has(e)));

  // Drag state
  const [draggedEv,  setDraggedEv]  = React.useState<string | null>(null);
  const [dragOverEv, setDragOverEv] = React.useState<string | null>(null);
  const [dragOverPos, setDragOverPos] = React.useState<"above" | "below">("below");

  const makeDragState = (ev: string): RowDragState => ({
    isDraggedRow: draggedEv === ev,
    isDragOver:   dragOverEv === ev,
    dragOverPos,
    onDragStart: (e) => {
      setDraggedEv(ev);
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData("text/plain", ev);
      // Minimal ghost so the browser default big screenshot doesn't appear
      const ghost = document.createElement("div");
      ghost.style.cssText = "position:fixed;top:-100px;left:-100px;background:#f3f4f6;padding:3px 8px;border-radius:4px;font-size:11px;white-space:nowrap;";
      ghost.textContent = ev;
      document.body.appendChild(ghost);
      e.dataTransfer.setDragImage(ghost, 0, 10);
      setTimeout(() => document.body.removeChild(ghost), 0);
    },
    onDragOver: (e) => {
      e.preventDefault();
      const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
      setDragOverPos(e.clientY < rect.top + rect.height / 2 ? "above" : "below");
      setDragOverEv(ev);
    },
    onDragLeave: (e) => {
      if (!(e.currentTarget as HTMLElement).contains(e.relatedTarget as Node))
        setDragOverEv(null);
    },
    onDrop: (e) => {
      e.preventDefault();
      if (!draggedEv || draggedEv === ev) { setDraggedEv(null); setDragOverEv(null); return; }
      setCustomOrder(prev => {
        const next = [...prev];
        const fi = next.indexOf(draggedEv);
        if (fi < 0) return prev;
        next.splice(fi, 1);
        const ti = next.indexOf(ev);
        if (ti < 0) return prev;
        next.splice(dragOverPos === "above" ? ti : ti + 1, 0, draggedEv);
        return next;
      });
      setDraggedEv(null); setDragOverEv(null);
    },
    onDragEnd: () => { setDraggedEv(null); setDragOverEv(null); },
  });

  const [collapseHidden, setCollapseHidden] = React.useState(true);

  const showLeftEdge  = !!pathPattern && !pathPattern.startsWith("path_start");
  const showRightEdge = !pathPattern || !pathPattern.includes("path_end");

  // ── Cell tooltip (diff mode) ──────────────────────────────────────────────
  const [cellTip, setCellTip] = React.useState<{
    x: number; y: number;
    ev: string; col: number; v: number; g1: number | null; g2: number | null;
  } | null>(null);

  return (
    <div style={{ position: "relative", flex: 1, overflow: "hidden" }}>
      <div ref={handleRef}
        style={{ position: "absolute", left: labelWidth - 1, top: 0, bottom: 0, width: 3, cursor: "col-resize", zIndex: 20, background: "transparent", transition: "background 0.12s" }}
        onMouseEnter={() => { if (handleRef.current) handleRef.current.style.background = "var(--retentioneering-yellow)"; }}
        onMouseLeave={() => { if (!resizing.current && handleRef.current) handleRef.current.style.background = "transparent"; }}
        onMouseDown={e => { e.preventDefault(); resizing.current = true; startX.current = e.clientX; startW.current = labelWidth; document.body.style.cursor = "col-resize"; document.body.style.userSelect = "none"; }}
      />
      <div ref={scrollRef} style={{ overflowX: "auto", overflowY: "auto", position: "absolute", inset: 0 }}>
        <table style={{ borderCollapse: "collapse", fontSize: 11, whiteSpace: "nowrap" }}>
          <thead>
            <tr>
              {/* Label column header with Go To */}
              <th style={{ position: "sticky", left: 0, top: 0, zIndex: 4, background: "#f9fafb", width: labelWidth, minWidth: labelWidth, height: HEADER_H, borderBottom: "2px solid #e5e7eb", borderRight: "2px solid #e5e7eb", textAlign: "left", padding: 0, verticalAlign: "bottom" }}>
                <div style={{ padding: "0 4px 4px 8px", display: "flex", alignItems: "flex-end", justifyContent: "space-between" }}>
                  <button onClick={openGoTo}
                    style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 10px", background: "#f3f4f6", border: "1px solid #e5e7eb", borderRadius: 6, cursor: "pointer", fontSize: 12, color: "#111827" }}>
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
                    <span>Search event</span>
                  </button>
                  <button onClick={handleLexSort} title={lexSortDir === "asc" ? "Sort Z→A" : "Sort A→Z"}
                    style={{ display: "flex", alignItems: "center", justifyContent: "center", width: 22, height: 22, border: "none", background: "transparent", cursor: "pointer", color: lexSortDir ? "#374151" : "#9ca3af", padding: 0, flexShrink: 0 }}
                    onMouseEnter={e => (e.currentTarget.style.color = "#374151")}
                    onMouseLeave={e => (e.currentTarget.style.color = lexSortDir ? "#374151" : "#9ca3af")}>
                    <LexSortIcon dir={lexSortDir} />
                  </button>
                  {goToOpen && goToRect && createPortal(
                    <div ref={dropdownRef} style={{ position: "fixed", top: goToRect.bottom, left: goToRect.left, width: Math.max(labelWidth, 200), background: "#fff", border: "1px solid #d1d5db", borderRadius: 7, boxShadow: "0 4px 16px rgba(0,0,0,0.12)", zIndex: 9999, overflow: "hidden" }}>
                      <div style={{ padding: "6px 8px", borderBottom: "1px solid #f3f4f6" }}>
                        <input ref={goToInputRef} value={goToQuery} onChange={e => setGoToQuery(e.target.value)} placeholder="Search events…"
                          style={{ width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 5, padding: "4px 8px", fontSize: 11, outline: "none" }} />
                      </div>
                      <div style={{ maxHeight: 240, overflowY: "auto" }}>
                        {goToFiltered.map(ev => (
                          <div key={ev} onMouseDown={() => { scrollToEvent(ev); setGoToOpen(false); setGoToQuery(""); }}
                            style={{ padding: "5px 10px", fontSize: 11, cursor: "pointer", color: "#111827", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}
                            onMouseEnter={e => (e.currentTarget.style.background = "#f3f4f6")}
                            onMouseLeave={e => (e.currentTarget.style.background = "")}>
                            {ev}
                          </div>
                        ))}
                        {goToFiltered.length === 0 && <div style={{ padding: "8px 10px", fontSize: 11, color: "#9ca3af" }}>No events found</div>}
                      </div>
                    </div>, document.body
                  )}
                </div>
              </th>
              {showLeftEdge && <EdgeTh height={HEADER_H} side="left" />}
              {/* Per-block column headers */}
              {blocks.map((block, bi) => (
                <React.Fragment key={bi}>
                  {bi > 0 && <SepTh height={HEADER_H} />}
                  {allColIndices[bi].map(ci => {
                    const col = block.columns[ci];
                    const isAnchor = col === 0 && blockHasBothSigns[bi];
                    const anchorBorder = "2px solid #e5e7eb";
                    const thinBorder   = "1px solid #f3f4f6";
                    const sortBtnSt: React.CSSProperties = { display: "flex", alignItems: "center", justifyContent: "center", width: SORT_BTN_W, flexShrink: 0, height: 20, border: "none", background: "transparent", cursor: "pointer", color: "#9ca3af", padding: 0 };
                    const hasSortBtns = col === 0 && (blockHasNeg[bi] || blockHasPos[bi]);
                    return (
                      <th key={`${bi}-${ci}`} style={{ position: "sticky", top: 0, zIndex: 2, background: "#f9fafb", width: CELL_W, minWidth: CELL_W, height: HEADER_H, borderBottom: "2px solid #e5e7eb", borderRight: isAnchor ? anchorBorder : thinBorder, borderLeft: isAnchor ? anchorBorder : undefined, verticalAlign: "middle", textAlign: "center", padding: hasSortBtns ? 0 : "0 2px" }}>
                        {hasSortBtns ? (
                          <div style={{ display: "flex", alignItems: "center", width: "100%", height: "100%" }}>
                            {blockHasNeg[bi]
                              ? <button title="Sort by preceding steps" onClick={() => handleSort(bi, "left")} style={sortBtnSt}
                                  onMouseEnter={e => (e.currentTarget.style.color = "#374151")}
                                  onMouseLeave={e => (e.currentTarget.style.color = "#9ca3af")}><SortIcon /></button>
                              : <span style={{ display: "inline-block", width: SORT_BTN_W, flexShrink: 0 }} />}
                            <span style={{ flex: 1, textAlign: "center", fontSize: 10, color: isAnchor ? "#111827" : "#6b7280", fontWeight: isAnchor ? 700 : 500 }}>{String(col)}</span>
                            {blockHasPos[bi]
                              ? <button title="Sort by following steps" onClick={() => handleSort(bi, "right")} style={sortBtnSt}
                                  onMouseEnter={e => (e.currentTarget.style.color = "#374151")}
                                  onMouseLeave={e => (e.currentTarget.style.color = "#9ca3af")}><SortIcon /></button>
                              : <span style={{ display: "inline-block", width: SORT_BTN_W, flexShrink: 0 }} />}
                          </div>
                        ) : (
                          <span style={{ fontSize: 10, color: isAnchor ? "#111827" : "#6b7280", fontWeight: isAnchor ? 700 : 500 }}>{String(col)}</span>
                        )}
                      </th>
                    );
                  })}
                </React.Fragment>
              ))}
              {showRightEdge && <EdgeTh height={HEADER_H} side="right" />}
            </tr>
          </thead>
          <tbody>
            {visibleEvents.map(ev => (
              <EventRow key={ev} ev={ev}
                blocks={blocks} allColIndices={allColIndices}
                blockEvMin={blockEvMin} blockEvMax={blockEvMax} blockHasBothSigns={blockHasBothSigns}
                isDiff={isDiff} labelWidth={labelWidth} heatmapType={heatmapType}
                showLeftEdge={showLeftEdge} showRightEdge={showRightEdge}

                isHidden={hiddenEvents.has(ev)} isPinned={pinnedEvents.has(ev)}
                onToggleHidden={onToggleHidden} onTogglePin={onTogglePin}
                eventCount={eventCounts?.[ev]}
                eventCountG1={eventCountsG1?.[ev]} eventCountG2={eventCountsG2?.[ev]}
                onCellHover={setCellTip}

                dragState={makeDragState(ev)} />
            ))}

            {/* Hidden events section */}
            {hiddenSectionEvents.length > 0 && (
              <>
                {/* Section header row */}
                <tr style={{ cursor: "pointer" }} onClick={() => setCollapseHidden(p => !p)}>
                  <td style={{ position: "sticky", left: 0, background: "#f3f4f6", zIndex: 1,
                    height: 22, borderTop: "2px solid #e5e7eb", borderBottom: collapseHidden ? "none" : "1px solid #e5e7eb",
                    borderRight: "2px solid #e5e7eb", padding: "0 8px", color: "#6b7280", fontSize: 11, fontWeight: 500,
                    userSelect: "none" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                      <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
                        style={{ flexShrink: 0, transform: collapseHidden ? "rotate(-90deg)" : "none", transition: "transform 0.15s" }}>
                        <path d="m6 9 6 6 6-6"/>
                      </svg>
                      <span>Hidden ({hiddenSectionEvents.length})</span>
                    </div>
                  </td>
                  <td colSpan={999} style={{ background: "#f3f4f6", borderTop: "2px solid #e5e7eb",
                    borderBottom: collapseHidden ? "none" : "1px solid #e5e7eb" }} />
                </tr>

                {/* Hidden event rows */}
                {!collapseHidden && hiddenSectionEvents.map(ev => (
                  <EventRow key={ev} ev={ev}
                    blocks={blocks} allColIndices={allColIndices}
                    blockEvMin={blockEvMin} blockEvMax={blockEvMax} blockHasBothSigns={blockHasBothSigns}
                    isDiff={isDiff} labelWidth={labelWidth} heatmapType={heatmapType}
                    showLeftEdge={showLeftEdge} showRightEdge={showRightEdge}

                    isHidden={true} isPinned={pinnedEvents.has(ev)}
                    onToggleHidden={onToggleHidden} onTogglePin={onTogglePin}
                    eventCount={eventCounts?.[ev]}
                    eventCountG1={eventCountsG1?.[ev]} eventCountG2={eventCountsG2?.[ev]}
                    onCellHover={setCellTip}

                    dimmed />
                ))}
              </>
            )}
          </tbody>
        </table>
      </div>

      {/* Cell tooltip portal */}
      {cellTip && Math.abs(cellTip.v) >= 0.005 && createPortal(
        <div style={{ position: "fixed", left: cellTip.x + 12, top: cellTip.y + 12, zIndex: 9999, pointerEvents: "none",
          minWidth: isDiff ? 240 : 180, background: "#fff", border: "1px solid #e5e7eb", borderRadius: 10,
          padding: "10px 14px", boxShadow: "0 8px 16px rgba(0,0,0,0.10)", fontSize: 13 }}>
          <div style={{ marginBottom: 2, fontWeight: 600, color: "#111827" }}>{cellTip.ev}</div>
          <div style={{ marginBottom: isDiff ? 8 : 0, fontSize: 11, color: "#6b7280" }}>step {cellTip.col}</div>
          {isDiff ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 16 }}>
                <span style={{ color: "#ef4444" }}>{diffSeg ?? "segment"}: {String(diffV2 ?? "group 2")}</span>
                <span style={{ fontFamily: "monospace", color: "#111827" }}>{cellTip.g2 !== null ? cellTip.g2.toFixed(4) : "—"}</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 16 }}>
                <span style={{ color: "#3b82f6" }}>{diffSeg ?? "segment"}: {String(diffV1 ?? "group 1")}</span>
                <span style={{ fontFamily: "monospace", color: "#111827" }}>{cellTip.g1 !== null ? cellTip.g1.toFixed(4) : "—"}</span>
              </div>
              <div style={{ borderTop: "1px solid #e5e7eb", margin: "2px 0" }} />
              <div style={{ display: "flex", justifyContent: "space-between", gap: 16 }}>
                <span style={{ fontWeight: 500, color: "#111827" }}>diff ({String(diffV2 ?? "g2")} − {String(diffV1 ?? "g1")})</span>
                <span style={{ fontFamily: "monospace", fontWeight: 600,
                  color: cellTip.v === 0 ? "#111827" : cellTip.v > 0 ? "#ef4444" : "#3b82f6" }}>
                  {cellTip.v >= 0 ? "+" : ""}{cellTip.v.toFixed(4)}
                </span>
              </div>
            </div>
          ) : (
            <span style={{ fontFamily: "monospace", fontWeight: 600, color: "#111827", fontSize: 14 }}>
              {cellTip.v.toFixed(6)}
            </span>
          )}
        </div>,
        document.body
      )}
    </div>
  );
}

// ── Sidebar helpers (matching SettingsSidebar style) ──────────────────────

const SC = {
  bg: "#ffffff", bgSection: "#f9fafb",
  border: "#e5e7eb", borderLight: "#d1d5db",
  text: "#111827", muted: "#6b7280",
  accent: "hsl(45, 93%, 58%)", accentFg: "hsl(0, 0%, 10%)",
} as const;

const sidebarSel: React.CSSProperties = {
  width: "100%", background: SC.bgSection, border: `1px solid ${SC.borderLight}`,
  borderRadius: 6, color: SC.text, fontSize: 13, padding: "6px 28px 6px 10px",
  cursor: "pointer", outline: "none", appearance: "none",
  backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%236b7280' stroke-width='2'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E")`,
  backgroundRepeat: "no-repeat", backgroundPosition: "right 8px center",
};

const SecHeader = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase" as const, color: SC.muted, marginBottom: 12 }}>{children}</div>
);

const FLabel = ({ children, tip }: { children: React.ReactNode; tip?: string }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 6 }}>
    <span style={{ fontSize: 13, fontWeight: 500, color: SC.text }}>{children}</span>
    {tip && <span title={tip} style={{ cursor: "help", color: SC.muted, fontSize: 12 }}>ⓘ</span>}
  </div>
);

function SingleSlider({ min, max, value, onChange, scale = "linear", integer = false }: {
  min: number; max: number; value: number; onChange: (v: number) => void;
  scale?: "linear" | "log"; integer?: boolean;
}) {
  const trackRef = React.useRef<HTMLDivElement>(null);

  const toPos = (v: number): number => {
    if (v <= min) return 0; if (v >= max) return 1;
    if (scale === "log" && min > 0 && max > 0)
      return (Math.log(v) - Math.log(min)) / (Math.log(max) - Math.log(min));
    return (v - min) / ((max - min) || 1);
  };
  const fromPos = (pos: number): number => {
    if (scale === "log" && min > 0 && max > 0)
      return Math.exp(Math.log(min) + pos * (Math.log(max) - Math.log(min)));
    return min + pos * (max - min);
  };

  const pct = toPos(value) * 100;

  const drag = (e: React.MouseEvent) => {
    e.preventDefault();
    const move = (ev: MouseEvent) => {
      const rect = trackRef.current?.getBoundingClientRect(); if (!rect) return;
      const pos = Math.max(0, Math.min(1, (ev.clientX - rect.left) / rect.width));
      const v = pos === 0 ? min : fromPos(pos);
      onChange(integer ? Math.round(v) : v);
    };
    const up = () => { document.removeEventListener("mousemove", move); document.removeEventListener("mouseup", up); };
    document.addEventListener("mousemove", move); document.addEventListener("mouseup", up);
    move(e.nativeEvent as MouseEvent);
  };

  return (
    <div ref={trackRef} style={{ position: "relative", height: 20, display: "flex", alignItems: "center", userSelect: "none", cursor: "ew-resize" }}
      onMouseDown={drag}>
      <div style={{ width: "100%", height: 4, background: "#e2e8f0", borderRadius: 9999, position: "relative" }}>
        <div style={{ position: "absolute", left: 0, width: `${pct}%`, height: "100%", background: "#94a3b8", borderRadius: 9999 }} />
      </div>
      <div style={{ ...THUMB, left: `${pct}%` }} />
    </div>
  );
}

// ── App ────────────────────────────────────────────────────────────────────

export function render({ model, el, isStatic = false }: RenderContext) {
  function App() {
    const [result,       setResult]       = React.useState<MatrixResult>(() => parseJson(model.get("result"), { matrices: [], event_counts: {} }));
    const [isLoading,    setIsLoading]    = React.useState<boolean>(() => (model.get("is_loading") as boolean) ?? false);
    const [height,       setHeight]       = React.useState<number>(() => (model.get("height") as number) ?? 600);
    const [sidebarOpen,  setSidebarOpen]  = React.useState<boolean>(() => (model.get("sidebar_open") as boolean) ?? true);
    const [pathIdCol,    setPathIdCol]    = React.useState<string>(() => (model.get("path_col") as string) || "");
    const [pathPattern,  setPathPattern]  = React.useState<string>(() => (model.get("path_pattern") as string) || "");
    const [appliedPathPattern, setAppliedPathPattern] = React.useState<string>(() => (model.get("path_pattern") as string) || "");

    // ── display state ──────────────────────────────────────────────────────
    const [stepWindow,   setStepWindow]   = React.useState(3);
    const [hiddenEvents, setHiddenEvents] = React.useState<Set<string>>(new Set());
    const [pinnedEvents,  setPinnedEvents]  = React.useState<Set<string>>(new Set());
    const [labelWidth,   setLabelWidth]   = React.useState(200);
    const [heatmapType,    setHeatmapType]    = React.useState<"overall"|"row"|"col">("overall");
    const [globalHeatmap,  setGlobalHeatmap]  = React.useState(true);
    const [popRange,     setPopRange]     = React.useState<[number, number]>([0, Infinity]);
    const [valueThreshold, setValueThreshold] = React.useState(0.01);

    // ── diff state ─────────────────────────────────────────────────────────
    const initDiff = parseJson<string[]>(model.get("diff") || "[]", []);
    const [diffSeg, setDiffSeg] = React.useState<string | null>(initDiff[0] ?? null);
    const [diffV1,  setDiffV1]  = React.useState<string | null>(initDiff[1] ?? null);
    const [diffV2,  setDiffV2]  = React.useState<string | null>(initDiff[2] ?? null);
    const [localDiffSeg, setLocalDiffSeg] = React.useState(diffSeg ?? "");
    const [localDiffV1,  setLocalDiffV1]  = React.useState(diffV1  ?? "");
    const [localDiffV2,  setLocalDiffV2]  = React.useState(diffV2  ?? "");
    // Sync local diff state when server diff changes
    React.useEffect(() => {
      setLocalDiffSeg(diffSeg ?? "");
      setLocalDiffV1(diffV1 ?? "");
      setLocalDiffV2(diffV2 ?? "");
    }, [diffSeg, diffV1, diffV2]);

    const pathCols  = parseJson<string[]>(model.get("path_cols"), []);
    const segLevels = parseJson<Record<string, string[]>>(model.get("segment_levels"), {});
    const segCols = Object.keys(segLevels);

    // ── subscriptions ──────────────────────────────────────────────────────
    React.useEffect(() => {
      const subs: Array<[string, () => void]> = [
        ["result",       () => { setResult(parseJson(model.get("result"), { matrices: [], event_counts: {} })); }],
        ["is_loading",   () => setIsLoading((model.get("is_loading") as boolean) ?? false)],
        ["height",       () => setHeight((model.get("height") as number) ?? 600)],
        ["sidebar_open", () => setSidebarOpen((model.get("sidebar_open") as boolean) ?? true)],
        ["path_col",  () => setPathIdCol((model.get("path_col") as string) || "")],
        ["path_pattern", () => { const v = (model.get("path_pattern") as string) || ""; setPathPattern(v); setAppliedPathPattern(v); }],
        ["diff",         () => { const d = parseJson<string[]>(model.get("diff") || "[]", []); setDiffSeg(d[0]??null); setDiffV1(d[1]??null); setDiffV2(d[2]??null); }],
      ];
      subs.forEach(([k, cb]) => model.on(`change:${k}`, cb));
      return () => subs.forEach(([k, cb]) => model.off(`change:${k}`, cb));
    }, []);

    const setParam = (key: string, val: unknown) => { model.set(key, val); model.save_changes(); };

    // Expose external navigation API for static HTML report links
    React.useEffect(() => {
      (el as any).__matrixApi = {
        scrollToCell: (eventName: string, step: number) => {
          // Expand step_window if needed so the target column is visible
          setStepWindow(prev => Math.max(prev, Math.abs(step)));
          setTimeout(() => {
            const rows = el.querySelectorAll("tr[data-event]");
            for (let i = 0; i < rows.length; i++) {
              const row = rows[i] as HTMLElement;
              if (row.dataset.event === eventName) {
                const cell = row.querySelector(`td[data-step="${step}"]`) as HTMLElement | null;
                if (cell) {
                  cell.scrollIntoView({ block: "nearest", inline: "center", behavior: "smooth" });
                  const prev = cell.style.background;
                  cell.style.background = "#fef3c7";
                  setTimeout(() => { cell.style.background = prev; }, 900);
                } else {
                  row.scrollIntoView({ block: "nearest", behavior: "smooth" });
                }
                break;
              }
            }
          }, 150);
        },
      };
    }, [setStepWindow]);

    const applyDiff = () => {
      if (localDiffSeg && localDiffV1 && localDiffV2 && localDiffV1 !== localDiffV2) {
        setDiffSeg(localDiffSeg); setDiffV1(localDiffV1); setDiffV2(localDiffV2);
        setParam("diff", JSON.stringify([localDiffSeg, localDiffV1, localDiffV2]));
      } else {
        setDiffSeg(null); setDiffV1(null); setDiffV2(null);
        setParam("diff", "");
      }
    };
    const resetDiff = () => { setLocalDiffSeg(""); setLocalDiffV1(""); setLocalDiffV2(""); setDiffSeg(null); setDiffV1(null); setDiffV2(null); setParam("diff", ""); };

    const toggleHidden = (ev: string) => setHiddenEvents(prev => { const s = new Set(prev); if (s.has(ev)) s.delete(ev); else s.add(ev); return s; });
    const togglePin = (ev: string) => setPinnedEvents(prev => { const s = new Set(prev); if (s.has(ev)) s.delete(ev); else s.add(ev); return s; });

    const matrices = result.matrices ?? [];
    const eventCounts = result.event_counts ?? {};
    const isDiff = !!diffSeg && matrices.length > 0;

    const popMin = React.useMemo(() => {
      const vals = Object.values(eventCounts).filter(v => v > 0);
      return vals.length > 0 ? Math.min(...vals) : 1;
    }, [eventCounts]);
    const popMax = React.useMemo(() => {
      const vals = Object.values(eventCounts);
      return vals.length > 0 ? Math.max(...vals) : 1;
    }, [eventCounts]);

    const filteredOut = React.useMemo(() => {
      const s = new Set<string>();
      if (!matrices[0]) return s;
      matrices[0].events.forEach(ev => {
        if (ev === "path_start" || ev === "path_end") return;
        if (pinnedEvents.has(ev)) return; // pinned events are immune to threshold filters
        // Event count threshold (OR logic with value threshold)
        const cnt = eventCounts[ev] ?? 0;
        if (cnt < popRange[0] || cnt > popRange[1]) { s.add(ev); return; }
        // Matrix value threshold: hide row if max |value| across all blocks < threshold
        if (valueThreshold > 0) {
          const ri = matrices[0].events.indexOf(ev);
          let maxAbs = 0;
          matrices.forEach(block => {
            block.values[ri]?.forEach(v => { maxAbs = Math.max(maxAbs, Math.abs(v)); });
          });
          if (maxAbs < valueThreshold) s.add(ev);
        }
      });
      return s;
    }, [popRange, valueThreshold, matrices, eventCounts, pinnedEvents]);

    const diffDirty = localDiffSeg !== (diffSeg ?? "") || localDiffV1 !== (diffV1 ?? "") || localDiffV2 !== (diffV2 ?? "");
    const localLevels = localDiffSeg ? (segLevels[localDiffSeg] ?? []) : [];
    const canApplyDiff = localDiffSeg && localDiffV1 && localDiffV2 && localDiffV1 !== localDiffV2;

    return (
      <div style={{ display: "flex", flexDirection: "row", height, background: "#fff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif", position: "relative" }}>

        {/* Main content */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden", minWidth: 0, position: "relative" }}>

          {/* Sidebar toggle — top-right of canvas */}
          <button onClick={() => { const n = !sidebarOpen; setSidebarOpen(n); setParam("sidebar_open", n); }} title="Toggle settings"
            style={{ position: "absolute", top: 8, right: 8, zIndex: 25, display: "flex", alignItems: "center", justifyContent: "center", width: 28, height: 28, borderRadius: 6, cursor: "pointer", background: "#f3f4f6", border: "1px solid #d1d5db", color: "#6b7280" }}>
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><rect x="1" y="1" width="14" height="14" rx="2" stroke="currentColor" strokeWidth="1.5"/><line x1="10" y1="1" x2="10" y2="15" stroke="currentColor" strokeWidth="1.5"/></svg>
          </button>

          {/* Loading */}
          {isLoading && <ComputingSpinner />}

          {/* Table */}
          {matrices.length === 0 ? (
            <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: "#9ca3af", fontSize: 13 }}>No data</div>
          ) : (
            <div style={{ flex: 1, overflow: "hidden", display: "flex" }}>
              <MatrixView blocks={matrices} stepWindow={stepWindow} isDiff={isDiff}
                labelWidth={labelWidth} onLabelResize={setLabelWidth}
                hiddenEvents={hiddenEvents} filteredOut={filteredOut} pinnedEvents={pinnedEvents}
                onToggleHidden={toggleHidden} onTogglePin={togglePin}

                heatmapType={heatmapType} globalHeatmap={globalHeatmap}
                eventCounts={eventCounts}
                eventCountsG1={result.event_counts_g1}
                eventCountsG2={result.event_counts_g2}
                pathPattern={pathPattern}
                diffSeg={diffSeg} diffV1={diffV1} diffV2={diffV2} />
            </div>
          )}
        </div>

        {/* Sidebar */}
        {sidebarOpen && (
          <div style={{ width: 280, minWidth: 280, height: "100%", background: SC.bg, borderLeft: `1px solid ${SC.border}`, display: "flex", flexDirection: "column", overflow: "hidden" }}>
            {/* Header */}
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 16px", borderBottom: `1px solid ${SC.border}` }}>
              <span style={{ fontSize: 13, fontWeight: 600, color: SC.text }}>Settings</span>
            </div>

            {/* Scrollable content */}
            <div style={{ flex: 1, overflowY: "auto", padding: 16 }}>

              <SecHeader>Values Settings</SecHeader>

              {pathCols.length > 1 && (
                <div style={{ marginBottom: 20 }}>
                  <FLabel tip="Column used as the path identifier.">Path Column</FLabel>
                  <select value={pathIdCol} onChange={e => { setPathIdCol(e.target.value); setParam("path_col", e.target.value); }} style={sidebarSel} disabled={isLoading || isStatic}>
                    {pathCols.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
              )}

              <div style={{ marginBottom: 20 }}>
                <FLabel tip="Filter to a specific sequence of events, e.g. path_start->.*->purchase">Path Pattern</FLabel>
                <input
                  value={pathPattern}
                  onChange={e => !isStatic && setPathPattern(e.target.value)}
                  onKeyDown={e => {
                    if (isStatic) return;
                    if (e.key === "Enter") { setAppliedPathPattern(pathPattern); setParam("path_pattern", pathPattern); }
                    if (e.key === "Escape") { setPathPattern(appliedPathPattern); }
                  }}
                  readOnly={isStatic}
                  placeholder="e.g. path_start->.*->purchase"
                  style={{ width: "100%", boxSizing: "border-box", background: SC.bgSection, border: `1px solid ${SC.borderLight}`, borderRadius: 6, color: isStatic ? SC.muted : SC.text, fontSize: 13, padding: "6px 10px", outline: "none", fontFamily: "monospace", cursor: isStatic ? "default" : undefined }}
                />
                {!isStatic && pathPattern !== appliedPathPattern && (
                  <button onClick={() => { setAppliedPathPattern(pathPattern); setParam("path_pattern", pathPattern); }}
                    style={{ marginTop: 6, width: "100%", padding: "6px 0", background: SC.accent, border: `1px solid ${SC.accent}`, borderRadius: 6, cursor: "pointer", color: SC.accentFg, fontSize: 12, fontWeight: 600 }}>
                    Apply
                  </button>
                )}
              </div>

              <div style={{ marginBottom: 24 }}>
                <FLabel tip="Compare two groups. Red = more in group 2, blue = more in group 1.">Diff by Segment</FLabel>
                <select value={localDiffSeg} onChange={e => {
                  const col = e.target.value;
                  setLocalDiffSeg(col);
                  if (!col) { resetDiff(); return; }
                  const lvls = segLevels[col] ?? [];
                  setLocalDiffV1(lvls[0] != null ? String(lvls[0]) : "");
                  setLocalDiffV2(lvls[1] != null ? String(lvls[1]) : "");
                }} style={{ ...sidebarSel, marginBottom: localDiffSeg ? 8 : 0 }} disabled={isLoading || isStatic}>
                  <option value="">— None</option>
                  {segCols.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
                {localDiffSeg && (segLevels[localDiffSeg] ?? []).length >= 2 && (
                  <>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 5, flex: 1, minWidth: 0 }}>
                        <span style={{ color: "rgb(59,130,246)", fontSize: 13, flexShrink: 0 }}>●</span>
                        <select value={localDiffV1} onChange={e => setLocalDiffV1(e.target.value)} style={{ ...sidebarSel, flex: 1, minWidth: 0, width: "auto" }} disabled={isLoading || isStatic}>
                          {(segLevels[localDiffSeg] ?? []).map(v => <option key={String(v)} value={String(v)} disabled={String(v) === localDiffV2}>{String(v)}</option>)}
                        </select>
                      </div>
                      <span style={{ color: SC.muted, fontSize: 11, flexShrink: 0 }}>vs</span>
                      <div style={{ display: "flex", alignItems: "center", gap: 5, flex: 1, minWidth: 0 }}>
                        <span style={{ color: "rgb(239,68,68)", fontSize: 13, flexShrink: 0 }}>●</span>
                        <select value={localDiffV2} onChange={e => setLocalDiffV2(e.target.value)} style={{ ...sidebarSel, flex: 1, minWidth: 0, width: "auto" }} disabled={isLoading || isStatic}>
                          {(segLevels[localDiffSeg] ?? []).map(v => <option key={String(v)} value={String(v)} disabled={String(v) === localDiffV1}>{String(v)}</option>)}
                        </select>
                      </div>
                    </div>
                    {diffDirty && !isStatic && (
                      <button onClick={applyDiff} disabled={!canApplyDiff || isLoading}
                        style={{ width: "100%", padding: "6px 0", background: canApplyDiff ? SC.accent : SC.bgSection, border: `1px solid ${canApplyDiff ? SC.accent : SC.borderLight}`, borderRadius: 6, cursor: canApplyDiff ? "pointer" : "default", color: canApplyDiff ? SC.accentFg : SC.muted, fontSize: 12, fontWeight: 600 }}>
                        Apply
                      </button>
                    )}
                  </>
                )}
              </div>

              <div style={{ height: 1, background: SC.border, margin: "0 0 20px" }} />

              <SecHeader>Visibility Settings</SecHeader>

              <div style={{ marginBottom: 20 }}>
                <FLabel tip="Steps visible on each side of the anchor column.">Step window</FLabel>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <div style={{ flex: 1 }}><SingleSlider min={1} max={20} value={stepWindow} onChange={setStepWindow} integer /></div>
                  <input type="number" min={1} max={20} value={stepWindow}
                    onChange={e => setStepWindow(Math.max(1, Math.min(20, parseInt(e.target.value) || 1)))}
                    style={{ width: 52, border: `1px solid ${SC.borderLight}`, borderRadius: 6, padding: "5px 8px", fontSize: 13, textAlign: "center", outline: "none", background: SC.bg, color: SC.text, boxShadow: "inset 0 1px 2px rgba(0,0,0,0.04)" }} />
                </div>
              </div>

              <div style={{ marginBottom: 20 }}>
                <FLabel tip="Hides events whose frequency in the entire eventstream falls outside the selected range. Pinned events and path_start / path_end are immune.">Event Count</FLabel>
                <RangeSlider
                  min={popMin} max={popMax}
                  value={[Math.max(popMin, popRange[0] <= 0 ? popMin : popRange[0]), Math.min(popMax, !isFinite(popRange[1]) ? popMax : popRange[1])]}
                  onChange={setPopRange}
                  scale="log"
                  formatValue={v => fmtCount(Math.round(v))} />
              </div>

              <div style={{ marginBottom: 20 }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 6 }}>
                  <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
                    <span style={{ fontSize: 13, fontWeight: 500, color: SC.text }}>Matrix Value</span>
                    <span title="Hides rows where the maximum absolute value across the entire row (all steps, all blocks) is less than the threshold. Pinned events are immune." style={{ cursor: "help", color: SC.muted, fontSize: 12 }}>ⓘ</span>
                  </span>
                  <span style={{ fontSize: 12, color: SC.muted }}>≥ {valueThreshold > 0 ? valueThreshold.toFixed(3) : "—"}</span>
                </div>
                <SingleSlider min={0.001} max={1} value={valueThreshold > 0 ? valueThreshold : 0.001}
                  onChange={v => setValueThreshold(v <= 0.001 ? 0 : parseFloat(v.toPrecision(3)))} scale="log" />
              </div>

              <div style={{ height: 1, background: SC.border, margin: "0 0 20px" }} />

              <div style={{ marginBottom: 0 }}>
                <FLabel>Heatmap</FLabel>
                <select value={heatmapType} onChange={e => setHeatmapType(e.target.value as "overall"|"row"|"col")} style={sidebarSel} disabled={isLoading}>
                  <option value="overall">Overall</option>
                  <option value="row">By row</option>
                  <option value="col">By column</option>
                </select>
                {(heatmapType === "overall" || heatmapType === "row") && !!appliedPathPattern && (
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: 8 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                      <span style={{ fontSize: 13, color: SC.text }}>Across all blocks</span>
                      <span title="When enabled, the heatmap scale is computed across all blocks together, so intensities are comparable between blocks. Applies to Overall and By row modes."
                        style={{ cursor: "help", display: "inline-flex", alignItems: "center" }}>
                        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#9ca3af" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
                      </span>
                    </div>
                    <div onClick={() => setGlobalHeatmap(v => !v)}
                      style={{ width: 32, height: 18, borderRadius: 9, background: globalHeatmap ? SC.accent : "#d1d5db", position: "relative", cursor: "pointer", transition: "background 0.2s", flexShrink: 0 }}>
                      <div style={{ position: "absolute", top: 2, left: globalHeatmap ? 16 : 2, width: 14, height: 14, borderRadius: "50%", background: "#fff", transition: "left 0.2s" }} />
                    </div>
                  </div>
                )}
              </div>

            </div>
          </div>
        )}

        <RetentioneeringSpinKeyframes />
      </div>
    );
  }

  const root = createRoot(el);
  root.render(<App />);
  return () => root.unmount();
}
