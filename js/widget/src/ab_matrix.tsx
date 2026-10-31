/**
 * A/B difference matrices: `ab_transitions` (source x target) and `ab_steps`
 * (event x step). Each cell is one row of the `get_ab_transitions` /
 * `get_ab_steps` frame (through the twins `ab_transitions_data` /
 * `ab_steps_data`); the encoding rules are in `ab_matrix_encoding.ts`.
 *
 * Data parameters edited in the sidebar set one traitlet each and Python
 * recomputes; view parameters (intervals, significance filter, colour clip,
 * sorting, hidden / pinned events, cell size, reading hint) live here and are
 * written back so `state_file` keeps them. A static export keeps the view
 * controls and locks the data ones.
 */
import * as React from "react";
import { createRoot } from "react-dom/client";
import {
  parseJson, ComputingSpinner, RetentioneeringSpinKeyframes, useHostSubscriptions, focusCells,
  type RenderContext,
} from "./widget-utils";
import {
  type CellStatus, type IntervalKind, type MatrixPayload, type MatrixRow, type SignificanceFilter, type SortKey,
  type ViewMode, type PowerInfo,
  axisStats, cellBackground, cellId, cellLook, cellStatus, cellText, crossImage, curveGeometry, deltaSign, familyText,
  fmtArmValue, fmtP, fmtSigned, fmtTimes, decimalsFor, hatchImage, indexRows, intervalNote, intervalOf, keepsSignificance,
  mdeScaleMax, needBackground, needOf, needText, orderKeys, parsePayload, populationText, readingHint, scaleMax, unitInfo,
  HATCH_COLOR, MATRIX_BLUE, MATRIX_RED, MAX_ALPHA, NEED_HUE, NEED_MAX_TIMES, OUTLINE_BLACK, OUTLINE_GREY, OUTLINE_WIDTH,
  TEXT_MUTED,
} from "./ab_matrix_encoding";

export type Kind = "transitions" | "steps";

// ── options ────────────────────────────────────────────────────────────────

const WEIGHTS: [string, string][] = [
  ["unique_paths", "unique_paths: share of paths"],
  ["avg_per_path", "avg_per_path: per path"],
  ["proba_out", "proba_out (conditional)"],
  ["proba_in", "proba_in (conditional)"],
  ["share_of_total", "share_of_total (conditional)"],
];
const CORRECTIONS: [string, string][] = [
  ["bh", "Benjamini–Hochberg"], ["holm", "Holm"], ["bonferroni", "Bonferroni"], ["none", "None"],
];
const CI_LEVELS = [0.9, 0.95, 0.99];
const FILTERS: [SignificanceFilter, string][] = [
  ["all", "Show every row and column"], ["dim", "Dim those without a significant cell"],
  ["hide", "Hide those without a significant cell"],
];
const SORTS: [SortKey, string][] = [
  ["name", "Name"], ["max_abs_delta", "Largest |delta|"], ["n_significant", "Significant cells"],
];
const CELL_SIZES: [string, string][] = [
  ["auto", "Auto"], ["normal", "Normal (delta and interval)"], ["compact", "Compact (interval in the tooltip)"],
];

const DATA_KEYS: Record<Kind, string[]> = {
  transitions: [
    "edge_weight", "horizon", "path_col", "edges", "test", "correction", "ci_level", "power",
    "n_bootstrap", "bootstrap_weights", "min_support", "random_state",
  ],
  steps: ["max_steps", "horizon", "path_col", "correction", "ci_level", "power", "min_support"],
};

export interface View {
  show_ci: boolean;
  interval_kind: IntervalKind;
  significance_filter: SignificanceFilter;
  color_clip: number;
  row_sort: SortKey;
  col_sort: SortKey;
  cell_size: string;
  visibility: Record<string, { isHidden: boolean; isPinned: boolean }>;
  reading_hint: boolean;
  view_mode: ViewMode;
  /** The "data needed" view's goal in the frame's units; 0 = none. */
  target_delta: number;
}

function readView(host: RenderContext["host"]): View {
  const get = (k: string) => host.get(k);
  const bool = (v: unknown, d: boolean) => (typeof v === "boolean" ? v : d);
  return {
    show_ci: bool(get("show_ci"), true),
    interval_kind: (get("interval_kind") as IntervalKind) || "per_cell",
    significance_filter: (get("significance_filter") as SignificanceFilter) || "all",
    color_clip: Number(get("color_clip")) || 0,
    row_sort: (get("row_sort") as SortKey) || "name",
    col_sort: (get("col_sort") as SortKey) || "name",
    cell_size: (get("cell_size") as string) || "auto",
    visibility: parseJson(get("event_visibility"), {}),
    reading_hint: bool(get("reading_hint"), true),
    view_mode: get("view_mode") === "data_needed" ? "data_needed" : "delta",
    target_delta: Math.max(0, Number(get("target_delta")) || 0),
  };
}

function readParams(host: RenderContext["host"], kind: Kind): Record<string, any> {
  const out: Record<string, any> = {};
  for (const key of DATA_KEYS[kind]) out[key] = host.get(key);
  return out;
}

// ── small pieces ───────────────────────────────────────────────────────────

const SIDEBAR_W = 264;
const LABEL_W = 140;
const sel: React.CSSProperties = {
  width: "100%", boxSizing: "border-box", border: "1px solid #d1d5db", borderRadius: 6, color: "#111827",
  fontSize: 12, padding: "5px 8px", cursor: "pointer", outline: "none", background: "#f9fafb",
};
const SH = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: "#6b7280", margin: "0 0 6px" }}>{children}</div>
);
const Rule = () => <div style={{ height: 1, background: "#e5e7eb", margin: "14px 0" }} />;
const Hint = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 10, color: "#6b7280", marginTop: 4, lineHeight: 1.4 }}>{children}</div>
);
const Label = ({ children }: { children: React.ReactNode }) => (
  <div style={{ fontSize: 11, color: "#374151", margin: "8px 0 4px" }}>{children}</div>
);

function SidebarToggle({ onClick }: { onClick: () => void }) {
  return (
    <button onClick={onClick} title="Toggle settings" aria-label="Toggle settings sidebar" style={{ position: "absolute", top: 8, right: 12, zIndex: 25, display: "flex", alignItems: "center", justifyContent: "center", width: 30, height: 30, borderRadius: 6, cursor: "pointer", background: "#f3f4f6", border: "1px solid #d1d5db", color: "#6b7280" }}>
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <rect x="1" y="1" width="14" height="14" rx="2" stroke="currentColor" strokeWidth="1.5" />
        <line x1="10" y1="1" x2="10" y2="15" stroke="currentColor" strokeWidth="1.5" />
      </svg>
    </button>
  );
}

/** The hide button's eye (a hidden event leaves the matrix; the sidebar lists it). */
function EyeIcon() {
  return (
    <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7Z" /><circle cx="12" cy="12" r="3" />
    </svg>
  );
}

function PinIcon({ pinned }: { pinned: boolean }) {
  return (
    <svg width="11" height="11" viewBox="0 0 24 24" fill={pinned ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="17" x2="12" y2="22" />
      <path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6h1a2 2 0 0 0 0-4H8a2 2 0 0 0 0 4h1v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z" />
    </svg>
  );
}

const iconBtn = (active: boolean, color: string): React.CSSProperties => ({
  display: "flex", alignItems: "center", justifyContent: "center", width: 16, height: 16, padding: 0,
  border: "none", background: "transparent", cursor: "pointer", color: active ? color : "#c4c9d1", flexShrink: 0,
});

/** A legend swatch drawn with the cells' own rules. */
function Swatch({ status, bg }: { status: CellStatus; bg?: string }) {
  const look = cellLook(status);
  return (
    <span data-testid={`ab-legend-${status}`} style={{
      display: "inline-block", width: 26, height: 16, flexShrink: 0, backgroundColor: bg ?? "#fff",
      backgroundImage: look.cross ? crossImage() : undefined,
      outline: look.outlineColor ? `${OUTLINE_WIDTH}px ${look.outlineStyle} ${look.outlineColor}` : "1px solid #f3f4f6",
      outlineOffset: look.outlineColor ? `-${OUTLINE_WIDTH}px` : "-1px",
    }} />
  );
}

// ── tooltip ────────────────────────────────────────────────────────────────

interface TipTarget { r: string; c: string; idx: number | null; x: number; y: number }

export interface TipApi { show: (t: TipTarget) => void; hide: () => void }

const TipLine = ({ label, value, color }: { label: string; value: React.ReactNode; color?: string }) => (
  <div style={{ display: "flex", justifyContent: "space-between", gap: 14 }}>
    <span style={{ color: color ?? "#6b7280" }}>{label}</span>
    <span style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace", color: "#111827", textAlign: "right" }}>{value}</span>
  </div>
);

function Tooltip({ apiRef, payload, kind, mode, target }: {
  apiRef: React.MutableRefObject<TipApi | null>; payload: MatrixPayload; kind: Kind; mode: ViewMode; target: number;
}) {
  const [tip, setTip] = React.useState<TipTarget | null>(null);
  React.useEffect(() => {
    apiRef.current = { show: (t) => setTip(t), hide: () => setTip(null) };
    return () => { apiRef.current = null; };
  }, []);
  if (!tip) return null;
  const unit = unitInfo(payload);
  const row: MatrixRow | undefined = tip.idx != null ? payload.rows[tip.idx] : undefined;
  const status = cellStatus(row, payload.alpha);
  const title = kind === "transitions" ? `${tip.r} → ${tip.c}` : `${tip.r} @ step ${tip.c}`;
  const W = 420;
  const left = Math.max(8, Math.min(tip.x + 14, window.innerWidth - W - 8));
  const top = Math.max(8, Math.min(tip.y + 14, window.innerHeight - 40 - (row ? 360 : 40)));
  const statusText: Record<CellStatus, string> = {
    significant: `significant (adjusted p < ${payload.alpha})`,
    not_significant: "tested, not significant: cannot say the difference is non-zero",
    untested: "not tested",
    empty: "never occurs in either arm (no row in the frame)",
  };
  const g1 = payload.group1_label, g2 = payload.group2_label;
  const need = row && tip.idx != null
    ? needOf(row, payload.power?.scale_to_test?.[tip.idx], payload.alpha, target > 0 ? target : null)
    : null;
  let body: React.ReactNode = null;
  if (row) {
    const [lo, hi] = intervalOf(row, "per_cell");
    const d = decimalsFor(unit, row.delta, lo, hi);
    const suffix = unit.suffix ? ` ${unit.suffix}` : "";
    const level = `${+(payload.ci_level * 100).toFixed(1)}%`;
    const reason = tip.idx != null ? payload.untested_reason?.[tip.idx] : null;
    const hasAdj = row.delta_ci_low_adj != null || row.delta_ci_high_adj != null;
    body = (
      <>
        <div style={{ display: "flex", flexDirection: "column", gap: 2, marginTop: 6 }}>
          <TipLine label={g1} color={`rgb(${MATRIX_RED})`} value={fmtArmValue(row.group1_value, unit)} />
          <TipLine label={g2} color={`rgb(${MATRIX_BLUE})`} value={fmtArmValue(row.group2_value, unit)} />
          <TipLine label={`delta (${g1} − ${g2})`} value={`${fmtSigned(row.delta, d, unit.scale)}${suffix}`} />
          <TipLine label={`${level} interval (per cell)`} value={`[${fmtSigned(lo, d, unit.scale)}, ${fmtSigned(hi, d, unit.scale)}]${suffix}`} />
          {hasAdj && (
            <TipLine label="simultaneous (Bonferroni)" value={`[${fmtSigned(row.delta_ci_low_adj, d, unit.scale)}, ${fmtSigned(row.delta_ci_high_adj, d, unit.scale)}]${suffix}`} />
          )}
          <TipLine label="relative delta" value={row.rel_delta != null ? `${fmtSigned(row.rel_delta, 1, 100)} %` : "n/a"} />
          <TipLine label="p-value" value={fmtP(row.p_value)} />
          <TipLine label={`adjusted p (${familyText(payload)})`} value={fmtP(row.p_value_adjusted)} />
          <TipLine label={`MDE (at alpha ${row.mde_alpha != null ? +Number(row.mde_alpha).toPrecision(3) : "n/a"})`}
            value={row.mde != null ? `${fmtSigned(row.mde, decimalsFor(unit, row.mde), unit.scale).replace(/^\+/, "")}${suffix}` : "n/a"} />
          {kind === "transitions" && row.group1_edge_paths != null && (
            <TipLine label="paths with the transition" value={`${row.group1_edge_paths} of ${row.group1_paths} / ${row.group2_edge_paths} of ${row.group2_paths}`} />
          )}
          <TipLine label="support (paths with a value)" value={`${row.group1_support ?? "n/a"} / ${row.group2_support ?? "n/a"}`} />
          {!!row.support_warning && <TipLine label="support warning" value={String(row.support_warning)} />}
          <TipLine label="conditional" value={row.conditional ? "yes: post-treatment denominator" : "no"} />
          <TipLine label="verdict" value={String(row.verdict ?? "n/a")} />
          <TipLine label="test" value={String(row.test ?? "n/a")} />
          {status === "untested" && reason && <TipLine label="why not tested" value={reason} />}
          {mode === "data_needed" && need && need.timesFor === "test" && (
            <TipLine label="users to become testable" value={`${fmtTimes(need.times)} (${Math.ceil((row.group1_paths ?? 0) * (need.times ?? 0))} / ${Math.ceil((row.group2_paths ?? 0) * (need.times ?? 0))} paths)`} />
          )}
          {mode === "data_needed" && need && need.timesFor === "target" && (
            <TipLine label={`users for an MDE of ${fmtSigned(target, decimalsFor(unit, target), unit.scale).replace(/^\+/, "")}${suffix}`}
              value={need.times != null && need.times <= 1 ? "enough now" : `${fmtTimes(need.times)} (${Math.ceil((row.group1_paths ?? 0) * (need.times ?? 0))} / ${Math.ceil((row.group2_paths ?? 0) * (need.times ?? 0))} paths)`} />
          )}
          {mode === "data_needed" && status === "untested" && need && need.timesFor === null && (
            <TipLine label="users to become testable" value="more users of the same mix do not make it testable" />
          )}
        </div>
        {row.reading && (
          <div data-testid="ab-tip-reading" style={{ marginTop: 8, paddingTop: 6, borderTop: "1px solid #e5e7eb", color: "#374151", lineHeight: 1.45 }}>
            {row.reading}
          </div>
        )}
      </>
    );
  }
  return (
    <div data-testid="ab-tooltip" style={{
      position: "fixed", left, top, zIndex: 9999, width: W, maxWidth: "calc(100vw - 16px)", pointerEvents: "none",
      background: "#fff", border: "1px solid #e5e7eb", borderRadius: 10, padding: "10px 12px",
      boxShadow: "0 8px 20px rgba(0,0,0,0.12)", fontSize: 11, color: "#111827",
    }}>
      <div style={{ fontWeight: 600, fontSize: 12 }}>{title}</div>
      <div data-testid="ab-tip-status" style={{ fontSize: 11, color: status === "significant" ? OUTLINE_BLACK : "#6b7280", marginTop: 2 }}>
        {statusText[status]}
      </div>
      {body}
    </div>
  );
}

// ── matrix ─────────────────────────────────────────────────────────────────

export interface MatrixProps {
  payload: MatrixPayload;
  kind: Kind;
  rowKeys: string[];
  colKeys: string[];
  index: Map<string, number>;
  dimRows: Set<string>;
  dimCols: Set<string>;
  scale: number;
  view: View;
  pinned: Set<string>;
  onTogglePin: (key: string) => void;
  onToggleHidden: (key: string) => void;
  tipRef: React.MutableRefObject<TipApi | null>;
  /** The "data needed" view's MDE scale maximum. */
  mdeScale?: number;
}

export const Matrix = React.memo(function Matrix({
  payload, kind, rowKeys, colKeys, index, dimRows, dimCols, scale, view, pinned, onTogglePin, onToggleHidden, tipRef,
  mdeScale = 0,
}: MatrixProps) {
  const needed = view.view_mode === "data_needed" && !!payload.power;
  const target = view.target_delta > 0 ? view.target_delta : null;
  const unit = unitInfo(payload);
  const size = view.cell_size === "auto" ? (colKeys.length > 24 ? "compact" : "normal") : view.cell_size;
  const compact = size === "compact";
  const showInterval = view.show_ci && !compact;
  const W = compact ? 48 : 74;
  const H = compact ? 24 : showInterval ? 40 : 28;
  const headerH = kind === "transitions" ? 118 : 30;

  const onMove = (e: React.MouseEvent) => {
    const td = (e.target as HTMLElement).closest("td[data-testid=ab-cell]") as HTMLElement | null;
    if (!td) { tipRef.current?.hide(); return; }
    const idx = td.dataset.idx ? Number(td.dataset.idx) : null;
    tipRef.current?.show({ r: td.dataset.row ?? "", c: td.dataset.col ?? "", idx, x: e.clientX, y: e.clientY });
  };

  const colHeader = (c: string) => {
    const dim = dimCols.has(c);
    if (kind === "steps") {
      return (
        <th key={c} data-testid="ab-col-label" data-col={c} style={{
          position: "sticky", top: 0, zIndex: 2, background: "#fff", width: W, minWidth: W, maxWidth: W, height: headerH,
          fontSize: 11, fontWeight: 500, color: "#6b7280", borderBottom: "2px solid #e5e7eb", opacity: dim ? 0.35 : 1, padding: 0,
        }}>{c}</th>
      );
    }
    const isPinned = pinned.has(c);
    return (
      <th key={c} data-testid="ab-col-label" data-col={c} title={c} style={{
        position: "sticky", top: 0, zIndex: 2, background: "#fff", width: W, minWidth: W, maxWidth: W, height: headerH,
        verticalAlign: "bottom", borderBottom: "2px solid #e5e7eb", opacity: dim ? 0.35 : 1, padding: "2px 0",
      }}>
        <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2 }}>
          <div style={{
            writingMode: "vertical-rl", transform: "rotate(180deg)", maxHeight: headerH - 26, overflow: "hidden",
            textOverflow: "ellipsis", whiteSpace: "nowrap", fontSize: 11, fontWeight: isPinned ? 700 : 500, color: "#374151",
          }}>{c}</div>
          <button title="Hide event (row and column)" aria-label={`Hide ${c}`} onClick={() => onToggleHidden(c)} style={iconBtn(false, "#ef4444")}>
            <EyeIcon />
          </button>
        </div>
      </th>
    );
  };

  return (
    <table data-testid="ab-matrix" onMouseMove={onMove} onMouseLeave={() => tipRef.current?.hide()} style={{
      borderCollapse: "separate", borderSpacing: 0, fontFamily: "system-ui,-apple-system,sans-serif", tableLayout: "fixed",
    }}>
      <thead>
        <tr>
          <th style={{
            position: "sticky", top: 0, left: 0, zIndex: 3, background: "#fff", width: LABEL_W, minWidth: LABEL_W,
            maxWidth: LABEL_W, height: headerH, borderBottom: "2px solid #e5e7eb", borderRight: "2px solid #e5e7eb",
            verticalAlign: "bottom", textAlign: "left", padding: "4px 8px", fontSize: 10, fontWeight: 500, color: "#9ca3af",
          }}>
            {kind === "transitions" ? <>source ↓<br />target →</> : <>event ↓ step →</>}
          </th>
          {colKeys.map(colHeader)}
        </tr>
      </thead>
      <tbody>
        {rowKeys.map((r) => {
          const isPinned = pinned.has(r);
          const dimRow = dimRows.has(r);
          return (
            <tr key={r} data-event={r}>
              <td data-testid="ab-row-label" data-row={r} style={{
                position: "sticky", left: 0, zIndex: 1, background: "#fff", width: LABEL_W, minWidth: LABEL_W, maxWidth: LABEL_W,
                height: H, padding: "0 4px 0 8px", borderRight: "2px solid #e5e7eb", borderBottom: "1px solid #f3f4f6",
                opacity: dimRow ? 0.35 : 1,
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: 2 }}>
                  <span title={r} style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontSize: 11, fontWeight: isPinned ? 700 : 500, color: "#374151" }}>{r}</span>
                  <button title={isPinned ? "Unpin" : "Pin to the top"} aria-label={`Pin ${r}`} onClick={() => onTogglePin(r)} style={iconBtn(isPinned, "#f59e0b")}><PinIcon pinned={isPinned} /></button>
                  <button title={kind === "transitions" ? "Hide event (row and column)" : "Hide event"} aria-label={`Hide ${r}`} onClick={() => onToggleHidden(r)} style={iconBtn(false, "#ef4444")}><EyeIcon /></button>
                </div>
              </td>
              {colKeys.map((c) => {
                const idx = index.get(cellId(r, c));
                const row = idx != null ? payload.rows[idx] : undefined;
                const status = cellStatus(row, payload.alpha);
                const dim = dimRow || dimCols.has(c);
                if (needed) {
                  const need = needOf(row, idx != null ? payload.power?.scale_to_test?.[idx] : null, payload.alpha, target);
                  const words = row ? needText(need, unit) : null;
                  const hatched = !!row && !need.testable;
                  return (
                    <td key={c} data-testid="ab-cell" data-row={r} data-col={c} data-step={kind === "steps" ? c : undefined}
                      data-idx={idx != null ? String(idx) : undefined} data-status={status} data-mode="data_needed"
                      data-hatched={hatched ? "true" : "false"} data-times={need.times != null ? String(need.times) : undefined}
                      data-dim={dim ? "true" : "false"}
                      style={{
                        width: W, minWidth: W, maxWidth: W, height: H, padding: 0, boxSizing: "border-box",
                        backgroundColor: row ? needBackground(need, mdeScale, target != null) : "#fff",
                        backgroundImage: hatched ? hatchImage() : undefined,
                        outline: hatched ? `1px dashed ${OUTLINE_GREY}` : undefined,
                        outlineOffset: hatched ? "-2px" : undefined,
                        borderRight: "1px solid #fff", borderBottom: "1px solid #fff",
                        textAlign: "center", verticalAlign: "middle", color: hatched ? TEXT_MUTED : "#111827",
                        opacity: dim ? 0.3 : 1, fontVariantNumeric: "tabular-nums", lineHeight: 1.15, overflow: "hidden",
                        whiteSpace: "nowrap",
                      }}>
                      {words && (!compact || need.testable) && (
                        <div style={{ fontSize: compact ? 10 : 11 }}>{words.main}</div>
                      )}
                      {words && words.times && (!compact || !need.testable) && (
                        <div data-testid="ab-cell-times" style={{ fontSize: compact ? 9 : 9, color: hatched ? TEXT_MUTED : "#4b5563" }}>{words.times}</div>
                      )}
                    </td>
                  );
                }
                const look = cellLook(status);
                const text = row ? cellText(row, unit, view.interval_kind) : null;
                return (
                  <td key={c} data-testid="ab-cell" data-row={r} data-col={c} data-step={kind === "steps" ? c : undefined}
                    data-idx={idx != null ? String(idx) : undefined} data-status={status} data-sign={deltaSign(row?.delta)}
                    data-cross={look.cross ? "true" : "false"} data-dim={dim ? "true" : "false"}
                    style={{
                      width: W, minWidth: W, maxWidth: W, height: H, padding: 0, boxSizing: "border-box",
                      backgroundColor: row ? cellBackground(row.delta, scale) : "#fff",
                      backgroundImage: look.cross ? crossImage() : undefined,
                      outline: look.outlineColor ? `${OUTLINE_WIDTH}px ${look.outlineStyle} ${look.outlineColor}` : undefined,
                      outlineOffset: look.outlineColor ? `-${OUTLINE_WIDTH + 1.5}px` : undefined,
                      borderRight: "1px solid #fff", borderBottom: "1px solid #fff",
                      textAlign: "center", verticalAlign: "middle", color: look.textColor,
                      opacity: dim ? 0.3 : 1, fontVariantNumeric: "tabular-nums", lineHeight: 1.15, overflow: "hidden",
                      whiteSpace: "nowrap",
                    }}>
                    {text && (
                      <div style={{ fontSize: compact ? 10 : 11, fontWeight: status === "significant" ? 700 : 400 }}>{text.delta}</div>
                    )}
                    {text && showInterval && text.interval && (
                      <div data-testid="ab-cell-interval" style={{ fontSize: 9, color: status === "untested" ? TEXT_MUTED : "#4b5563" }}>{text.interval}</div>
                    )}
                  </td>
                );
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
});

// ── header and legend ──────────────────────────────────────────────────────

/** The header's curve: testable cells and median MDE against the users. */
export function PowerCurve({ power, unit, target }: { power: PowerInfo; unit: ReturnType<typeof unitInfo>; target: number | null }) {
  const W = 230, H = 64, PAD = 6;
  const g = curveGeometry(power.curve, W, H, PAD, target);
  if (!g.points.length) return null;
  const now = power.curve.find((p) => p.scale === 1);
  const suffix = unit.suffix ? ` ${unit.suffix}` : "";
  const fmtMde = (v: number | null | undefined) =>
    v == null || !Number.isFinite(v) ? "n/a" : `${(v * unit.scale).toFixed(decimalsFor(unit, v))}${suffix}`;
  const top = power.curve[power.curve.length - 1];
  return (
    <div data-testid="ab-power-curve" style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
      <svg width={W} height={H + 12} viewBox={`0 0 ${W} ${H + 12}`} role="img"
        aria-label="Testable cells and median MDE against the users" style={{ overflow: "visible" }}>
        <rect x={0} y={0} width={W} height={H} fill="#f9fafb" stroke="#e5e7eb" />
        {g.currentX != null && <line data-testid="ab-curve-now" x1={g.currentX} x2={g.currentX} y1={0} y2={H} stroke="#6b7280" strokeDasharray="3 2" />}
        {g.targetY != null && <line data-testid="ab-curve-target" x1={0} x2={W} y1={g.targetY} y2={g.targetY} stroke={`rgb(${NEED_HUE})`} strokeDasharray="2 3" strokeWidth={1} />}
        <path data-testid="ab-curve-testable" d={g.testablePath} fill="none" stroke="#374151" strokeWidth={1.5} />
        <path data-testid="ab-curve-mde" d={g.mdePath} fill="none" stroke={`rgb(${NEED_HUE})`} strokeWidth={1.5} />
        {g.points.map((p) => (
          <g key={p.scale}>
            <circle cx={p.x} cy={p.yTestable} r={p.scale === 1 ? 3 : 2} fill="#374151" />
            {p.yMde != null && <circle cx={p.x} cy={p.yMde} r={p.scale === 1 ? 3 : 2} fill={`rgb(${NEED_HUE})`} />}
            <text x={p.x} y={H + 10} fontSize={8} textAnchor="middle" fill={p.scale === 1 ? "#111827" : "#9ca3af"}>{`×${p.scale}`}</text>
          </g>
        ))}
      </svg>
      <div style={{ fontSize: 10, color: "#374151", lineHeight: 1.45 }}>
        <div><span style={{ color: "#374151" }}>━</span> testable cells: <b>{now?.testable ?? "?"}</b> of {now?.cells ?? "?"} now, {top?.testable ?? "?"} at ×{top?.scale}</div>
        <div><span style={{ color: `rgb(${NEED_HUE})` }}>━</span> median MDE: <b>{fmtMde(now?.mde_median)}</b> now, {fmtMde(top?.mde_median)} at ×{top?.scale}</div>
        <div style={{ color: "#6b7280" }}>x: users (both arms) as a multiple of now; dashed: now{target != null ? "; dotted: target" : ""}</div>
      </div>
    </div>
  );
}

/** The switch between the two views. */
export function ViewSwitch({ mode, onChange, available }: { mode: ViewMode; onChange: (m: ViewMode) => void; available: boolean }) {
  const btn = (m: ViewMode, label: string) => (
    <button key={m} data-testid={`ab-view-${m}`} aria-pressed={mode === m} disabled={!available && m === "data_needed"}
      onClick={() => onChange(m)} style={{
        border: "1px solid #d1d5db", borderLeft: m === "data_needed" ? "none" : "1px solid #d1d5db",
        borderRadius: m === "delta" ? "6px 0 0 6px" : "0 6px 6px 0", padding: "2px 10px", fontSize: 11, cursor: "pointer",
        background: mode === m ? "#111827" : "#fff", color: mode === m ? "#fff" : "#374151",
      }}>{label}</button>
  );
  return <div data-testid="ab-view-switch" style={{ display: "inline-flex", marginBottom: 6 }}>{btn("delta", "Delta")}{btn("data_needed", "Data needed")}</div>;
}

export function NeedLegend({ payload, kind, mdeScale, view }: { payload: MatrixPayload; kind: Kind; mdeScale: number; view: View }) {
  const unit = unitInfo(payload);
  const suffix = unit.suffix ? ` ${unit.suffix}` : "";
  const target = view.target_delta > 0 ? view.target_delta : null;
  const what = kind === "transitions" ? `edge weight ${payload.edge_weight}` : `steps 1–${payload.max_steps}`;
  const item = (node: React.ReactNode, text: string, id: string) => (
    <span key={id} style={{ display: "inline-flex", alignItems: "center", gap: 6, marginRight: 14 }}>{node}<span>{text}</span></span>
  );
  const ramp = (
    <span style={{ display: "inline-block", width: 90, height: 12, borderRadius: 2, background: `linear-gradient(to right, rgba(${NEED_HUE}, 0), rgba(${NEED_HUE}, ${MAX_ALPHA}))` }} />
  );
  const fmt = (v: number) => `${(v * unit.scale).toFixed(decimalsFor(unit, v))}${suffix}`;
  return (
    <div data-testid="ab-legend" data-mode="data_needed" style={{ fontSize: 11, color: "#374151", lineHeight: 1.5 }}>
      <div data-testid="ab-header" style={{ fontSize: 12, color: "#111827", marginBottom: 4 }}>
        <b>Data needed</b>
        <span style={{ color: "#6b7280" }}> · {what} · {populationText(payload)} · {familyText(payload)} · power {payload.attrs?.power != null ? `${+(Number(payload.attrs.power) * 100).toFixed(0)}%` : "80%"}</span>
      </div>
      {payload.power && <PowerCurve power={payload.power} unit={unit} target={target} />}
      <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", rowGap: 4, marginTop: 4 }}>
        {item(ramp, target != null
          ? `background: users needed for an MDE of ${fmt(target)}, none at ×1 or less … full at ×${NEED_MAX_TIMES}`
          : `background: MDE (minimum detectable effect), full at ${fmt(mdeScale)}`, "bg")}
        {item(
          <span data-testid="ab-legend-hatched" style={{ display: "inline-block", width: 26, height: 16, backgroundImage: hatchImage(), outline: `1px dashed ${HATCH_COLOR}`, outlineOffset: "-1px" }} />,
          "not testable now: ×k = the users at which it passes the support rule", "hatch",
        )}
      </div>
      <div style={{ color: "#6b7280", fontSize: 10, marginTop: 2 }}>
        Cells: MDE in {unit.label}{target != null ? ", ×k users to reach the target below it" : ""}. MDE at {payload.power?.level ?? "alpha / m"}; projections assume {payload.power?.assumption ?? "the same path composition"}.
      </div>
    </div>
  );
}

export function Legend({ payload, kind, scale, view }: { payload: MatrixPayload; kind: Kind; scale: number; view: View }) {
  const unit = unitInfo(payload);
  const top = scale * unit.scale;
  const d = decimalsFor(unit, scale);
  const suffix = unit.suffix ? ` ${unit.suffix}` : "";
  const g1 = payload.group1_label, g2 = payload.group2_label;
  const what = kind === "transitions"
    ? `edge weight ${payload.edge_weight}${payload.edges === "declared" ? " (declared edges)" : ""}`
    : `steps 1–${payload.max_steps}`;
  const alpha = payload.alpha;
  const item = (node: React.ReactNode, text: string, id: string) => (
    <span key={id} style={{ display: "inline-flex", alignItems: "center", gap: 6, marginRight: 14 }}>{node}<span>{text}</span></span>
  );
  return (
    <div data-testid="ab-legend" style={{ fontSize: 11, color: "#374151", lineHeight: 1.5 }}>
      <div data-testid="ab-header" style={{ fontSize: 12, color: "#111827", marginBottom: 4 }}>
        <span style={{ color: `rgb(${MATRIX_RED})` }}>●</span> <b>{g1}</b> vs <span style={{ color: `rgb(${MATRIX_BLUE})` }}>●</span> <b>{g2}</b>
        <span style={{ color: "#6b7280" }}> · {what} · {populationText(payload)} · {familyText(payload)} · alpha {alpha}</span>
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", rowGap: 4 }}>
        {item(
          <span style={{
            display: "inline-block", width: 90, height: 12, borderRadius: 2,
            background: `linear-gradient(to right, rgba(${MATRIX_BLUE}, ${MAX_ALPHA}), rgba(${MATRIX_BLUE}, 0) 50%, rgba(${MATRIX_RED}, 0) 50%, rgba(${MATRIX_RED}, ${MAX_ALPHA}))`,
          }} />,
          `background: delta ${g1} − ${g2}, blue lower … red higher, full at ±${top.toFixed(d)}${suffix}${view.color_clip > 0 ? " (clipped)" : ""}`,
          "bg",
        )}
        {item(<Swatch status="significant" />, `significantly non-zero (adjusted p < ${alpha})`, "sig")}
        {item(<Swatch status="not_significant" />, "cannot say it is non-zero (tested)", "ns")}
        {item(<Swatch status="untested" />, "not tested (dashed)", "nt")}
        {item(<Swatch status="empty" />, "never occurs in either arm", "empty")}
      </div>
      <div style={{ color: "#6b7280", fontSize: 10, marginTop: 2 }}>
        Cells: delta in {unit.label}{view.show_ci ? ", interval below it" : ""}. {intervalNote(payload.ci_level, view.interval_kind, payload.simultaneous)}
      </div>
    </div>
  );
}

// ── sidebar ────────────────────────────────────────────────────────────────

function NumberInput({ label, value, min, disabled, onCommit }: {
  label: string; value: number; min: number; disabled: boolean; onCommit: (v: number) => void;
}) {
  const [text, setText] = React.useState(String(value));
  React.useEffect(() => setText(String(value)), [value]);
  const n = Number(text);
  const valid = Number.isInteger(n) && n >= min;
  const commit = () => { if (valid && n !== value) onCommit(n); };
  return (
    <input aria-label={label} value={text} disabled={disabled} inputMode="numeric"
      onChange={(e) => setText(e.target.value)} onBlur={commit} onKeyDown={(e) => { if (e.key === "Enter") commit(); }}
      style={{ width: "100%", boxSizing: "border-box", border: `1px solid ${valid ? "#d1d5db" : "#fca5a5"}`, borderRadius: 6, padding: "5px 8px", fontSize: 12, outline: "none", background: disabled ? "#f9fafb" : "#fff" }} />
  );
}

function ClipInput({ view, unitScale, suffix, autoScale, onCommit }: {
  view: View; unitScale: number; suffix: string; autoScale: number; onCommit: (v: number) => void;
}) {
  const shown = view.color_clip > 0 ? String(+(view.color_clip * unitScale).toPrecision(4)) : "";
  const [text, setText] = React.useState(shown);
  React.useEffect(() => setText(shown), [shown]);
  const n = Number(text);
  const valid = text.trim() === "" || (Number.isFinite(n) && n > 0);
  const commit = () => {
    if (!valid) return;
    onCommit(text.trim() === "" ? 0 : n / unitScale);
  };
  return (
    <div style={{ display: "flex", gap: 6 }}>
      <input aria-label="Colour scale maximum" value={text} placeholder={`auto (${+(autoScale * unitScale).toPrecision(3)}${suffix ? ` ${suffix}` : ""})`}
        onChange={(e) => setText(e.target.value)} onBlur={commit} onKeyDown={(e) => { if (e.key === "Enter") commit(); }}
        style={{ flex: 1, minWidth: 0, boxSizing: "border-box", border: `1px solid ${valid ? "#d1d5db" : "#fca5a5"}`, borderRadius: 6, padding: "5px 8px", fontSize: 12, outline: "none" }} />
      <button onClick={() => { setText(""); onCommit(0); }} disabled={view.color_clip === 0}
        style={{ border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", fontSize: 11, padding: "0 8px", cursor: view.color_clip ? "pointer" : "default", color: view.color_clip ? "#374151" : "#c4c9d1" }}>
        Auto
      </button>
    </div>
  );
}

function TargetInput({ view, unitScale, onCommit }: { view: View; unitScale: number; onCommit: (v: number) => void }) {
  const shown = view.target_delta > 0 ? String(+(view.target_delta * unitScale).toPrecision(4)) : "";
  const [text, setText] = React.useState(shown);
  React.useEffect(() => setText(shown), [shown]);
  const n = Number(text);
  const valid = text.trim() === "" || (Number.isFinite(n) && n > 0);
  const commit = () => { if (valid) onCommit(text.trim() === "" ? 0 : n / unitScale); };
  return (
    <input aria-label="Target delta" value={text} placeholder="none"
      onChange={(e) => setText(e.target.value)} onBlur={commit} onKeyDown={(e) => { if (e.key === "Enter") commit(); }}
      style={{ width: "100%", boxSizing: "border-box", border: `1px solid ${valid ? "#d1d5db" : "#fca5a5"}`, borderRadius: 6, padding: "5px 8px", fontSize: 12, outline: "none" }} />
  );
}

function fixedText(key: string, value: unknown): string {
  if (key === "edges") {
    const list = parseJson<unknown[]>(value, []);
    return Array.isArray(list) && list.length ? `${list.length} declared` : "all (screen)";
  }
  if (key === "horizon") return value ? String(value) : "none (whole window)";
  if (key === "path_col") return value ? String(value) : "schema default";
  if (value === null || value === undefined || value === "") return "default";
  return String(value);
}

function Sidebar({ kind, host, isStatic, isLoading, params, view, setParam, setView, payload, autoScale }: {
  kind: Kind; host: RenderContext["host"]; isStatic: boolean; isLoading: boolean; params: Record<string, any>;
  view: View; setParam: (k: string, v: unknown) => void; setView: <K extends keyof View>(k: K, v: View[K]) => void;
  payload: MatrixPayload | null; autoScale: number;
}) {
  const disabled = isLoading || isStatic;
  const unit = payload ? unitInfo(payload) : { scale: 100, suffix: "pp", label: "" };
  const diff = parseJson<unknown>(host.get("diff"), null);
  const diffText = Array.isArray(diff) && diff.length === 3
    ? `${diff[0]}: ${diff[1]} vs ${diff[2]}`
    : diff && typeof diff === "object" && "group1_ids" in (diff as object)
      ? `id lists: ${(diff as any).group1_ids} vs ${(diff as any).group2_ids} ids`
      : String(diff ?? "");
  const assignment = (host.get("assignment_info") as string) || "";
  const hiddenKeys = Object.entries(view.visibility).filter(([, v]) => v.isHidden).map(([k]) => k);
  const ci = Number(params.ci_level) || 0.95;
  const fixed: [string, string][] = [
    ["diff", diffText],
    ["assignment", assignment ? `table (ITT${assignment.includes("assigned_at") ? ", with assigned_at" : ""})` : "none: the stream's paths"],
    ...(kind === "transitions"
      ? (["horizon", "path_col", "edges", "test", "power", "n_bootstrap", "bootstrap_weights", "random_state"] as const)
      : (["horizon", "path_col", "power"] as const)
    ).map((k) => [k, fixedText(k, params[k])] as [string, string]),
  ];
  const hint = kind === "transitions" ? readingHint(String(params.edge_weight)) : null;

  return (
    <div data-testid="ab-sidebar" style={{ width: SIDEBAR_W, minWidth: SIDEBAR_W, height: "100%", background: "#fff", borderLeft: "1px solid #e5e7eb", display: "flex", flexDirection: "column", overflow: "hidden", fontFamily: "system-ui, sans-serif" }}>
      <div style={{ padding: "10px 14px", borderBottom: "1px solid #e5e7eb", display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#111827" }}>Settings</span>
        {isLoading && <span style={{ fontSize: 11, color: "#6b7280", marginRight: 34 }}>Computing…</span>}
      </div>
      <div style={{ flex: 1, overflowY: "auto", padding: 14 }}>
        <SH>Test (recomputes)</SH>
        {kind === "transitions" ? (
          <>
            <Label>Edge weight</Label>
            <select aria-label="Edge weight" value={params.edge_weight} disabled={disabled} style={sel}
              onChange={(e) => setParam("edge_weight", e.target.value)}>
              {WEIGHTS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
            </select>
          </>
        ) : (
          <>
            <Label>Max steps</Label>
            <NumberInput label="Max steps" value={Number(params.max_steps) || 10} min={1} disabled={disabled}
              onCommit={(v) => setParam("max_steps", v)} />
          </>
        )}
        <Label>Correction</Label>
        <select aria-label="Correction" value={params.correction} disabled={disabled} style={sel}
          onChange={(e) => setParam("correction", e.target.value)}>
          {CORRECTIONS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
        <Label>Confidence level</Label>
        <select aria-label="Confidence level" value={String(ci)} disabled={disabled} style={sel}
          onChange={(e) => setParam("ci_level", Number(e.target.value))}>
          {(CI_LEVELS.includes(ci) ? CI_LEVELS : [...CI_LEVELS, ci].sort((a, b) => a - b))
            .map((v) => <option key={v} value={String(v)}>{`${+(v * 100).toFixed(1)}%`}</option>)}
        </select>
        <Label>Min support (pooled paths)</Label>
        <NumberInput label="Min support" value={Number(params.min_support) || 10} min={1} disabled={disabled}
          onCommit={(v) => setParam("min_support", v)} />
        {isStatic && <Hint>A static export has no kernel: these are the values it was computed with.</Hint>}

        <Rule />
        <SH>View</SH>
        <Label>Show</Label>
        <select aria-label="View mode" value={view.view_mode} style={sel} disabled={!payload?.power}
          onChange={(e) => setView("view_mode", e.target.value as ViewMode)}>
          <option value="delta">Delta (the difference)</option>
          <option value="data_needed">Data needed (MDE, users)</option>
        </select>
        <Label>Target delta{unit.suffix ? ` (${unit.suffix})` : ""}</Label>
        <TargetInput view={view} unitScale={unit.scale} onCommit={(v) => setView("target_delta", v)} />
        <Hint>Data needed: the users each cell needs for its MDE to reach this; empty = show the MDE.</Hint>
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#374151", marginTop: 8 }}>
          <input type="checkbox" aria-label="Show intervals" checked={view.show_ci} onChange={(e) => setView("show_ci", e.target.checked)} />
          Show intervals in the cells
        </label>
        <Label>Intervals</Label>
        <select aria-label="Interval kind" value={view.interval_kind} style={sel} disabled={!payload?.simultaneous}
          onChange={(e) => setView("interval_kind", e.target.value as IntervalKind)}>
          <option value="per_cell">Per cell (not simultaneous)</option>
          <option value="simultaneous">Simultaneous (Bonferroni)</option>
        </select>
        {!payload?.simultaneous && <Hint>Simultaneous intervals need correction = Bonferroni.</Hint>}
        <Label>Significance filter</Label>
        <select aria-label="Significance filter" value={view.significance_filter} style={sel}
          onChange={(e) => setView("significance_filter", e.target.value as SignificanceFilter)}>
          {FILTERS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
        <Label>Colour scale maximum{unit.suffix ? ` (${unit.suffix})` : ""}</Label>
        <ClipInput view={view} unitScale={unit.scale} suffix={unit.suffix} autoScale={autoScale}
          onCommit={(v) => setView("color_clip", v)} />
        <Hint>Clip below the largest |delta| so that small effects stay visible.</Hint>
        <Label>Sort rows</Label>
        <select aria-label="Sort rows" value={view.row_sort} style={sel} onChange={(e) => setView("row_sort", e.target.value as SortKey)}>
          {SORTS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
        {kind === "transitions" && (
          <>
            <Label>Sort columns</Label>
            <select aria-label="Sort columns" value={view.col_sort} style={sel} onChange={(e) => setView("col_sort", e.target.value as SortKey)}>
              {SORTS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
            </select>
          </>
        )}
        <Label>Cell size</Label>
        <select aria-label="Cell size" value={view.cell_size} style={sel} onChange={(e) => setView("cell_size", e.target.value)}>
          {CELL_SIZES.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
        {hint && (
          <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#374151", marginTop: 10 }}>
            <input type="checkbox" aria-label="Reading hint" checked={view.reading_hint} onChange={(e) => setView("reading_hint", e.target.checked)} />
            Reading hint ({String(params.edge_weight)})
          </label>
        )}
        {hiddenKeys.length > 0 && (
          <>
            <Label>Hidden events</Label>
            <div data-testid="ab-hidden-list" style={{ display: "flex", flexDirection: "column", gap: 3 }}>
              {hiddenKeys.map((k) => (
                <div key={k} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11 }}>
                  <span style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: "#6b7280" }}>{k}</span>
                  <button onClick={() => setView("visibility", { ...view.visibility, [k]: { ...view.visibility[k], isHidden: false } })}
                    style={{ border: "1px solid #e5e7eb", borderRadius: 5, background: "#fff", fontSize: 10, cursor: "pointer", padding: "1px 6px" }}>show</button>
                </div>
              ))}
              <button onClick={() => {
                const next: View["visibility"] = {};
                Object.entries(view.visibility).forEach(([k, v]) => { if (v.isPinned) next[k] = { isHidden: false, isPinned: true }; });
                setView("visibility", next);
              }} style={{ marginTop: 4, border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", fontSize: 11, cursor: "pointer", padding: "3px 0" }}>Show all</button>
            </div>
          </>
        )}

        <Rule />
        <SH>Fixed at construction</SH>
        <div data-testid="ab-fixed" style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          {fixed.map(([k, v]) => (
            <div key={k} style={{ display: "flex", justifyContent: "space-between", gap: 8, fontSize: 11 }}>
              <span style={{ color: "#6b7280", fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" }}>{k}</span>
              <span style={{ color: "#111827", textAlign: "right", overflowWrap: "anywhere" }}>{v}</span>
            </div>
          ))}
        </div>
        <Hint>Set in code: pass them again to change them.</Hint>
      </div>
    </div>
  );
}

// ── main render ────────────────────────────────────────────────────────────

const VIEW_TRAITS: Record<keyof View, string> = {
  show_ci: "show_ci",
  interval_kind: "interval_kind",
  significance_filter: "significance_filter",
  color_clip: "color_clip",
  row_sort: "row_sort",
  col_sort: "col_sort",
  cell_size: "cell_size",
  visibility: "event_visibility",
  reading_hint: "reading_hint",
  view_mode: "view_mode",
  target_delta: "target_delta",
};

export function render({ host, el, isStatic = false }: RenderContext) {
  const kind: Kind = host.get("widget_type") === "ab_steps" ? "steps" : "transitions";

  function App() {
    const [payload, setPayload] = React.useState<MatrixPayload | null>(() => parsePayload(host.get("result")));
    const [isLoading, setIsLoading] = React.useState<boolean>(() => (host.get("is_loading") as boolean) ?? false);
    const [error, setError] = React.useState<string>(() => (host.get("error") as string) || "");
    const [height, setHeight] = React.useState<number>(() => (host.get("height") as number) || 600);
    const [sidebarOpen, setSidebarOpen] = React.useState<boolean>(() => (host.get("sidebar_open") as boolean) ?? true);
    const [params, setParams] = React.useState(() => readParams(host, kind));
    const [view, setViewState] = React.useState<View>(() => readView(host));
    const tipRef = React.useRef<TipApi | null>(null);

    useHostSubscriptions(host, [
      ["result", () => setPayload(parsePayload(host.get("result")))],
      ["is_loading", () => setIsLoading((host.get("is_loading") as boolean) ?? false)],
      ["error", () => setError((host.get("error") as string) || "")],
      ["height", () => setHeight((host.get("height") as number) || 600)],
      ["sidebar_open", () => setSidebarOpen((host.get("sidebar_open") as boolean) ?? true)],
      ...DATA_KEYS[kind].map((k) => [k, () => setParams(readParams(host, kind))] as [string, () => void]),
    ]);

    const setParam = React.useCallback((key: string, value: unknown) => {
      setParams((p) => ({ ...p, [key]: value }));
      host.set(key, value);
    }, []);
    const setView = React.useCallback(<K extends keyof View>(key: K, value: View[K]) => {
      setViewState((v) => ({ ...v, [key]: value }));
      host.set(VIEW_TRAITS[key], key === "visibility" ? JSON.stringify(value) : value);
    }, []);

    const pinned = React.useMemo(
      () => new Set(Object.entries(view.visibility).filter(([, v]) => v.isPinned).map(([k]) => k)), [view.visibility],
    );
    const hidden = React.useMemo(
      () => new Set(Object.entries(view.visibility).filter(([, v]) => v.isHidden).map(([k]) => k)), [view.visibility],
    );
    const togglePin = React.useCallback((key: string) => {
      setViewState((v) => {
        const cur = v.visibility[key] ?? { isHidden: false, isPinned: false };
        const visibility = { ...v.visibility, [key]: { ...cur, isPinned: !cur.isPinned } };
        host.set("event_visibility", JSON.stringify(visibility));
        return { ...v, visibility };
      });
    }, []);
    const toggleHidden = React.useCallback((key: string) => {
      setViewState((v) => {
        const cur = v.visibility[key] ?? { isHidden: false, isPinned: false };
        const visibility = { ...v.visibility, [key]: { ...cur, isHidden: !cur.isHidden } };
        host.set("event_visibility", JSON.stringify(visibility));
        return { ...v, visibility };
      });
    }, []);

    const layout = React.useMemo(() => {
      if (!payload) return null;
      const index = indexRows(payload);
      let rowKeys = payload.row_keys.filter((k) => !hidden.has(k));
      let colKeys = payload.col_keys.map(String).filter((k) => kind === "steps" || !hidden.has(k));
      const cells: { r: string; c: string; row: MatrixRow }[] = [];
      for (const r of rowKeys) {
        for (const c of colKeys) {
          const i = index.get(cellId(r, c));
          if (i != null) cells.push({ r, c, row: payload.rows[i] });
        }
      }
      const stats = axisStats(cells, payload.alpha);
      const colPinned = kind === "transitions" ? pinned : new Set<string>();
      const dimRows = new Set<string>();
      const dimCols = new Set<string>();
      if (view.significance_filter !== "all") {
        const keepRow = (k: string) => keepsSignificance(k, stats.rows, pinned);
        const keepCol = (k: string) => keepsSignificance(k, stats.cols, colPinned);
        if (view.significance_filter === "hide") {
          rowKeys = rowKeys.filter(keepRow);
          colKeys = colKeys.filter(keepCol);
        } else {
          rowKeys.filter((k) => !keepRow(k)).forEach((k) => dimRows.add(k));
          colKeys.filter((k) => !keepCol(k)).forEach((k) => dimCols.add(k));
        }
      }
      rowKeys = orderKeys(rowKeys, view.row_sort, stats.rows, pinned);
      if (kind === "transitions") colKeys = orderKeys(colKeys, view.col_sort, stats.cols, colPinned);
      const shownRows = new Set(rowKeys);
      const shownCols = new Set(colKeys);
      const shown = cells.filter((x) => shownRows.has(x.r) && shownCols.has(x.c)).map((x) => x.row);
      return {
        index, rowKeys, colKeys, dimRows, dimCols,
        scale: scaleMax(shown, view.color_clip),
        autoScale: scaleMax(shown, 0),
        mdeScale: mdeScaleMax(shown, payload.alpha),
        nSignificant: shown.filter((row) => cellStatus(row, payload.alpha) === "significant").length,
        nTestable: shown.filter((row) => { const st = cellStatus(row, payload.alpha); return st === "significant" || st === "not_significant"; }).length,
        nShown: shown.length,
      };
    }, [payload, view.significance_filter, view.row_sort, view.col_sort, view.color_clip, hidden, pinned]);

    // report links: scrollToEvent("event@step") calls this (main.tsx)
    React.useEffect(() => {
      (el as HTMLElement & { __matrixApi?: unknown }).__matrixApi = {
        scrollToCell: (r: string, c: string | number) => {
          const td = el.querySelector(`td[data-testid=ab-cell][data-row="${CSS.escape(String(r))}"][data-col="${CSS.escape(String(c))}"]`) as HTMLElement | null;
          if (!td) return;
          td.scrollIntoView({ block: "nearest", inline: "center", behavior: "smooth" });
          focusCells(el, [td]);
        },
      };
      return () => { delete (el as HTMLElement & { __matrixApi?: unknown }).__matrixApi; };
    }, []);

    const handleToggle = () => setSidebarOpen((p) => { const n = !p; host.set("sidebar_open", n); return n; });
    const needed = view.view_mode === "data_needed" && !!payload?.power;
    const hint = kind === "transitions" && payload && view.reading_hint ? readingHint(payload.edge_weight) : null;

    return (
      <div data-testid={`ab-matrix-widget-${kind}`} style={{ position: "relative", display: "flex", flexDirection: "row", height, background: "#ffffff", borderRadius: 8, overflow: "hidden", border: "1px solid #e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
        <div style={{ flex: 1, position: "relative", overflow: "hidden", minWidth: 0, display: "flex", flexDirection: "column" }}>
          <SidebarToggle onClick={handleToggle} />
          <div style={{ padding: "10px 52px 8px 12px", borderBottom: "1px solid #e5e7eb", flexShrink: 0, maxHeight: "45%", overflowY: "auto" }}>
            {payload && <ViewSwitch mode={needed ? "data_needed" : "delta"} available={!!payload.power} onChange={(m) => setView("view_mode", m)} />}
            {payload && layout && !needed && <Legend payload={payload} kind={kind} scale={layout.scale} view={view} />}
            {payload && layout && needed && <NeedLegend payload={payload} kind={kind} mdeScale={layout.mdeScale} view={view} />}
            {payload && layout && (
              <div data-testid="ab-counts" style={{ fontSize: 10, color: "#6b7280", marginTop: 2 }}>
                {layout.rowKeys.length} rows × {layout.colKeys.length} columns shown, {needed
                  ? `${layout.nTestable} of ${layout.nShown} cells testable now`
                  : `${layout.nSignificant} significant cells`}
              </div>
            )}
            {hint && (
              <div data-testid="ab-reading-hint" style={{ marginTop: 6, padding: "6px 8px", background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 6, fontSize: 11, color: "#374151", lineHeight: 1.45 }}>
                {hint}
              </div>
            )}
            {error && !isLoading && (
              <div data-testid="ab-error" style={{ marginTop: 6, padding: 10, background: "#fff1f2", border: "1px solid #fca5a5", borderRadius: 8, fontSize: 12, color: "#dc2626", whiteSpace: "pre-wrap" }}>{error}</div>
            )}
            {!!payload?.warnings?.length && (
              <div data-testid="ab-warnings" style={{ marginTop: 6, padding: "6px 8px", background: "#fffbeb", border: "1px solid #fcd34d", borderRadius: 8, fontSize: 11, color: "#92400e", lineHeight: 1.5 }}>
                {payload.warnings.map((w, i) => <div key={i}>⚠ {w}</div>)}
              </div>
            )}
          </div>
          <div data-testid="ab-scroll" style={{ flex: 1, minHeight: 0, overflow: "auto", position: "relative" }}>
            {payload && layout && layout.rowKeys.length > 0 && (
              <Matrix payload={payload} kind={kind} rowKeys={layout.rowKeys} colKeys={layout.colKeys} index={layout.index}
                dimRows={layout.dimRows} dimCols={layout.dimCols} scale={layout.scale} view={view} pinned={pinned}
                onTogglePin={togglePin} onToggleHidden={toggleHidden} tipRef={tipRef} mdeScale={layout.mdeScale} />
            )}
            {payload && layout && layout.rowKeys.length === 0 && (
              <div style={{ padding: 24, color: "#9ca3af", fontSize: 13, textAlign: "center" }}>
                {view.significance_filter === "hide" ? "No significant cell: every row is hidden by the filter." : "Every row is hidden."}
              </div>
            )}
            {!payload && !isLoading && !error && (
              <div style={{ padding: 24, color: "#9ca3af", fontSize: 13, textAlign: "center" }}>No data</div>
            )}
          </div>
          {payload && <Tooltip apiRef={tipRef} payload={payload} kind={kind} mode={needed ? "data_needed" : "delta"} target={view.target_delta} />}
          {isLoading && <ComputingSpinner />}
        </div>
        {sidebarOpen && (
          <Sidebar kind={kind} host={host} isStatic={isStatic} isLoading={isLoading} params={params} view={view}
            setParam={setParam} setView={setView} payload={payload} autoScale={layout?.autoScale ?? 0} />
        )}
        <RetentioneeringSpinKeyframes />
      </div>
    );
  }

  const root = createRoot(el);
  root.render(<App />);
  return () => root.unmount();
}
