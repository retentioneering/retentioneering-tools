"use client";
import * as React from "react";
import * as SliderPrimitive from "@radix-ui/react-slider";
import { observer } from "mobx-react-lite";
import { TransitionMatrixStore } from "../../stores/TransitionMatrixStore";
import { RangeSlider } from "../TransitionGraph/RangeSlider";
import { type MatrixValueType } from "../../utils/value-types";
import { formatNumber, formatPopulation } from "../../utils/format-number";
import { formatTime } from "../../utils/format-time";
import { isTimeValueType } from "../../utils/value-types";

// ── constants ──────────────────────────────────────────────────────────────

export const SIDEBAR_WIDTH = 280;

/** Diff-mode sentinel meaning "every other value of this segment column". */
const REST_VALUE = "<REST>";
const REST_LABEL = "Rest (everyone else)";

const C = {
  bg: "#ffffff", bgSection: "#f9fafb",
  border: "#e5e7eb", borderLight: "#d1d5db",
  text: "#111827", muted: "#6b7280", mutedLight: "#4b5563",
  accent: "hsl(45, 93%, 58%)",
  accentFg: "hsl(0, 0%, 10%)",
} as const;

const VALUE_OPTIONS: { value: MatrixValueType; label: string; tooltip: string }[] = [
  { value: "unique_paths", label: "Unique Paths",    tooltip: "Number of unique paths that have an A→B transition." },
  { value: "count",        label: "Count",           tooltip: "Total number of A→B transitions." },
  { value: "share_of_total", label: "Share of Total", tooltip: "Count divided by all transitions: #(A→B) / #(*→*)." },
  { value: "avg_per_path",    label: "Avg per Path",       tooltip: "Count divided by total paths: #(A→B) / total paths." },
  { value: "proba_out",   label: "Probability Out",  tooltip: "P(A→B) = #(A→B) / #(A→*). Markov transition probabilities." },
  { value: "proba_in",    label: "Probability In",   tooltip: "P(A→B) = #(A→B) / #(*→B)." },
  { value: "time_median", label: "Time Median",      tooltip: "Median time the A→B transition takes." },
  { value: "time_q95",    label: "Time Q95",         tooltip: "95th percentile of time the A→B transition takes." },
];

// ── SingleSlider ───────────────────────────────────────────────────────────

function SingleSlider({ min, max, value, onChange }: {
  min: number; max: number; value: number; onChange: (v: number) => void;
}) {
  return (
    <SliderPrimitive.Root
      style={{ position: "relative", display: "flex", height: 20, width: "100%", alignItems: "center", userSelect: "none", touchAction: "none" }}
      min={min} max={max} step={1} value={[value]}
      onValueChange={([v]) => onChange(v)}
    >
      <SliderPrimitive.Track style={{ position: "relative", height: 6, flexGrow: 1, borderRadius: 9999, background: "#e2e8f0", overflow: "hidden" }}>
        <SliderPrimitive.Range style={{ position: "absolute", height: "100%", borderRadius: 9999, background: C.accent }} />
      </SliderPrimitive.Track>
      <SliderPrimitive.Thumb style={{
        display: "block", width: 16, height: 16, borderRadius: "50%",
        background: "#ffffff", border: `1.5px solid ${C.borderLight}`,
        boxShadow: "0 1px 3px rgba(0,0,0,0.15)", cursor: "pointer", outline: "none",
      }} />
    </SliderPrimitive.Root>
  );
}

// ── small UI primitives ────────────────────────────────────────────────────

function SectionHeader({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: C.muted, marginBottom: 12 }}>
      {children}
    </div>
  );
}

function FieldLabel({ children, tooltip }: { children: React.ReactNode; tooltip?: string }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 6 }}>
      <span style={{ fontSize: 13, fontWeight: 500, color: C.text }}>{children}</span>
      {tooltip && (
        <span title={tooltip} style={{ cursor: "help", color: C.muted, fontSize: 12, lineHeight: 1 }}>ⓘ</span>
      )}
    </div>
  );
}

const selectStyle: React.CSSProperties = {
  width: "100%", background: C.bgSection, border: `1px solid ${C.borderLight}`,
  borderRadius: 6, color: C.text, fontSize: 13, padding: "6px 28px 6px 10px",
  cursor: "pointer", outline: "none", appearance: "none",
  backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%236b7280' stroke-width='2'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E")`,
  backgroundRepeat: "no-repeat", backgroundPosition: "right 8px center",
};

// ── main component ─────────────────────────────────────────────────────────

export interface SettingsSidebarProps {
  store: TransitionMatrixStore;
  valuesType: MatrixValueType;
  onValuesTypeChange: (v: MatrixValueType) => void;
  showValueType?: boolean;
  pathCols: string[];
  pathIdCol: string;
  onPathIdColChange: (col: string) => void;
  segmentLevels: Record<string, string[]>;
  diffSegment: string | null;
  diffValue1: string | null;
  diffValue2: string | null;
  onDiffChange: (seg: string | null, v1: string | null, v2: string | null) => void;
  isLoading?: boolean;
  onFitGraph?: () => void;
  extraFooter?: React.ReactNode;
  headerRight?: React.ReactNode;
  loadingText?: string;
  /** Step window controls (step sankey only). */
  stepWindow?: number;
  maxSteps?: number;
  onStepWindowChange?: (w: number) => void;
  theme?: "dark" | "light" | "auto";
  /** When true (static HTML export), hide all controls that require backend recalculation. */
  isStatic?: boolean;
}

export const SettingsSidebar = observer(function SettingsSidebar({
  store, valuesType, onValuesTypeChange,
  showValueType = true,
  pathCols, pathIdCol, onPathIdColChange,
  segmentLevels, diffSegment, diffValue1, diffValue2, onDiffChange,
  isLoading = false, onFitGraph,
  extraFooter, headerRight, loadingText = "Computing…",
  stepWindow, maxSteps, onStepWindowChange,
  isStatic = false,
}: SettingsSidebarProps) {
  const segmentCols = Object.keys(segmentLevels);
  const isDiff = store.matrixType === "differential";
  const isTime = isTimeValueType(valuesType);

  // Local (pending) diff state — not applied until the user clicks Apply
  const [localSeg,  setLocalSeg]  = React.useState<string>(diffSegment ?? "");
  const [localVal1, setLocalVal1] = React.useState<string>(diffValue1  ?? "");
  const [localVal2, setLocalVal2] = React.useState<string>(diffValue2  ?? "");

  // Keep local state in sync when parent resets diff externally (e.g. on logout)
  React.useEffect(() => {
    setLocalSeg(diffSegment   ?? "");
    setLocalVal1(diffValue1   ?? "");
    setLocalVal2(diffValue2   ?? "");
  }, [diffSegment, diffValue1, diffValue2]);

  const localLevels = localSeg ? (segmentLevels[localSeg] ?? []) : [];

  const handleLocalSegChange = (col: string) => {
    setLocalSeg(col);
    if (!col) {
      setLocalVal1(""); setLocalVal2("");
      onDiffChange(null, null, null); // "None" clears immediately — nothing to configure
      return;
    }
    const lvls = segmentLevels[col] ?? [];
    // Convert to string — option values are always strings in HTML selects,
    // DuckDB on the Python side handles type coercion back to the column dtype.
    setLocalVal1(lvls[0] != null ? String(lvls[0]) : "");
    setLocalVal2(lvls[1] != null ? String(lvls[1]) : "");
  };

  const canApply = localSeg !== "" && localVal1 !== "" && localVal2 !== "" && localVal1 !== localVal2;
  const isDirty  = localSeg !== (diffSegment ?? "") ||
                   localVal1 !== (diffValue1  ?? "") ||
                   localVal2 !== (diffValue2  ?? "");

  const handleApply = () => {
    if (canApply) onDiffChange(localSeg, localVal1, localVal2);
  };

  return (
    <div style={{
      width: SIDEBAR_WIDTH, minWidth: SIDEBAR_WIDTH, height: "100%",
      background: C.bg, borderLeft: `1px solid ${C.border}`,
      display: "flex", flexDirection: "column", overflow: "hidden",
    }}>
      {/* Header */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "12px 16px", borderBottom: `1px solid ${C.border}`,
      }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: C.text }}>Settings</span>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {isLoading && (
            <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: C.muted }}>
              <div style={{ width: 12, height: 12, border: `1.5px solid ${C.borderLight}`, borderTop: `1.5px solid ${C.accent}`, borderRadius: "50%", animation: "retentioneering-spin 0.8s linear infinite" }} />
              {loadingText}
            </div>
          )}
          {headerRight}
        </div>
      </div>

      {/* Scrollable content */}
      <div style={{ flex: 1, overflowY: "auto", padding: "16px" }}>

        {/* ── Values Settings ──────────────────────────────────── */}
        <SectionHeader>Values Settings</SectionHeader>

        {/* Value Type */}
        {showValueType && (
          <div style={{ marginBottom: 20 }}>
            <FieldLabel tooltip="How edge weights are calculated.">Edge Weight Type</FieldLabel>
            <select value={valuesType} onChange={(e) => onValuesTypeChange(e.target.value as MatrixValueType)} style={selectStyle} disabled={isLoading || isStatic}>
              {VALUE_OPTIONS.map(({ value, label, tooltip }) => (
                <option key={value} value={value} title={tooltip}>{label}</option>
              ))}
            </select>
          </div>
        )}

        {/* Path Column — shown only when schema has >1 path cols */}
        {pathCols.length > 1 && (
          <div style={{ marginBottom: 20 }}>
            <FieldLabel tooltip="Column used as the path identifier (e.g. user_id or session_id).">
              Path Column
            </FieldLabel>
            <select
              value={pathIdCol}
              onChange={(e) => onPathIdColChange(e.target.value)}
              style={selectStyle}
              disabled={isLoading || isStatic}
            >
              {pathCols.map((col) => (
                <option key={col} value={col}>{col}</option>
              ))}
            </select>
          </div>
        )}

        {/* Diff by Segment */}
        <div style={{ marginBottom: 24 }}>
          <FieldLabel tooltip="Compare two groups. Positive values (red edges) = more transitions in group 2. Negative (blue) = more in group 1.">
            Diff by Segment
          </FieldLabel>

          {/* Segment column selector — "None" applies immediately */}
          <select
            value={localSeg}
            onChange={(e) => handleLocalSegChange(e.target.value)}
            style={{ ...selectStyle, marginBottom: localSeg ? 8 : 0 }}
            disabled={isLoading || isStatic}
          >
            <option value="">— None</option>
            {segmentCols.map((col) => <option key={col} value={col}>{col}</option>)}
          </select>

          {localSeg && localLevels.length >= 2 && (
            <>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 5, flex: 1, minWidth: 0 }}>
                  <span title="Group 1 (blue)" style={{ color: "rgb(59, 130, 246)", fontSize: 13, flexShrink: 0, lineHeight: 1 }}>●</span>
                  <select
                    value={localVal1}
                    onChange={(e) => setLocalVal1(e.target.value)}
                    style={{ ...selectStyle, flex: 1, minWidth: 0, width: "auto" }}
                    disabled={isLoading || isStatic}
                  >
                    {localLevels.map((v) => <option key={String(v)} value={String(v)} disabled={String(v) === String(localVal2)}>{String(v)}</option>)}
                  </select>
                </div>
                <span style={{ color: C.muted, fontSize: 11, flexShrink: 0 }}>vs</span>
                <div style={{ display: "flex", alignItems: "center", gap: 5, flex: 1, minWidth: 0 }}>
                  <span title="Group 2 (red)" style={{ color: "rgb(239, 68, 68)", fontSize: 13, flexShrink: 0, lineHeight: 1 }}>●</span>
                  <select
                    value={localVal2}
                    onChange={(e) => setLocalVal2(e.target.value)}
                    style={{ ...selectStyle, flex: 1, minWidth: 0, width: "auto" }}
                    disabled={isLoading || isStatic}
                  >
                    {localLevels.map((v) => <option key={String(v)} value={String(v)} disabled={String(v) === String(localVal1)}>{String(v)}</option>)}
                    <option value={REST_VALUE}>{REST_LABEL}</option>
                  </select>
                </div>
              </div>

              {/* Apply button — visible when selection differs from active diff */}
              {isDirty && (
                <button
                  onClick={handleApply}
                  disabled={!canApply || isLoading || isStatic}
                  style={{
                    width: "100%", padding: "6px 0",
                    background: canApply && !isStatic ? C.accent : C.bgSection,
                    border: `1px solid ${canApply && !isStatic ? C.accent : C.borderLight}`,
                    borderRadius: 6, cursor: canApply && !isStatic ? "pointer" : "default",
                    color: canApply && !isStatic ? C.accentFg : C.muted,
                    fontSize: 12, fontWeight: 600,
                  }}
                >
                  Apply
                </button>
              )}
            </>
          )}

          {localSeg && localLevels.length < 2 && (
            <p style={{ fontSize: 11, color: C.muted, marginTop: 4 }}>Not enough values in this segment.</p>
          )}
        </div>

        <div style={{ height: 1, background: C.border, margin: "0 0 20px" }} />

        {/* ── Visibility Settings ──────────────────────────────── */}
        <SectionHeader>Visibility Settings</SectionHeader>

        {/* Step window — shown only for step sankey */}
        {maxSteps != null && stepWindow != null && onStepWindowChange && (
          <div style={{ marginBottom: 20 }}>
            <FieldLabel tooltip="Number of steps to display around each anchor event. Decreasing this hides outer columns without recomputing.">
              Step window
            </FieldLabel>
            <div style={{ fontSize: 12, color: C.muted, marginBottom: 8 }}>
              {stepWindow} of {maxSteps}
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <div style={{ flex: 1 }}>
                <SingleSlider min={1} max={maxSteps} value={stepWindow} onChange={onStepWindowChange} />
              </div>
              <input
                type="number"
                min={1}
                max={maxSteps}
                value={stepWindow}
                onChange={e => {
                  const v = Math.max(1, Math.min(maxSteps, parseInt(e.target.value) || 1));
                  onStepWindowChange(v);
                }}
                style={{
                  width: 52, border: `1px solid ${C.borderLight}`, borderRadius: 6,
                  padding: "5px 8px", fontSize: 13, textAlign: "center",
                  outline: "none", background: C.bg, color: C.text,
                  boxShadow: "inset 0 1px 2px rgba(0,0,0,0.04)",
                }}
              />
            </div>
          </div>
        )}

        {/* Event Count filter */}
        <div style={{ marginBottom: 20 }}>
          <FieldLabel tooltip="How many times each event appears in the dataset. Move the left handle to hide rare events, move the right handle to hide very frequent ones.">
            Event Count
          </FieldLabel>

          {store.hasData && (store.populationBounds.max !== store.populationBounds.min) ? (
            <RangeSlider
              min={store.populationBounds.min}
              max={store.populationBounds.max}
              value={[store.filters.population.min, store.filters.population.max]}
              onChange={([min, max]) => store.setPopulationRange(min, max)}
              formatValue={(v) => formatPopulation(v)}
              scale="log"
            />
          ) : (
            <div style={{ height: 20, background: C.bgSection, borderRadius: 4 }} />
          )}
        </div>

      </div>

      {extraFooter && (
        <div style={{ borderTop: `1px solid ${C.border}`, padding: "10px 16px", flexShrink: 0 }}>
          {extraFooter}
        </div>
      )}
    </div>
  );
});
