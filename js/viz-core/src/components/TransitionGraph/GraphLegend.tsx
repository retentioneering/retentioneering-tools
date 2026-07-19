import * as React from "react";
import type { MatrixValueType } from "../../utils/value-types";

export type GraphLegendMode = "normal" | "diff" | "focus";

interface GraphLegendProps {
  isDark: boolean;
  mode: GraphLegendMode;
  valuesType: MatrixValueType;
  coverage: { shown: number; total: number; weightShare: number };
  diffLabel1?: string | null;
  diffLabel2?: string | null;
  widgetId: string;
  /** Initial state when the user hasn't toggled the legend yet (persisted
   *  toggles always win). Static HTML exports start collapsed. */
  defaultCollapsed?: boolean;
  /** Direction currently shown by the node focus (mode === "focus"). */
  focusDirection?: "out" | "in";
}

const VALUE_TYPE_LABELS: Record<MatrixValueType, string> = {
  unique_paths: "unique paths",
  count: "transition count",
  share_of_total: "share of total",
  avg_per_path: "avg per path",
  proba_out: "P(next | source)",
  proba_in: "P(prev | target)",
  time_median: "median time",
  time_q95: "95th pct time",
};

function Swatch({ color, kind }: { color: string; kind: "line" | "dot" }) {
  if (kind === "dot") {
    return (
      <span
        style={{
          display: "inline-block",
          width: 10,
          height: 10,
          borderRadius: "50%",
          background: color,
          flexShrink: 0,
        }}
      />
    );
  }
  return (
    <span
      style={{
        display: "inline-block",
        width: 18,
        height: 3,
        borderRadius: 2,
        background: color,
        flexShrink: 0,
      }}
    />
  );
}

/**
 * Contextual legend + coverage indicator, bottom-left of the graph canvas.
 * Content adapts to the current mode (normal / differential / focus);
 * the coverage line keeps the user honest about how much of the graph the
 * current edge filter actually shows.
 */
export function GraphLegend({
  isDark,
  mode,
  valuesType,
  coverage,
  diffLabel1,
  diffLabel2,
  widgetId,
  defaultCollapsed = false,
  focusDirection = "out",
}: GraphLegendProps) {
  const storageKey = `transition-graph-legend:${widgetId}`;
  const [collapsed, setCollapsed] = React.useState<boolean>(() => {
    try {
      const stored = window.localStorage.getItem(storageKey);
      if (stored === "collapsed") return true;
      if (stored === "open") return false;
      return defaultCollapsed;
    } catch {
      return defaultCollapsed;
    }
  });
  const toggleCollapsed = React.useCallback(() => {
    setCollapsed((current) => {
      const next = !current;
      try {
        window.localStorage.setItem(storageKey, next ? "collapsed" : "open");
      } catch {
        /* localStorage unavailable */
      }
      return next;
    });
  }, [storageKey]);

  const textColor = isDark ? "#d1d5db" : "#4b5563";
  const mutedColor = isDark ? "#9ca3af" : "#6b7280";
  const panelStyle: React.CSSProperties = {
    display: "flex",
    flexDirection: "column",
    gap: 4,
    padding: "6px 10px",
    borderRadius: 6,
    border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
    background: isDark ? "rgba(31,41,55,0.92)" : "rgba(255,255,255,0.92)",
    fontSize: 11,
    lineHeight: "16px",
    color: textColor,
    maxWidth: 280,
  };
  const rowStyle: React.CSSProperties = {
    display: "flex",
    alignItems: "center",
    gap: 6,
  };

  const edgeColor = isDark ? "rgb(156,163,175)" : "rgb(75,85,99)";
  const nodeColor = "rgb(250,204,21)";

  if (collapsed) {
    return (
      <button
        onClick={toggleCollapsed}
        title="Show legend"
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          width: 24,
          height: 24,
          borderRadius: "50%",
          border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
          background: isDark ? "rgba(31,41,55,0.92)" : "rgba(255,255,255,0.92)",
          color: mutedColor,
          cursor: "pointer",
          fontSize: 12,
          fontStyle: "italic",
          fontFamily: "Georgia, serif",
          padding: 0,
        }}
      >
        i
      </button>
    );
  }

  const coverageLine = (
    <div style={{ ...rowStyle, color: mutedColor }}>
      <span>
        edges: {coverage.shown} / {coverage.total}
        {coverage.total > 0 && coverage.shown < coverage.total
          ? ` (${Math.round(coverage.weightShare * 100)}% of weight)`
          : ""}
      </span>
    </div>
  );

  return (
    <div style={panelStyle}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
        <span style={{ fontWeight: 600, color: mutedColor, textTransform: "uppercase", fontSize: 10, letterSpacing: 0.4 }}>
          Legend
        </span>
        <button
          onClick={toggleCollapsed}
          title="Hide legend"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            width: 14,
            height: 14,
            background: "transparent",
            border: "none",
            cursor: "pointer",
            color: mutedColor,
            padding: 0,
          }}
        >
          <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M18 6 6 18M6 6l12 12" />
          </svg>
        </button>
      </div>

      {mode === "focus" ? (
        <>
          <div style={rowStyle}>
            <Swatch color={edgeColor} kind="line" />
            <span>
              {focusDirection === "in"
                ? "showing incoming transitions"
                : "showing outgoing transitions"}
            </span>
          </div>
          <div style={{ ...rowStyle, color: mutedColor }}>
            <span>
              {focusDirection === "in"
                ? "click node = outgoing"
                : "double-click node = incoming"}
            </span>
          </div>
        </>
      ) : mode === "diff" ? (
        <>
          <div style={rowStyle}>
            <Swatch color="rgb(239,68,68)" kind="line" />
            <span>more in {diffLabel1 ?? "group 1"}</span>
          </div>
          <div style={rowStyle}>
            <Swatch color="rgb(59,130,246)" kind="line" />
            <span>more in {diffLabel2 ?? "group 2"}</span>
          </div>
        </>
      ) : (
        <>
          <div style={rowStyle}>
            <Swatch color={nodeColor} kind="dot" />
            <span>node size = event count</span>
          </div>
          <div style={rowStyle}>
            <Swatch color={edgeColor} kind="line" />
            <span>edge width = {VALUE_TYPE_LABELS[valuesType] ?? valuesType}</span>
          </div>
        </>
      )}

      {coverageLine}

      <div style={{ ...rowStyle, color: mutedColor, flexWrap: "wrap", rowGap: 0 }}>
        <span>click node = outgoing · 2×click = incoming · click edge = inspect</span>
        <span>⌘click = add to path (repeats ok) · ⌘2×click = undo last</span>
      </div>
    </div>
  );
}
