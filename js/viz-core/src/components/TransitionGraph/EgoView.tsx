import React from "react";
import { DiffBreakdownTooltip } from "./DiffBreakdownTooltip";

// Ego view: a modal "centered two-step sankey" for one event — incoming
// transitions on the left, outgoing on the right, ribbon thickness
// proportional to the edge weight. Rendered as plain SVG (not cytoscape:
// the same event may appear on both sides, which cytoscape's model
// forbids). Clicking a neighbor re-centers the view on it.

export interface EgoEdge {
  source: string;
  target: string;
  weight: number;
  isSelfLoop: boolean;
}

interface DiffBreakdown {
  group1Value: number | null;
  group2Value: number | null;
  diffValue: number | null;
}

interface EgoViewProps {
  node: string;
  edges: EgoEdge[];
  // Sparse raw transition counts ({source: {target: n}}). When present (and
  // not in diff mode) the sides show exact shares: incoming = share of the
  // transitions INTO the center (proba_in), outgoing = share of the
  // transitions OUT of it (proba_out) — independent of the edge weight
  // displayed on the graph. Without counts (diff mode, old exports) the
  // view falls back to the displayed edge weights.
  counts?: Record<string, Record<string, number>> | null;
  formatValue: (value: number) => string;
  isDark: boolean;
  isDifferential: boolean;
  // Diff mode only: per-group breakdown for a transition, and the segment
  // labels — the same data and copy the graph's own edge-hover tooltip
  // uses, so ego view ribbons explain a diff exactly like graph edges do.
  getDiffBreakdown?: (source: string, target: string) => DiffBreakdown;
  diffLabels?: {
    segmentName: string | null;
    value1Label: string;
    value2Label: string;
  };
  onNavigate: (node: string) => void;
  onClose: () => void;
}

const MAX_ROWS_PER_SIDE = 12;
// Minimum bar height + gap keep the label pitch ≥ the 12px font, so labels
// of weak edges never overlap (at the cost of slightly exaggerating them).
const ROW_MIN_HEIGHT = 9;
const ROW_GAP = 5;
const STACK_BUDGET = 340; // px budget for the taller side's bars
const LABEL_WIDTH = 220;
const BAR_WIDTH = 12;
const RIBBON_SPAN = 190;
const SVG_WIDTH = LABEL_WIDTH * 2 + BAR_WIDTH * 3 + RIBBON_SPAN * 2;

const RED = "rgb(239, 68, 68)";
const BLUE = "rgb(59, 130, 246)";

interface Row {
  other: string;
  weight: number;
  isSelfLoop: boolean;
  count: number | null; // raw transitions behind a share value, if known
  height: number; // ribbon height at the label end
  y: number; // label-end bar top
  attachHeight: number; // ribbon height at the center-bar end (compressed)
  attachY: number; // ribbon attachment top on the center bar
}

// Bar/ribbon heights from weights: proportional, clamped to a minimum so
// weak edges stay clickable.
const buildRows = (
  entries: Array<{
    other: string;
    weight: number;
    isSelfLoop: boolean;
    count: number | null;
  }>,
  pxPerUnit: number,
): Row[] =>
  entries.map((e) => ({
    ...e,
    height: Math.max(ROW_MIN_HEIGHT, Math.abs(e.weight) * pxPerUnit),
    y: 0,
    attachHeight: 0,
    attachY: 0,
  }));

// Label-end stacking: rows keep their natural height, with a gap between
// them for readability.
const stackLabelSide = (rows: Row[], centerline: number, gap: number) => {
  const total =
    rows.reduce((sum, r) => sum + r.height, 0) +
    Math.max(0, rows.length - 1) * gap;
  let y = centerline - total / 2;
  rows.forEach((r) => {
    r.y = y;
    y += r.height + gap;
  });
};

// Center-bar-end stacking: rows are compressed (attachHeight, set by the
// caller) to exactly fill centerHeight, touching with no gaps — so the
// topmost ribbon's attach edge always meets the bar's top edge.
const stackAttachSide = (rows: Row[], centerline: number) => {
  const total = rows.reduce((sum, r) => sum + r.attachHeight, 0);
  let y = centerline - total / 2;
  rows.forEach((r) => {
    r.attachY = y;
    y += r.attachHeight;
  });
};

const ribbonPath = (
  x0: number,
  y0: number,
  h0: number,
  x1: number,
  y1: number,
  h1: number,
): string => {
  const mid = (x0 + x1) / 2;
  return [
    `M ${x0} ${y0}`,
    `C ${mid} ${y0} ${mid} ${y1} ${x1} ${y1}`,
    `L ${x1} ${y1 + h1}`,
    `C ${mid} ${y1 + h1} ${mid} ${y0 + h0} ${x0} ${y0 + h0}`,
    "Z",
  ].join(" ");
};

const truncate = (name: string, max = 26): string =>
  name.length > max ? name.slice(0, max - 1) + "…" : name;

export const EgoView: React.FC<EgoViewProps> = ({
  node,
  edges,
  counts,
  formatValue,
  isDark,
  isDifferential,
  getDiffBreakdown,
  diffLabels,
  onNavigate,
  onClose,
}) => {
  const [hovered, setHovered] = React.useState<string | null>(null);
  const [diffTooltip, setDiffTooltip] = React.useState<{
    x: number;
    y: number;
    source: string;
    target: string;
  } | null>(null);

  React.useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    };
    window.addEventListener("keydown", onKeyDown, true);
    return () => window.removeEventListener("keydown", onKeyDown, true);
  }, [onClose]);

  const sharesMode = counts != null && !isDifferential;

  const {
    incoming,
    outgoing,
    moreIn,
    moreOut,
    centerHeight,
    svgHeight,
    totalIn,
    totalOut,
  } = React.useMemo(() => {
    const bySize = (a: { weight: number }, b: { weight: number }) =>
      Math.abs(b.weight) - Math.abs(a.weight);
    let inAll: Array<{
      other: string;
      weight: number;
      isSelfLoop: boolean;
      count: number | null;
    }>;
    let outAll: typeof inAll;
    let totalIn = 0;
    let totalOut = 0;
    if (sharesMode && counts) {
      const inRaw: Array<{ other: string; cnt: number }> = [];
      Object.entries(counts).forEach(([src, row]) => {
        const cnt = row[node];
        if (cnt) inRaw.push({ other: src, cnt });
      });
      const outRaw = Object.entries(counts[node] ?? {})
        .filter(([, cnt]) => cnt > 0)
        .map(([dst, cnt]) => ({ other: dst, cnt }));
      totalIn = inRaw.reduce((s, e) => s + e.cnt, 0);
      totalOut = outRaw.reduce((s, e) => s + e.cnt, 0);
      inAll = inRaw
        .map((e) => ({
          other: e.other,
          weight: totalIn ? e.cnt / totalIn : 0,
          isSelfLoop: e.other === node,
          count: e.cnt,
        }))
        .sort(bySize);
      outAll = outRaw
        .map((e) => ({
          other: e.other,
          weight: totalOut ? e.cnt / totalOut : 0,
          isSelfLoop: e.other === node,
          count: e.cnt,
        }))
        .sort(bySize);
    } else {
      inAll = edges
        .filter((e) => e.target === node)
        .map((e) => ({
          other: e.source,
          weight: e.weight,
          isSelfLoop: e.isSelfLoop,
          count: null,
        }))
        .sort(bySize);
      outAll = edges
        .filter((e) => e.source === node)
        .map((e) => ({
          other: e.target,
          weight: e.weight,
          isSelfLoop: e.isSelfLoop,
          count: null,
        }))
        .sort(bySize);
    }
    const inTop = inAll.slice(0, MAX_ROWS_PER_SIDE);
    const outTop = outAll.slice(0, MAX_ROWS_PER_SIDE);

    const sumIn = inTop.reduce((s, e) => s + Math.abs(e.weight), 0);
    const sumOut = outTop.reduce((s, e) => s + Math.abs(e.weight), 0);
    const maxSum = Math.max(sumIn, sumOut);
    const pxPerUnit = maxSum > 0 ? STACK_BUDGET / maxSum : 0;

    const incoming = buildRows(inTop, pxPerUnit);
    const outgoing = buildRows(outTop, pxPerUnit);
    const tallest = Math.max(
      incoming.reduce((s, r) => s + r.height, 0) +
        Math.max(0, incoming.length - 1) * ROW_GAP,
      outgoing.reduce((s, r) => s + r.height, 0) +
        Math.max(0, outgoing.length - 1) * ROW_GAP,
      48,
    );
    const svgHeight = tallest + 72;
    const centerline = svgHeight / 2 + 12;
    stackLabelSide(incoming, centerline, ROW_GAP);
    stackLabelSide(outgoing, centerline, ROW_GAP);

    // Center bar: half the height it would take to naturally fit the
    // taller side's ribbons — a compact anchor, not a full stack.
    const inHeightSum = incoming.reduce((s, r) => s + r.height, 0);
    const outHeightSum = outgoing.reduce((s, r) => s + r.height, 0);
    const centerHeight = Math.max(inHeightSum, outHeightSum, 20) / 2;

    // Each side's ribbons are compressed uniformly so they exactly fill
    // centerHeight at the bar end, touching with no gaps — the topmost
    // ribbon's attach edge always meets the bar's top edge exactly.
    const inScale = inHeightSum > 0 ? centerHeight / inHeightSum : 0;
    const outScale = outHeightSum > 0 ? centerHeight / outHeightSum : 0;
    incoming.forEach((r) => {
      r.attachHeight = r.height * inScale;
    });
    outgoing.forEach((r) => {
      r.attachHeight = r.height * outScale;
    });
    stackAttachSide(incoming, centerline);
    stackAttachSide(outgoing, centerline);

    return {
      incoming,
      outgoing,
      moreIn: inAll.length - inTop.length,
      moreOut: outAll.length - outTop.length,
      centerHeight,
      svgHeight,
      totalIn,
      totalOut,
    };
  }, [edges, counts, sharesMode, node]);

  const textColor = isDark ? "#f3f4f6" : "#111827";
  const mutedColor = isDark ? "#9ca3af" : "#6b7280";
  const barColor = isDark ? "#6b7280" : "#9ca3af";
  // Amber marks the center event and, on hover, the neighbor being pointed
  // at — more visible against the modal's white/dark background than the
  // brand yellow.
  const centerColor = isDark ? "#fbbf24" : "#f59e0b";
  const ribbonFill = (weight: number, key: string): string => {
    const base = isDifferential
      ? weight > 0
        ? RED
        : BLUE
      : isDark
        ? "#9ca3af"
        : "#6b7280";
    const opacity = hovered === null ? 0.4 : hovered === key ? 0.75 : 0.15;
    return base.startsWith("rgb")
      ? base.replace("rgb", "rgba").replace(")", `, ${opacity})`)
      : base +
          Math.round(opacity * 255)
            .toString(16)
            .padStart(2, "0");
  };

  const leftBarX = LABEL_WIDTH;
  const centerX = LABEL_WIDTH + BAR_WIDTH + RIBBON_SPAN;
  const rightBarX = centerX + BAR_WIDTH + RIBBON_SPAN;
  const centerline = svgHeight / 2 + 12;
  const centerTop = centerline - centerHeight / 2;

  const renderSide = (rows: Row[], side: "in" | "out", total: number) =>
    rows.map((row, i) => {
      const key = `${side}:${row.other}:${i}`;
      const barX = side === "in" ? leftBarX : rightBarX;
      const labelX = side === "in" ? leftBarX - 8 : rightBarX + BAR_WIDTH + 8;
      const valueText = sharesMode
        ? `${(row.weight * 100).toFixed(1)}%`
        : isDifferential
          ? (row.weight > 0 ? "+" : "") + formatValue(row.weight)
          : formatValue(row.weight);
      const ribbon =
        side === "in"
          ? ribbonPath(
              leftBarX + BAR_WIDTH,
              row.y,
              row.height,
              centerX,
              row.attachY,
              row.attachHeight,
            )
          : ribbonPath(
              centerX + BAR_WIDTH,
              row.attachY,
              row.attachHeight,
              rightBarX,
              row.y,
              row.height,
            );
      const [source, target] =
        side === "in" ? [row.other, node] : [node, row.other];
      return (
        <g
          key={key}
          onClick={() => onNavigate(row.other)}
          onMouseEnter={() => setHovered(key)}
          onMouseMove={(e) => {
            if (isDifferential && getDiffBreakdown) {
              setDiffTooltip({ x: e.clientX, y: e.clientY, source, target });
            }
          }}
          onMouseLeave={() => {
            setHovered(null);
            setDiffTooltip(null);
          }}
          style={{ cursor: "pointer" }}
        >
          <path d={ribbon} fill={ribbonFill(row.weight, key)} />
          <rect
            x={barX}
            y={row.y}
            width={BAR_WIDTH}
            height={row.height}
            rx={2}
            fill={hovered === key ? centerColor : barColor}
          />
          <text
            x={labelX}
            y={row.y + row.height / 2}
            dominantBaseline="central"
            textAnchor={side === "in" ? "end" : "start"}
            fontSize={12}
            fill={hovered === key ? centerColor : textColor}
          >
            {(row.isSelfLoop ? "↻ " : "") + truncate(row.other)}
            <tspan fill={mutedColor}>{`  ${valueText}`}</tspan>
          </text>
          {/* Diff mode shows the rich DiffBreakdownTooltip instead (below);
              elsewhere a native tooltip is enough. */}
          {!isDifferential && (
            <title>{`${side === "in" ? `${row.other} → ${node}` : `${node} → ${row.other}`} ${valueText}${
              sharesMode ? ` (${row.count} transitions out of ${total})` : ""
            } – click to re-center`}</title>
          )}
        </g>
      );
    });

  const emptySide = (side: "in" | "out") => (
    <text
      x={side === "in" ? leftBarX - 8 : rightBarX + BAR_WIDTH + 8}
      y={centerline}
      dominantBaseline="central"
      textAnchor={side === "in" ? "end" : "start"}
      fontSize={12}
      fill={mutedColor}
    >
      no {side === "in" ? "incoming" : "outgoing"} transitions
    </text>
  );

  const moreNote = (count: number, side: "in" | "out") =>
    count > 0 ? (
      <text
        x={side === "in" ? leftBarX - 8 : rightBarX + BAR_WIDTH + 8}
        y={svgHeight - 8}
        textAnchor={side === "in" ? "end" : "start"}
        fontSize={11}
        fill={mutedColor}
      >
        +{count} weaker not shown
      </text>
    ) : null;

  return (
    <div
      onClick={onClose}
      style={{
        position: "absolute",
        inset: 0,
        zIndex: 40,
        background: isDark ? "rgba(0,0,0,0.55)" : "rgba(17,24,39,0.35)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: isDark ? "#1f2937" : "#ffffff",
          border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
          borderRadius: 8,
          padding: "12px 16px 8px",
          maxWidth: "94%",
          maxHeight: "92%",
          overflow: "auto",
          boxShadow: "0 10px 30px rgba(0,0,0,0.25)",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "baseline",
            gap: 12,
            marginBottom: 4,
          }}
        >
          <span style={{ fontSize: 14, fontWeight: 600, color: textColor }}>
            Ego view
          </span>
          <span
            title="Incoming and outgoing edges"
            style={{ cursor: "help", color: mutedColor, fontSize: 12 }}
          >
            ⓘ
          </span>
          <button
            onClick={onClose}
            aria-label="Close ego view"
            style={{
              marginLeft: "auto",
              border: "none",
              background: "transparent",
              color: mutedColor,
              cursor: "pointer",
              fontSize: 16,
              lineHeight: 1,
              padding: 4,
            }}
          >
            ×
          </button>
        </div>
        <svg
          width={SVG_WIDTH}
          height={svgHeight}
          viewBox={`0 0 ${SVG_WIDTH} ${svgHeight}`}
          style={{ display: "block", maxWidth: "100%" }}
        >
          {sharesMode && (
            <>
              <text
                x={leftBarX}
                y={16}
                textAnchor="end"
                fontSize={11}
                fill={mutedColor}
              >
                % of incoming transitions (proba_in)
              </text>
              <text
                x={rightBarX + BAR_WIDTH}
                y={16}
                textAnchor="start"
                fontSize={11}
                fill={mutedColor}
              >
                % of outgoing transitions (proba_out)
              </text>
            </>
          )}
          {renderSide(incoming, "in", totalIn)}
          {renderSide(outgoing, "out", totalOut)}
          {incoming.length === 0 && emptySide("in")}
          {outgoing.length === 0 && emptySide("out")}
          <rect
            x={centerX}
            y={centerTop}
            width={BAR_WIDTH}
            height={centerHeight}
            rx={2}
            fill={centerColor}
          />
          <text
            x={centerX + BAR_WIDTH / 2}
            y={centerTop - 10}
            textAnchor="middle"
            fontSize={13}
            fontWeight={600}
            fill={textColor}
          >
            {node}
          </text>
          {moreNote(moreIn, "in")}
          {moreNote(moreOut, "out")}
        </svg>
      </div>
      {diffTooltip &&
        getDiffBreakdown &&
        (() => {
          const breakdown = getDiffBreakdown(
            diffTooltip.source,
            diffTooltip.target,
          );
          return (
            <div
              style={{
                position: "fixed",
                left: diffTooltip.x + 12,
                top: diffTooltip.y + 12,
                zIndex: 50,
                pointerEvents: "none",
              }}
            >
              <DiffBreakdownTooltip
                title={
                  <span
                    style={{ display: "flex", alignItems: "center", gap: 8 }}
                  >
                    <span>{diffTooltip.source}</span>
                    <span style={{ color: mutedColor }}>→</span>
                    <span>{diffTooltip.target}</span>
                  </span>
                }
                segmentName={diffLabels?.segmentName ?? null}
                value1Label={diffLabels?.value1Label ?? "group 1"}
                value2Label={diffLabels?.value2Label ?? "group 2"}
                group1Value={breakdown.group1Value}
                group2Value={breakdown.group2Value}
                diffValue={breakdown.diffValue}
                isDark={isDark}
              />
            </div>
          );
        })()}
    </div>
  );
};
