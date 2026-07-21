import * as React from "react";
import cytoscape from "cytoscape";
import type { Core, ElementDefinition } from "cytoscape";
// @ts-expect-error
import fcose from "cytoscape-fcose";
import { observer } from "mobx-react-lite";
import { TransitionMatrixStore } from "../../stores/TransitionMatrixStore";
import { WidgetHost } from "../../WidgetHost";
import { useEdgeColors } from "./hooks/useEdgeColors";
import { useNodeColors } from "./hooks/useNodeColors";
import { useNodePositions } from "./hooks/useNodePositions";
import { useGraphLayout } from "./hooks/useGraphLayout";
import { DEFAULT_VALUE_TYPE, isTimeValueType, isProbabilityValueType, type MatrixValueType } from "../../utils/value-types";
import { formatNumber } from "../../utils/format-number";
import { formatTime } from "../../utils/format-time";
import { resolveDiffLabels } from "../../utils/diff-tooltip";
import { withSeededRandom } from "../../utils/seeded-random";
import { RangeSlider } from "./RangeSlider";
import { SearchBar } from "./SearchBar";
import { DiffBreakdownTooltip } from "./DiffBreakdownTooltip";
import { GraphLegend } from "./GraphLegend";
import { EgoView } from "./EgoView";
import {
  type GraphView,
  parseGraphView,
  encodeGraphView,
} from "./graph-view";

// Register the fcose layout
if (typeof cytoscape !== "undefined") {
  cytoscape.use(fcose);
}

type TooltipState = {
  type: "edge";
  id: string;
  from: string;
  to: string;
  forwardWeight: number;
  group1Value: number | null;
  group2Value: number | null;
  diffValue: number | null;
  backwardWeight?: number;
  isBidirectional: boolean;
  position: { x: number; y: number };
} | {
  type: "node";
  eventId: string;
  group1Value: number | null;
  group2Value: number | null;
  diffValue: number | null;
  position: { x: number; y: number };
} | null;

type ColorPickerState =
  | {
      kind: "edge";
      x: number;
      y: number;
      source: string;
      target: string;
    }
  | {
      kind: "node";
      x: number;
      y: number;
      nodeId: string;
    };

const NODE_BASE_MIN_SIZE = 24;
const NODE_BASE_MAX_SIZE = 40;
const NODE_FOCUS_DIMMED_SIZE = 18;
const NODE_FOCUS_CONNECTED_MIN_SIZE = 20;
const NODE_FOCUS_CONNECTED_MAX_SIZE = 42;
const NODE_FOCUS_ACTIVE_SIZE = 48;
const DEFAULT_NODE_COLOR = "250, 204, 21";
const FOCUS_LABEL_MIN_VISIBLE_RATIO = 0.08;
const BASE_ARROW_MIN_VISIBLE_RATIO = 0.14;
const FOCUS_ARROW_MIN_VISIBLE_RATIO = 0.22;
const DEFAULT_LOOP_DIRECTION = "-90deg";
// Adaptive edge labels: label everything on small graphs, a bounded share of
// the visible (non-filtered) edges on large ones.
const EDGE_LABELS_ALL_THRESHOLD = 12;
const EDGE_LABELS_MIN = 10;
const EDGE_LABELS_MAX = 25;
const EDGE_LABELS_SHARE = 0.15;
// Deterministic seed for the fcose layout runs (see withSeededRandom).
const LAYOUT_RANDOM_SEED = 0x9e3779b9;
// Single padding for every fit-to-canvas call (initial render, Reset
// layout, the Fit button, the sidebar Fit action) — they must all produce
// the identical viewport.
const FIT_PADDING = 30;
// Per-node top-k edge filter (the default "auto" mode).
const DEFAULT_TOP_K = 3;
const TOP_K_MIN = 1;
const TOP_K_MAX = 10;
// Hard cap on created cytoscape edge elements. Beyond it the ultra-thin tail
// is not even instantiated (a 200-event proba graph approaches n² edges);
// the legend's coverage indicator reports what is hidden.
const EDGE_BUILD_CAP = 5000;

/** Edge filter state: per-node top-k ("auto") or a manual weight range. */
export type EdgeFilterSpec =
  | { mode: "topk"; k: number }
  | { mode: "range"; range: [number, number] };

const edgeKey = (source: string, target: string) => `${source}|${target}`;

// ── Path stats (route badge) ─────────────────────────────────────────────────

type PathStatsResult = {
  n_paths: number;
  unique_paths: number;
  unique_paths_share: number;
  occurrences: number;
  avg_per_path: number;
  time_median: number | null;
  time_q95: number | null;
  proba: number;
};

type PathMetric =
  | "unique_paths"
  | "count"
  | "avg_per_path"
  | "time_median"
  | "time_q95"
  | "proba_out";

const PATH_METRIC_LABELS: Record<PathMetric, string> = {
  unique_paths: "unique paths",
  count: "traversals",
  avg_per_path: "avg per path",
  time_median: "median time",
  time_q95: "p95 time",
  proba_out: "P(route)",
};

// Default badge metric follows the edge weight shown on the graph; types
// with no honest route generalization fall back to unique paths.
const defaultPathMetric = (valuesType: MatrixValueType): PathMetric => {
  switch (valuesType) {
    case "count":
      return "count";
    case "avg_per_path":
      return "avg_per_path";
    case "time_median":
      return "time_median";
    case "time_q95":
      return "time_q95";
    case "proba_out":
      return "proba_out";
    default: // unique_paths, proba_in, share_of_total
      return "unique_paths";
  }
};

/**
 * Per-node top-k filter rule: keep an edge if it is among the k strongest
 * outgoing edges of its source (self-loops count as outgoing) OR it is the
 * single strongest incoming edge of its target — so no visible node ends up
 * with zero edges. `edges` must be sorted by |weight| descending.
 */
function computeTopKKeptSet(
  edges: Array<{
    source: string;
    target: string;
    weight: number;
    isSelfLoop: boolean;
  }>,
  k: number,
): Set<string> {
  const outTaken = new Map<string, number>();
  const seenIncoming = new Set<string>();
  const kept = new Set<string>();
  edges.forEach((edge) => {
    const taken = outTaken.get(edge.source) ?? 0;
    if (taken < k) {
      outTaken.set(edge.source, taken + 1);
      kept.add(edgeKey(edge.source, edge.target));
    }
    // First incoming edge per target in weight-sorted order = its strongest.
    if (!edge.isSelfLoop && !seenIncoming.has(edge.target)) {
      seenIncoming.add(edge.target);
      kept.add(edgeKey(edge.source, edge.target));
    }
  });
  return kept;
}

// Palette colors (RGB strings)
const PALETTE_COLORS = [
  { name: "Yellow", value: "250, 204, 21" }, // Platform Yellow
  { name: "Orange", value: "249, 115, 22" },
  { name: "Red", value: "239, 68, 68" },
  { name: "Pink", value: "236, 72, 153" },
  { name: "Purple", value: "168, 85, 247" },
  { name: "Indigo", value: "99, 102, 241" },
  { name: "Blue", value: "59, 130, 246" },
  { name: "Cyan", value: "6, 182, 212" },
  { name: "Teal", value: "20, 184, 166" },
  { name: "Green", value: "34, 197, 94" },
];

const NODE_COLOR_PATTERN = /^\s*\d{1,3}\s*,\s*\d{1,3}\s*,\s*\d{1,3}\s*$/;
const nodeImageCache = new Map<string, string>();

const normalizeRgbColor = (value: string | undefined | null): string => {
  if (!value || !NODE_COLOR_PATTERN.test(value)) return DEFAULT_NODE_COLOR;

  const channels = value.split(",").map((channel) => {
    const parsed = Number(channel.trim());
    if (!Number.isFinite(parsed)) return 0;
    return Math.max(0, Math.min(255, Math.round(parsed)));
  });

  return `${channels[0]}, ${channels[1]}, ${channels[2]}`;
};

const getNodeImageDataUri = (nodeColor: string, innerRadius = 27): string => {
  const normalizedColor = normalizeRgbColor(nodeColor);
  const r = Math.round(innerRadius);
  const cacheKey = `${normalizedColor}:${r}`;
  const cached = nodeImageCache.get(cacheKey);
  if (cached) return cached;

  const innerCircle = r > 0 ? `\n  <circle cx="50" cy="50" r="${r}" fill="rgb(0, 0, 0)" />` : "";
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100">
  <circle cx="50" cy="50" r="48" fill="rgb(0, 0, 0)" />
  <circle cx="50" cy="50" r="36" fill="rgb(${normalizedColor})" />${innerCircle}
</svg>`;
  const encoded = `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
  nodeImageCache.set(cacheKey, encoded);
  return encoded;
};

const clamp01 = (value: number) => Math.min(1, Math.max(0, value));

/** Interpolate between two RGB triplets and return an "r, g, b" string. */
const lerpRgb = (
  from: [number, number, number],
  to: [number, number, number],
  t: number,
): string => {
  const r = Math.round(from[0] + (to[0] - from[0]) * clamp01(t));
  const g = Math.round(from[1] + (to[1] - from[1]) * clamp01(t));
  const b = Math.round(from[2] + (to[2] - from[2]) * clamp01(t));
  return `${r}, ${g}, ${b}`;
};
const NODE_DIFF_NEUTRAL: [number, number, number] = [229, 231, 235]; // gray-200
const NODE_DIFF_RED: [number, number, number]     = [239, 68, 68];   // red-500
const NODE_DIFF_BLUE: [number, number, number]    = [59, 130, 246];  // blue-500
const applyAlphaColor = (rgb: string, alpha: number) =>
  `rgba(${rgb}, ${clamp01(alpha)})`;
const normalizeRange = (
  value: number,
  min: number,
  max: number,
  fallback = 0.5,
): number => {
  if (!Number.isFinite(value)) return fallback;
  if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) {
    return fallback;
  }
  return clamp01((value - min) / (max - min));
};

function createInitialPosition(eventId: string): StoredPosition {
  // Use a stable hash-based position instead of index-based position
  // This ensures the same event always gets the same initial position
  // regardless of how many other events are visible
  let hash = 0;
  for (let i = 0; i < eventId.length; i++) {
    hash = (hash << 5) - hash + eventId.charCodeAt(i);
    hash = hash & hash; // Convert to 32bit integer
  }

  // Use hash for angle distribution
  const angle = (Math.abs(hash) % 360) * (Math.PI / 180);

  // Use a second hash for radius variation (300-600 range)
  let hash2 = hash;
  for (let i = 0; i < eventId.length; i++) {
    hash2 = (hash2 << 3) + hash2 + eventId.charCodeAt(eventId.length - 1 - i);
    hash2 = hash2 & hash2;
  }
  const radius = 300 + (Math.abs(hash2) % 300);

  return {
    x: Math.cos(angle) * radius,
    y: Math.sin(angle) * radius,
  };
}

function calculateSelfLoopDirection(node: cytoscape.NodeSingular): string {
  const nodePos = node.position();

  // Use only visible non-loop edges as constraints for the loop orientation.
  const connectedEdges = node
    .connectedEdges()
    .filter(
      (edge: cytoscape.EdgeSingular) =>
        edge.source().id() !== edge.target().id() && !edge.hasClass("filtered"),
    );

  if (connectedEdges.length === 0) {
    return DEFAULT_LOOP_DIRECTION;
  }

  const angles: number[] = [];
  connectedEdges.forEach((edge: cytoscape.EdgeSingular) => {
    const otherNode =
      edge.source().id() === node.id() ? edge.target() : edge.source();
    const otherPos = otherNode.position();
    const dx = otherPos.x - nodePos.x;
    const dy = otherPos.y - nodePos.y;
    // Degrees from 12 o'clock, clockwise.
    const angle = Math.atan2(dx, -dy) * (180 / Math.PI);
    angles.push(angle);
  });

  angles.sort((a, b) => a - b);

  let maxGap = 0;
  let bestAngle = -90;

  for (let i = 0; i < angles.length; i++) {
    const current = angles[i];
    const next = angles[(i + 1) % angles.length];
    let gap = next - current;

    if (i === angles.length - 1) {
      gap = 360 + next - current;
    }

    if (gap > maxGap) {
      maxGap = gap;
      bestAngle = current + gap / 2;
      if (bestAngle > 180) bestAngle -= 360;
    }
  }

  return `${bestAngle}deg`;
}

function fallbackCopyText(text: string): void {
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  try {
    document.execCommand("copy");
  } catch {
    /* clipboard unavailable */
  }
  textarea.remove();
}

function copyTextToClipboard(text: string): void {
  if (navigator.clipboard?.writeText) {
    navigator.clipboard.writeText(text).catch(() => fallbackCopyText(text));
  } else {
    fallbackCopyText(text);
  }
}

function recalculateSelfLoopDirections(cy: Core, nodeIds?: Set<string>): void {
  const selfLoops = cy.edges("edge[source = target]");
  if (selfLoops.length === 0) return;

  cy.batch(() => {
    selfLoops.forEach((edge: cytoscape.EdgeSingular) => {
      const ownerNodeId = edge.source().id();
      if (nodeIds && !nodeIds.has(ownerNodeId)) return;
      edge.data("loopDirection", calculateSelfLoopDirection(edge.source()));
    });
  });
}

export interface StoredPosition { x: number; y: number; }
export interface StoredViewport { zoom: number; pan: { x: number; y: number }; }

export interface TransitionGraphProps {
  store: TransitionMatrixStore;
  /** WidgetHost the graph uses for the (optional) backend "graph_layout" auto-layout compute. */
  host: WidgetHost | null;
  widgetId?: string;
  valuesType?: MatrixValueType;
  onValuesTypeChange?: (v: MatrixValueType) => void;
  diffSegment?: string | null;
  diffValue1?: string | null;
  diffValue2?: string | null;
  theme?: "dark" | "light" | "auto";
  isFullscreen?: boolean;
  onFullscreenToggle?: () => void;
  eventCounts?: Record<string, number>;
  eventCountsG1?: Record<string, number>;
  eventCountsG2?: Record<string, number>;
  // persistence: initial positions from external storage (traitlet / file)
  initialPositions?: Record<string, StoredPosition>;
  // called on every drag-end; parent can persist to Python / file
  onPositionsChange?: (positions: Record<string, StoredPosition>) => void;
  // persistence: edge filter — per-node top-k ("auto", the default) or a
  // manual [min, max] weight range normalized to 0..1
  initialEdgeFilter?: EdgeFilterSpec | null;
  onEdgeFilterChange?: (filter: EdgeFilterSpec) => void;
  // persistence: canvas zoom/pan
  initialViewport?: StoredViewport | null;
  onViewportChange?: (viewport: StoredViewport) => void;
  // ref that receives a fit() function — call to fit the graph to the canvas
  fitRef?: React.MutableRefObject<(() => void) | undefined>;
  // GraphView: named visual presets rendered as pills above the graph
  views?: GraphView[];
  // view (or name from `views`) applied once after the first build
  initialView?: GraphView | string | null;
  // ref that receives applyView() — external entry point (analysis links)
  applyViewRef?: React.MutableRefObject<
    ((view: GraphView | string) => void) | undefined
  >;
}

export const TransitionGraph = observer(function TransitionGraph({
  store,
  host,
  widgetId,
  valuesType: valuesTypeProp,
  onValuesTypeChange,
  diffSegment,
  diffValue1,
  diffValue2,
  theme = "auto",
  isFullscreen = false,
  onFullscreenToggle,
  eventCounts,
  eventCountsG1,
  eventCountsG2,
  initialPositions,
  onPositionsChange,
  initialEdgeFilter,
  onEdgeFilterChange,
  initialViewport,
  onViewportChange,
  fitRef,
  views,
  initialView,
  applyViewRef,
}: TransitionGraphProps) {
  // internal state for uncontrolled valuesType
  const [internalValuesType, setInternalValuesType] = React.useState<MatrixValueType>(valuesTypeProp ?? DEFAULT_VALUE_TYPE);
  React.useEffect(() => { if (valuesTypeProp !== undefined) setInternalValuesType(valuesTypeProp); }, [valuesTypeProp]);
  const currentValuesType = valuesTypeProp !== undefined ? valuesTypeProp : internalValuesType;
  const handleValuesTypeChange = (v: MatrixValueType) => { setInternalValuesType(v); onValuesTypeChange?.(v); };

  // Node POSITIONS are namespaced per widget instance (widget_id is a fresh
  // uuid per Python widget), so one widget's manual arrangement never leaks
  // into another or a re-created one — that leak silently suppressed the
  // backend auto-layout. Colors and the legend collapse state are user
  // preferences keyed by event names and deliberately stay in the shared
  // namespace, surviving cell re-runs.
  const effectiveWidgetId = widgetId ?? "default";
  const sharedPreferencesId = "default";
  const { getEdgeColor, setEdgeColor, removeEdgeColor } = useEdgeColors(sharedPreferencesId);
  const { getNodeColor, setNodeColor, removeNodeColor } = useNodeColors(sharedPreferencesId);
  const {
    positions: savedPositions,
    savePositions,
    hasSavedPositions,
  } = useNodePositions(effectiveWidgetId);

  // Skip the backend layout compute entirely when the saved arrangement
  // (state file / traitlet) already covers every event — the result would
  // be discarded anyway (saved positions always win per node). Partial
  // coverage still computes: events missing from the saved arrangement get
  // computed positions. Mount-only decision, like the compute itself.
  const skipLayoutCompute = React.useMemo(() => {
    if (!initialPositions) return false;
    const eventIds = Array.from(store.events.keys()).filter((id) => id !== "");
    if (eventIds.length === 0) return false;
    return eventIds.every((id) => {
      const position = initialPositions[id];
      return (
        position &&
        Number.isFinite(position.x) &&
        Number.isFinite(position.y)
      );
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  const { data: graphLayoutData, isLoading: isGraphLayoutLoading } =
    useGraphLayout(skipLayoutCompute ? null : host);

  const [edgeFilter, setEdgeFilterState] = React.useState<EdgeFilterSpec>(
    () => initialEdgeFilter ?? { mode: "topk", k: DEFAULT_TOP_K },
  );
  const handleEdgeFilterChange = React.useCallback(
    (filter: EdgeFilterSpec) => {
      // User-driven filter change: the state diverged from any applied view
      setActiveViewName(null);
      setEdgeFilterState(filter);
      onEdgeFilterChange?.(filter);
    },
    [onEdgeFilterChange],
  );

  // One-time hint explaining the default Auto edge filter. Dismissal is a
  // user preference in the shared namespace (like colors/legend collapse,
  // see the comment above sharedPreferencesId) so it survives cell re-runs
  // instead of reappearing every time the Python widget gets a fresh id.
  const edgeFilterHintStorageKey = `transition-graph-edge-filter-hint:${sharedPreferencesId}`;
  const [edgeFilterHintDismissed, setEdgeFilterHintDismissed] =
    React.useState<boolean>(() => {
      try {
        return (
          window.localStorage.getItem(edgeFilterHintStorageKey) === "dismissed"
        );
      } catch {
        return false;
      }
    });
  const dismissEdgeFilterHint = React.useCallback(() => {
    setEdgeFilterHintDismissed(true);
    try {
      window.localStorage.setItem(edgeFilterHintStorageKey, "dismissed");
    } catch {
      /* localStorage unavailable */
    }
  }, [edgeFilterHintStorageKey]);

  const requestedValueType = currentValuesType;
  const isDark =
    theme === "dark" ||
    (theme !== "light" &&
      typeof window !== "undefined" &&
      window.matchMedia("(prefers-color-scheme: dark)").matches);
  const [searchOpen, setSearchOpen] = React.useState(false);

  const [committedValueType, setCommittedValueType] =
    React.useState<MatrixValueType>(requestedValueType);
  const [committedDiffSegment, setCommittedDiffSegment] = React.useState<
    string | null
  >(diffSegment ?? null);
  const [committedMatrixType, setCommittedMatrixType] = React.useState<
    "differential" | "proba_in" | "proba_out" | undefined
  >(store.matrixType);
  const lastCommittedDataVersionRef = React.useRef(0);

  const isTimeValue = isTimeValueType(committedValueType);
  const isProbability = isProbabilityValueType(committedValueType);

  const containerRef = React.useRef<HTMLDivElement>(null);
  const cyRef = React.useRef<Core | null>(null);
  const positionsRef = React.useRef<Record<string, StoredPosition>>({});
  // Last known zoom/pan — restored on cy recreation (recompute) and persisted
  // through onViewportChange. Kept in refs so the graph-creation effect doesn't
  // depend on the callback identity.
  const viewportRef = React.useRef<StoredViewport | null>(initialViewport ?? null);
  const viewportSaveTimeoutRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);
  const onViewportChangeRef = React.useRef(onViewportChange);
  onViewportChangeRef.current = onViewportChange;
  const focusProgressRef = React.useRef(0);
  const focusAnimationFrameRef = React.useRef<number | null>(null);
  const resizeRafRef = React.useRef<number | null>(null);
  const dashAnimRef = React.useRef<ReturnType<typeof setInterval> | null>(null);
  const isDraggingRef = React.useRef(false);

  const [tooltip, setTooltip] = React.useState<TooltipState>(null);
  const [colorPicker, setColorPicker] = React.useState<ColorPickerState | null>(
    null,
  );
  const [positionsVersion, setPositionsVersion] = React.useState(0);
  const [sceneMaskOpacity, setSceneMaskOpacity] = React.useState(0);
  const [focusedNodes, setFocusedNodes] = React.useState<string[]>([]);
  // Node focus shows ONE direction at a time outside diff mode: click =
  // outgoing, double-click = incoming. No color code needed — the shown
  // edges stay neutral gray on the dimmed background. Diff mode always
  // shows both directions (its red/blue is the natural code there).
  const [focusDirection, setFocusDirection] = React.useState<"out" | "in">(
    "out",
  );
  // Edge-focus mode (click an edge) — mutually exclusive with focusedNodes.
  const [focusedEdge, setFocusedEdge] = React.useState<{
    source: string;
    target: string;
  } | null>(null);
  // Path-focus mode (GraphView focus.type === "path" or an interactive
  // Cmd/Ctrl+click selection): the amber overlay marks the route, this state
  // dims everything outside it.
  const [focusedPath, setFocusedPath] = React.useState<string[] | null>(null);
  // True while the path is being assembled by Cmd/Ctrl+clicks: the dim is
  // softer and node labels stay visible so the next node can still be found
  // and clicked. Applied views (pills/links) dim at full strength.
  const [pathSelecting, setPathSelecting] = React.useState(false);
  // Ego view modal: the event whose neighborhood is expanded, or null.
  const [egoNode, setEgoNode] = React.useState<string | null>(null);
  // Mirrors for the cytoscape tap handlers (closures inside the build
  // effect would otherwise see stale state).
  const focusedNodesRef = React.useRef(focusedNodes);
  focusedNodesRef.current = focusedNodes;
  const focusedPathRef = React.useRef(focusedPath);
  focusedPathRef.current = focusedPath;
  // GraphView machinery: name of the last applied view pill (null = none /
  // user diverged), the pre-view snapshot the Default pill restores, and a
  // viewport request processed after the commit (post-rebuild) it triggered.
  const [activeViewName, setActiveViewName] = React.useState<string | null>(
    null,
  );
  const viewSnapshotRef = React.useRef<{
    edgeFilter: EdgeFilterSpec;
    focusedNodes: string[];
    focusedEdge: { source: string; target: string } | null;
    focusedPath: string[] | null;
    focusDirection: "out" | "in";
    nodePositions: Record<string, StoredPosition>;
    population: { min: number; max: number } | null;
    hidden: string[];
    overlayEdges: Array<{ from: string; to: string }>;
    viewport: StoredViewport | null;
  } | null>(null);
  const pendingViewViewportRef = React.useRef<GraphView | null>(null);
  const [copiedView, setCopiedView] = React.useState(false);
  // Route badge: backend stats for the focused path + user's metric choice
  // (null = follow the edge weight)
  const [pathStats, setPathStats] = React.useState<PathStatsResult | null>(
    null,
  );
  const [pathMetricOverride, setPathMetricOverride] =
    React.useState<PathMetric | null>(null);
  // Bumped after every cytoscape rebuild so effects that mutate the current cy
  // instance (filtering, label allocation) re-run against the new generation.
  const [graphVersion, setGraphVersion] = React.useState(0);
  const sceneTransitionTimeoutRef = React.useRef<ReturnType<
    typeof setTimeout
  > | null>(null);
  const hasShownInitialSceneRef = React.useRef(false);
  const isDifferential =
    !!committedDiffSegment || committedMatrixType === "differential";
  const diffLabels = React.useMemo(
    () => resolveDiffLabels(diffSegment, diffValue1, diffValue2),
    [diffSegment, diffValue1, diffValue2],
  );
  const graphLayoutResult = graphLayoutData?.result ?? null;
  const tooltipSide = React.useMemo<"left" | "right">(() => {
    if (!tooltip) return "right";
    const containerWidth = containerRef.current?.clientWidth ?? 0;
    if (containerWidth <= 0) return "right";
    return tooltip.position.x > containerWidth * 0.62 ? "left" : "right";
  }, [tooltip]);
  const edgeColorPicker = colorPicker?.kind === "edge" ? colorPicker : null;
  const nodeColorPicker = colorPicker?.kind === "node" ? colorPicker : null;


  React.useEffect(() => {
    if (!store.hasData || store.isUpdating) return;
    if (store.dataVersion <= lastCommittedDataVersionRef.current) return;

    setCommittedValueType(requestedValueType);
    setCommittedDiffSegment(diffSegment ?? null);
    setCommittedMatrixType(store.matrixType);
    lastCommittedDataVersionRef.current = store.dataVersion;
  }, [
    store.hasData,
    store.dataVersion,
    store.isUpdating,
    store.matrixType,
    requestedValueType,
    diffSegment,
  ]);

  React.useEffect(() => {
    if (!store.hasData) return;

    if (sceneTransitionTimeoutRef.current) {
      clearTimeout(sceneTransitionTimeoutRef.current);
      sceneTransitionTimeoutRef.current = null;
    }

    if (!hasShownInitialSceneRef.current) {
      // First appearance — no flash, just show immediately
      hasShownInitialSceneRef.current = true;
      setSceneMaskOpacity(0);
      return;
    }

    // Subsequent data refreshes: brief flash to signal update
    setSceneMaskOpacity(1);
    sceneTransitionTimeoutRef.current = setTimeout(() => {
      setSceneMaskOpacity(0);
      sceneTransitionTimeoutRef.current = null;
    }, 180);
  }, [store.dataVersion, store.hasData]);

  React.useEffect(() => {
    return () => {
      if (sceneTransitionTimeoutRef.current) {
        clearTimeout(sceneTransitionTimeoutRef.current);
        sceneTransitionTimeoutRef.current = null;
      }
    };
  }, []);

  // Track if we've done initial load
  const initialLoadDoneRef = React.useRef(false);

  // Load positions on initial mount. Priority:
  //   1. initialPositions prop (from Python traitlet — survives kernel restart)
  //   2. localStorage (survives page refresh within the same browser session)
  //   3. None — fcose auto-layout runs
  React.useEffect(() => {
    if (initialLoadDoneRef.current) return;

    if (initialPositions && Object.keys(initialPositions).length > 0) {
      positionsRef.current = { ...initialPositions };
      initialLoadDoneRef.current = true;
      setPositionsVersion((v) => v + 1);
    } else if (hasSavedPositions) {
      positionsRef.current = savedPositions;
      initialLoadDoneRef.current = true;
      setPositionsVersion((v) => v + 1);
    } else {
      positionsRef.current = {};
    }
  }, [initialPositions, hasSavedPositions, savedPositions]);

  // Reset initialLoadDoneRef when widgetId changes
  React.useEffect(() => {
    initialLoadDoneRef.current = false;
  }, [effectiveWidgetId]);

  const persistPositions = React.useCallback(() => {
    // From this point positionsRef is the source of truth: ignore the
    // traitlet echo of this very persist coming back through the
    // initialPositions prop — it would only trigger a redundant rebuild
    // (visible as a viewport flicker on the first drag).
    initialLoadDoneRef.current = true;
    savePositions(positionsRef.current);       // localStorage
    onPositionsChange?.(positionsRef.current); // → Python traitlet / file
  }, [savePositions, onPositionsChange]);

  // Deterministic auto-layout pipeline for graphs WITHOUT backend semantic
  // positions: fcose starting from the current (preset, hash-based)
  // positions, left-to-right flip anchored on session_start, persist, fit.
  // When the backend provides semantic positions they are final and are
  // applied as-is — running fcose over them (even constrained) destroys the
  // cluster-block structure they encode.
  const runAutoLayout = React.useCallback(
    (cy: Core) => {
      const layout = cy.layout({
        name: "fcose",
        randomize: false,
        animate: false,
        fit: true,
        padding: FIT_PADDING,
        nodeSeparation: 150,
        nodeRepulsion: 50000,
        idealEdgeLength: 200,
        edgeElasticity: 0.45,
        nestingFactor: 0.1,
        gravity: 0.1,
        gravityRange: 3.8,
      } as any);

      layout.on("layoutstop", () => {
        // Enforce Left-to-Right orientation
        const anchorNode = "session_start";
        const hasAnchor = cy.getElementById(anchorNode).length > 0;
        const effectiveStartNode = hasAnchor
          ? anchorNode
          : cy.nodes()[0]?.id();

        if (
          effectiveStartNode &&
          cy.getElementById(effectiveStartNode).length > 0
        ) {
          const startPos = cy.getElementById(effectiveStartNode).position();

          // If start node is on the right (positive x), flip everything
          if (startPos.x > 0) {
            cy.nodes().forEach((node) => {
              const pos = node.position();
              node.position({ x: -pos.x, y: pos.y });
            });
          }
        }

        recalculateSelfLoopDirections(cy);

        // Update positions ref after layout. Auto-layout results are NOT
        // persisted — the layout is deterministic, so a recompute always
        // reproduces them; only manual arrangements (drag) are saved.
        cy.nodes().forEach((node) => {
          const pos = node.position();
          positionsRef.current[node.id()] = { x: pos.x, y: pos.y };
        });

        // Fit after auto-layout completes
        requestAnimationFrame(() => cy.fit(undefined, FIT_PADDING));
      });

      // fcose runs synchronously with animate:false; the seeded PRNG is what
      // actually makes the run reproducible — cose-base still draws
      // Math.random() internally even with randomize:false.
      withSeededRandom(LAYOUT_RANDOM_SEED, () => layout.run());
    },
    [],
  );

  const visibleEvents = store.visibleEvents;
  // Include ALL events (visible + hidden) so users can find and unhide them
  const graphSearchEvents = React.useMemo(
    () =>
      Array.from(store.events.values()).map((event) => ({
        id: event.id,
        name: event.id,
      })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [store.events.size],
  );

  // Fit a node together with its visible neighborhood so none of its edges
  // end outside the viewport. Manual zoom math because animate's `fit` has
  // no upper zoom clamp (an isolated node would blow up to maxZoom) and
  // display:none elements still contribute to boundingBox.
  const fitNodeNeighborhood = React.useCallback(
    (cy: Core, eventId: string, direction: "out" | "in" | "both" = "both") => {
      const node = cy.getElementById(eventId);
      if (node.length === 0) return;
      const connected =
        direction === "out"
          ? node.outgoers("edge")
          : direction === "in"
            ? node.incomers("edge")
            : node.connectedEdges();
      const edges = connected.filter(
        (edge: cytoscape.EdgeSingular) => !edge.hasClass("filtered"),
      );
      const neighborhood = edges.union(edges.connectedNodes()).union(node);
      const bb = neighborhood.boundingBox();
      const padding = 60;
      const zoomX = bb.w > 0 ? (cy.width() - padding * 2) / bb.w : Infinity;
      const zoomY = bb.h > 0 ? (cy.height() - padding * 2) / bb.h : Infinity;
      const zoom = Math.max(Math.min(zoomX, zoomY, 2.2), cy.minZoom());
      cy.animate({
        center: { eles: neighborhood },
        zoom: Number.isFinite(zoom) ? zoom : 2.2,
        duration: 350,
        easing: "ease-out-cubic",
      });
    },
    [],
  );

  const focusNodeFromSearch = React.useCallback(
    (eventId: string) => {
      const cy = cyRef.current;
      if (!cy) return;
      fitNodeNeighborhood(cy, eventId, "out");
      // Focus the node persistently instead of a temporary highlight
      setActiveViewName(null);
      setFocusedEdge(null);
      setFocusDirection("out");
      setFocusedNodes([eventId]);
      setSearchOpen(false);
    },
    [setFocusedNodes, fitNodeNeighborhood],
  );

  // ── GraphView: apply / snapshot / reset ────────────────────────────────────

  const captureViewSnapshot = React.useCallback(() => {
    viewSnapshotRef.current = {
      edgeFilter,
      focusedNodes,
      focusedEdge,
      focusedPath,
      focusDirection,
      nodePositions: Object.fromEntries(
        Object.entries(positionsRef.current).map(([id, pos]) => [
          id,
          { ...pos },
        ]),
      ),
      population: store.populationCustomized
        ? { ...store.filters.population }
        : null,
      hidden: Array.from(store.events.entries())
        .filter(([, e]) => e.isHidden)
        .map(([id]) => id),
      overlayEdges: Array.from(store.overlayPathEdges).map((key: string) => {
        const [from, to] = key.split("|");
        return { from, to };
      }),
      viewport: viewportRef.current
        ? { zoom: viewportRef.current.zoom, pan: { ...viewportRef.current.pan } }
        : null,
    };
  }, [edgeFilter, focusedNodes, focusedEdge, focusedPath, focusDirection, store]);

  const setHiddenEvents = React.useCallback(
    (hidden: string[]) => {
      const hiddenSet = new Set(hidden);
      store.events.forEach((e, id) => {
        const shouldHide = hiddenSet.has(id);
        if (e.isHidden !== shouldHide) {
          store.events.set(id, { ...e, isHidden: shouldHide });
        }
      });
    },
    [store],
  );

  // Restore the pre-view snapshot (shared by the Default pill and view
  // switching — views are absolute presets over the Default state, so
  // switching pills must not inherit leftovers from the previous view).
  // Apply exact node coordinates (from a view or a snapshot) to the live
  // cy instance and the in-memory ref. Not persisted — only manual drags
  // are ever saved.
  const applyNodePositions = React.useCallback(
    (positions: Record<string, StoredPosition>) => {
      const cy = cyRef.current;
      Object.entries(positions).forEach(([id, pos]) => {
        positionsRef.current[id] = { ...pos };
        const node = cy?.getElementById(id);
        if (node && node.length > 0) node.position({ ...pos });
      });
      if (cy) recalculateSelfLoopDirections(cy);
    },
    [],
  );

  const restoreViewSnapshot = React.useCallback(() => {
    const snap = viewSnapshotRef.current;
    if (!snap) return;
    setEdgeFilterState(snap.edgeFilter);
    if (snap.population) {
      store.setPopulationRange(snap.population.min, snap.population.max);
    } else {
      store.filters.population = { ...store.populationBounds };
      store.populationCustomized = false;
    }
    setHiddenEvents(snap.hidden);
    applyNodePositions(snap.nodePositions);
    store.applyPathEdges(snap.overlayEdges);
    setFocusedNodes(snap.focusedNodes);
    setFocusedEdge(snap.focusedEdge);
    setFocusedPath(snap.focusedPath);
    setFocusDirection(snap.focusDirection);
    setPathSelecting(false);
  }, [store, setHiddenEvents, applyNodePositions]);

  const applyView = React.useCallback(
    (target: GraphView | string) => {
      const view =
        typeof target === "string"
          ? (views ?? []).find((v) => v.name === target) ?? null
          : parseGraphView(target);
      if (!view) return;

      // First view application: remember the state the Default pill
      // restores. Subsequent ones: start from that baseline so views don't
      // inherit each other's filters/focus.
      if (!viewSnapshotRef.current) captureViewSnapshot();
      else restoreViewSnapshot();

      if (view.edgeFilter) setEdgeFilterState(view.edgeFilter);
      if (view.eventCountFilter) {
        store.setPopulationRange(...view.eventCountFilter);
      }
      if (view.hiddenEvents) setHiddenEvents(view.hiddenEvents);
      if (view.nodePositions) applyNodePositions(view.nodePositions);

      store.applyPathEdges([]);
      setPathSelecting(false); // applied views always dim at full strength
      const focus = view.focus;
      if (focus?.type === "node") {
        setFocusedEdge(null);
        setFocusedPath(null);
        setFocusDirection(focus.direction === "in" ? "in" : "out");
        setFocusedNodes([focus.id]);
      } else if (focus?.type === "edge") {
        setFocusedNodes([]);
        setFocusedPath(null);
        setFocusedEdge({ source: focus.source, target: focus.target });
      } else if (focus?.type === "path") {
        setFocusedNodes([]);
        setFocusedEdge(null);
        setFocusedPath(focus.nodes);
      } else {
        setFocusedNodes([]);
        setFocusedEdge(null);
        setFocusedPath(null);
      }

      // Viewport is applied by a dedicated effect AFTER this commit's
      // rebuild/filtering settled (hidden-event changes rebuild the graph).
      pendingViewViewportRef.current = view;
      setActiveViewName(view.name ?? null);
    },
    [
      views,
      captureViewSnapshot,
      restoreViewSnapshot,
      setHiddenEvents,
      applyNodePositions,
      store,
    ],
  );

  const resetToDefaultView = React.useCallback(() => {
    const snap = viewSnapshotRef.current;
    if (!snap) {
      setActiveViewName(null);
      return;
    }
    restoreViewSnapshot();
    pendingViewViewportRef.current = {
      viewport: snap.viewport ?? "fit",
    };
    setActiveViewName(null);
  }, [restoreViewSnapshot]);

  // Serialize the CURRENT live state as a GraphView — the "Copy view link"
  // button (docs authors: click together the state, copy, paste).
  const buildCurrentView = React.useCallback((): GraphView => {
    const view: GraphView = { v: 1 };
    if (focusedPath) {
      view.focus = { type: "path", nodes: focusedPath };
    } else if (focusedEdge) {
      view.focus = { type: "edge", ...focusedEdge };
    } else if (focusedNodes.length === 1) {
      view.focus = {
        type: "node",
        id: focusedNodes[0],
        ...(focusDirection === "in" ? { direction: "in" as const } : {}),
      };
    }
    view.edgeFilter = edgeFilter;
    if (store.populationCustomized) {
      view.eventCountFilter = [
        store.filters.population.min,
        store.filters.population.max,
      ];
    }
    const hidden = Array.from(store.events.entries())
      .filter(([, e]) => e.isHidden)
      .map(([id]) => id);
    if (hidden.length > 0) view.hiddenEvents = hidden;
    if (viewportRef.current) {
      view.viewport = {
        zoom: viewportRef.current.zoom,
        pan: { ...viewportRef.current.pan },
      };
    }
    // Node positions are deliberately NOT captured: manual arrangements
    // persist on their own (per-dataset widget_id namespace + the
    // node_positions traitlet, which exports also carry), and pinning them
    // in every copied view would freeze future layout improvements. A view
    // may still declare nodePositions by hand.
    return view;
  }, [edgeFilter, focusedNodes, focusedEdge, focusedPath, focusDirection, store]);

  React.useEffect(() => {
    if (applyViewRef) applyViewRef.current = applyView;
  });

  // Calculate max weight for the current view and value type
  const maxWeight = React.useMemo(() => {
    if (!store.hasData || visibleEvents.length === 0) return 0;
    let currentMax = 0;

    visibleEvents.forEach((rowEvent) => {
      visibleEvents.forEach((colEvent) => {
        const val = store.getMatrixValue(rowEvent.id, colEvent.id);
        if (!Number.isFinite(val) || val === 0) return;

        const magnitude = isDifferential ? Math.abs(val) : val;
        if (magnitude > currentMax) {
          currentMax = magnitude;
        }
      });
    });

    return currentMax;
  }, [store, visibleEvents, isDifferential]);

  const nodeBaseSizes = React.useMemo(() => {
    const sizeMap = new Map<string, number>();
    if (visibleEvents.length === 0) return sizeMap;

    const logPopulations = visibleEvents.map((event) =>
      Math.log1p(Math.max(0, event.population)),
    );

    const minPopulation = Math.min(...logPopulations);
    const maxPopulation = Math.max(...logPopulations);

    visibleEvents.forEach((event, index) => {
      const normalized = normalizeRange(
        logPopulations[index],
        minPopulation,
        maxPopulation,
        0.6,
      );
      const nodeSize =
        NODE_BASE_MIN_SIZE +
        normalized * (NODE_BASE_MAX_SIZE - NODE_BASE_MIN_SIZE);
      sizeMap.set(event.id, nodeSize);
    });

    return sizeMap;
  }, [visibleEvents]);

  const formatValue = React.useCallback(
    (value: number) => (isTimeValue ? formatTime(value) : formatNumber(value)),
    [isTimeValue],
  );

  // Single source of truth for the edge set: consumed by the graph-build
  // effect, the label allocator, and the legend's coverage indicator.
  // Sorted by |weight| descending.
  const edgeList = React.useMemo(() => {
    if (!store.hasData) return [];
    const graphEvents = visibleEvents.filter((e) => e.id !== "");

    // Normalization mirroring the edge rendering rules. Probabilities keep
    // their absolute meaning in normal mode (thickness = probability, 100%
    // = thickest) — but NOT in diff mode: |Δp| rarely approaches 1, so an
    // absolute scale would render every diff edge thin and pale. Diffs are
    // normalized against the largest |Δp| on the graph, like other types.
    const normalize = (weight: number): number => {
      const magnitude = isDifferential ? Math.abs(weight) : weight;
      if (isProbability && !isDifferential) {
        return Math.max(0, Math.min(1, magnitude));
      }
      if (maxWeight <= 0) return 0;
      return magnitude / maxWeight;
    };

    const list: Array<{
      source: string;
      target: string;
      weight: number;
      normalizedWeight: number;
      isSelfLoop: boolean;
      hasBackward: boolean;
    }> = [];

    graphEvents.forEach((rowEvent) => {
      graphEvents.forEach((colEvent) => {
        const isSelfLoop = rowEvent.id === colEvent.id;

        const forwardValue = store.getMatrixValue(rowEvent.id, colEvent.id);
        if (!Number.isFinite(forwardValue) || forwardValue === 0) return;

        const backwardValue = store.getMatrixValue(colEvent.id, rowEvent.id);
        const hasBackward =
          !isSelfLoop && Number.isFinite(backwardValue) && backwardValue !== 0;

        list.push({
          source: rowEvent.id,
          target: colEvent.id,
          weight: forwardValue,
          normalizedWeight: normalize(forwardValue),
          isSelfLoop,
          hasBackward,
        });
      });
    });

    list.sort((a, b) => Math.abs(b.weight) - Math.abs(a.weight));
    return list;
  }, [store, visibleEvents, maxWeight, isProbability, isDifferential]);

  // Smallest nonzero |weight| on the graph — the data-driven lower bound of
  // the manual slider's log scale. Without it the scale starts at
  // max × 0.0001 and half the track can cover values that simply don't
  // exist (e.g. integer unique_paths diffs never go below 1).
  const minNonzeroWeight = React.useMemo(() => {
    let min = Infinity;
    edgeList.forEach((edge) => {
      const w = Math.abs(edge.weight);
      if (w > 0 && w < min) min = w;
    });
    return Number.isFinite(min) ? min : 0;
  }, [edgeList]);

  // Kept-edge set for the per-node top-k ("auto") filter mode.
  const keptEdgeKeys = React.useMemo(
    () =>
      edgeFilter.mode === "topk"
        ? computeTopKKeptSet(edgeList, edgeFilter.k)
        : null,
    [edgeFilter, edgeList],
  );

  // Single filter predicate shared by the cy filtering effect and the
  // legend's coverage indicator.
  const isEdgeVisible = React.useCallback(
    (source: string, target: string, normalizedWeight: number) => {
      if (edgeFilter.mode === "topk") {
        return keptEdgeKeys?.has(edgeKey(source, target)) ?? true;
      }
      return (
        normalizedWeight >= edgeFilter.range[0] &&
        normalizedWeight <= edgeFilter.range[1]
      );
    },
    [edgeFilter, keptEdgeKeys],
  );

  // Mirror of isEdgeVisible for the graph-build effect: read through a ref
  // so filter changes do NOT rebuild the graph (the filter effect handles
  // them in place), yet a rebuild still creates edges with the correct
  // `filtered` class — otherwise the first painted frame after any rebuild
  // flashes every edge.
  const isEdgeVisibleRef = React.useRef(isEdgeVisible);
  isEdgeVisibleRef.current = isEdgeVisible;

  // Same ref pattern for applyFocusState (declared below the build effect):
  // lets a rebuild re-apply focus dimming synchronously, before the first
  // paint, without a hook-ordering problem.
  const applyFocusStateRef = React.useRef<
    ((cy: Core, progress: number) => void) | null
  >(null);

  // Adaptive label allocation over the currently visible (non-filtered)
  // edges: small graphs get labels everywhere, large ones a bounded top share
  // by |weight| — so tightening the threshold automatically labels more of
  // what remains. Mutates edge *data* (not style), so resetFocusVisuals
  // restores the current allocation when focus mode exits. Declared before
  // the graph-build effect, which calls it for the first paint.
  const allocateEdgeLabels = React.useCallback((cy: Core) => {
    const visible = cy.edges().not(".filtered");
    const total = visible.length;
    const labeledCount =
      total <= EDGE_LABELS_ALL_THRESHOLD
        ? total
        : Math.min(
            EDGE_LABELS_MAX,
            Math.max(EDGE_LABELS_MIN, Math.ceil(total * EDGE_LABELS_SHARE)),
          );

    const sorted = visible.sort(
      (a, b) =>
        Math.abs(b.data("weight") as number) -
        Math.abs(a.data("weight") as number),
    );

    cy.batch(() => {
      cy.edges(".filtered").forEach((edge) => {
        edge.data("showLabel", false);
        edge.data("label", "");
        edge.data("zIndex", 1);
      });
      sorted.forEach((edge, index) => {
        const labeled = index < labeledCount;
        edge.data("showLabel", labeled);
        // Invariant: label must be "" when unlabeled — the edge[showLabel]
        // selector matches field *presence*, not truthiness.
        edge.data("label", labeled ? (edge.data("baseValue") as string) : "");
        edge.data("zIndex", labeled ? 999 : 1);
      });
    });
  }, []);

  // Edges that get cytoscape elements at all. null = build everything; above
  // the cap only the strongest EDGE_BUILD_CAP edges plus everything any
  // top-k setting could show (k ≤ TOP_K_MAX) are instantiated.
  const builtEdgeKeys = React.useMemo(() => {
    if (edgeList.length <= EDGE_BUILD_CAP) return null;
    const kept = computeTopKKeptSet(edgeList, TOP_K_MAX);
    edgeList
      .slice(0, EDGE_BUILD_CAP)
      .forEach((edge) => kept.add(edgeKey(edge.source, edge.target)));
    return kept;
  }, [edgeList]);

  // Coverage indicator data for the legend: how much of the graph the current
  // edge filter actually shows.
  const coverage = React.useMemo(() => {
    const total = edgeList.length;
    let shown = 0;
    let sumShown = 0;
    let sumTotal = 0;
    edgeList.forEach((edge) => {
      const w = Math.abs(edge.weight);
      sumTotal += w;
      const isBuilt =
        builtEdgeKeys === null ||
        builtEdgeKeys.has(edgeKey(edge.source, edge.target));
      if (
        isBuilt &&
        isEdgeVisible(edge.source, edge.target, edge.normalizedWeight)
      ) {
        shown += 1;
        sumShown += w;
      }
    });
    return {
      shown,
      total,
      weightShare: sumTotal > 0 ? sumShown / sumTotal : 1,
    };
  }, [edgeList, isEdgeVisible, builtEdgeKeys]);

  // Build / rebuild Cytoscape graph
  React.useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    if (!store.hasData || visibleEvents.length === 0) {
      cyRef.current?.destroy();
      cyRef.current = null;
      return;
    }

    // Guard: cytoscape rejects empty-string element IDs; skip events that the
    // backend generated with an empty name (e.g. from null URL column values).
    const graphEvents = visibleEvents.filter((e) => e.id !== "");

    // Edge set (with normalized weights) comes from the shared edgeList memo,
    // capped by builtEdgeKeys on huge graphs. Labels are allocated adaptively
    // after build by allocateEdgeLabels.
    const edgesToAdd = builtEdgeKeys
      ? edgeList.filter((edge) =>
          builtEdgeKeys.has(edgeKey(edge.source, edge.target)),
        )
      : edgeList;

    // Prepare Cytoscape elements
    const elements: ElementDefinition[] = [];
    let requiresAutoLayout = false;
    let usedApiPositions = false;

    // Get API-computed positions if available
    const apiPositions = graphLayoutResult ?? {};

    // Pre-compute node share diffs for coloring in differential mode
    const nodeDiffMap = new Map<string, number>();
    let maxAbsNodeDiff = 0;
    if (isDifferential) {
      graphEvents.forEach((event) => {
        const d = store.getNodeShareDiff(event.id);
        if (d != null) {
          nodeDiffMap.set(event.id, d);
          if (Math.abs(d) > maxAbsNodeDiff) maxAbsNodeDiff = Math.abs(d);
        }
      });
    }

    graphEvents.forEach((event) => {
      const existing = positionsRef.current[event.id];
      const isValidPosition =
        existing && Number.isFinite(existing.x) && Number.isFinite(existing.y);

      // Check if API has a position for this event
      const apiPosition = apiPositions[event.id];
      const hasApiPosition =
        apiPosition &&
        Number.isFinite(apiPosition.x) &&
        Number.isFinite(apiPosition.y);

      // Priority: saved position > API position > hash-based position.
      // While the backend layout compute is still in flight, hash positions
      // are TRANSIENT: they are not recorded into positionsRef, so the API
      // result can still claim these nodes when it arrives (recorded
      // positions would win forever and silently discard it).
      let position: StoredPosition;
      if (isValidPosition) {
        position = existing;
      } else if (hasApiPosition) {
        position = { x: apiPosition.x, y: apiPosition.y };
        positionsRef.current[event.id] = position;
        usedApiPositions = true;
      } else if (isGraphLayoutLoading) {
        position = createInitialPosition(event.id);
      } else {
        position = createInitialPosition(event.id);
        positionsRef.current[event.id] = position;
        requiresAutoLayout = true;
      }

      const safeX = Number.isFinite(position.x) ? position.x : 0;
      const safeY = Number.isFinite(position.y) ? position.y : 0;

      // In differential mode, color nodes by share diff unless manually overridden
      const manualColor = getNodeColor(event.id);
      let nodeColor: string;
      let nodeInnerRadius = 27;
      if (isDifferential && !manualColor && maxAbsNodeDiff > 0) {
        const d = nodeDiffMap.get(event.id);
        if (d != null) {
          const t = Math.pow(Math.abs(d) / maxAbsNodeDiff, 1.2);
          nodeColor = lerpRgb(NODE_DIFF_NEUTRAL, d > 0 ? NODE_DIFF_RED : NODE_DIFF_BLUE, 0.1 + t * 0.9);
          nodeInnerRadius = 27 * (1 - t); // 27 at zero diff → 0 at max diff
        } else {
          nodeColor = NODE_DIFF_NEUTRAL.join(", ");
        }
      } else {
        nodeColor = normalizeRgbColor(manualColor);
      }

      elements.push({
        group: "nodes",
        data: {
          id: event.id,
          label: event.id,
          nodeColor,
          nodeImage: getNodeImageDataUri(nodeColor, nodeInnerRadius),
          nodeSize: nodeBaseSizes.get(event.id) ?? NODE_BASE_MIN_SIZE,
        },
        position: { x: safeX, y: safeY },
      });
    });

    // Add edges
    edgesToAdd.forEach((edge) => {
      const forwardNormalized = edge.normalizedWeight;
      const isSelfLoop = edge.isSelfLoop;

      let edgeSize: number;
      let baseEdgeColor: string;
      let edgeOpacity: number;

      if (isDifferential) {
        // For differential mode, use power function to make large values pop
        const scaledNormalized = Math.pow(forwardNormalized, 1.2);
        edgeSize = 1 + scaledNormalized * 9;
        // Lower base opacity for better saturation contrast (weak diffs = pale, strong = vivid)
        edgeOpacity = 0.1 + scaledNormalized * 0.9;

        if (edge.weight > 0) {
          baseEdgeColor = "239, 68, 68"; // red-500
        } else {
          baseEdgeColor = "59, 130, 246"; // blue-500
        }
      } else {
        // For probability types (0-1), apply sqrt to stretch small differences
        // For other types, use linear scaling with lower base opacity
        const scaledNormalized = isProbability
          ? Math.sqrt(forwardNormalized)
          : forwardNormalized;

        edgeSize = 0.5 + scaledNormalized * 6.5;

        const defaultEdgeColor = isDark ? "156, 163, 175" : "75, 85, 99";
        const customColor = getEdgeColor(edge.source, edge.target);
        baseEdgeColor = customColor ?? defaultEdgeColor;

        // Lower base opacity for better contrast between thin and thick edges
        edgeOpacity = 0.25 + scaledNormalized * 0.75;
      }

      const edgeColor = applyAlphaColor(baseEdgeColor, edgeOpacity);

      const baseValue = isDifferential
        ? (edge.weight > 0 ? "+" : "") + formatValue(edge.weight)
        : formatValue(edge.weight);

      elements.push({
        group: "edges",
        // Filter state is applied at creation time so a rebuild never paints
        // a frame with every edge visible; the filter effect re-applies it
        // in place when the filter changes.
        classes: isEdgeVisibleRef.current(
          edge.source,
          edge.target,
          forwardNormalized,
        )
          ? undefined
          : "filtered",
        data: {
          id: `${edge.source}-${edge.target}`,
          source: edge.source,
          target: edge.target,
          // Labels are assigned adaptively by allocateEdgeLabels right after
          // the build (invariant: label is "" whenever showLabel is false).
          label: "",
          baseValue, // Just the number for dynamic arrow generation
          weight: edge.weight,
          normalizedWeight: forwardNormalized,
          showBaseArrow: forwardNormalized >= BASE_ARROW_MIN_VISIBLE_RATIO,
          isBidirectional: edge.hasBackward,
          isSelfLoop,
          baseColor: baseEdgeColor,
          baseAlpha: edgeOpacity,
          edgeSize,
          edgeColor,
          showLabel: false,
          loopDirection: DEFAULT_LOOP_DIRECTION,
          zIndex: 1, // Raised to 999 for labeled edges by allocateEdgeLabels
        },
      });
    });

    // Create stylesheet
    const labelColor = isDark ? "#f3f4f6" : "#1f2937";
    const outlineColor = isDark ? "#1f2937" : "#ffffff";
    // Edge label text color: dark in dark theme (light outline), light in light theme (dark outline)
    const edgeLabelColor = isDark ? "#1f2937" : "#f3f4f6";
    const overlayColor = isDark ? "#fbbf24" : "#f59e0b"; // amber for AI overlay
    const stylesheet: cytoscape.StylesheetStyle[] = [
      // Node styles
      {
        selector: "node",
        style: {
          "background-image": "data(nodeImage)",
          "background-fit": "contain",
          "background-clip": "node",
          width: "data(nodeSize)",
          height: "data(nodeSize)",
          "border-width": 0,
          "shadow-blur": 3,
          "shadow-color": "rgba(0,0,0,0.4)",
          "shadow-opacity": 0.5,
          "shadow-offset-x": 0,
          "shadow-offset-y": 1,
          label: "data(label)",
          "font-size": 14,
          "font-weight": "bold",
          color: labelColor,
          "text-valign": "center",
          "text-halign": "right",
          "text-margin-x": 8,
          "text-outline-color": outlineColor,
          "text-outline-width": 2,
          "overlay-opacity": 0, // Disable click/hover overlay
        } as any,
      },
      {
        selector: "node.search-focused",
        style: {
          "border-width": 5,
          "border-color": isDark ? "#fde68a" : "#d97706",
          "border-opacity": 1,
          "z-index": 9998,
          "shadow-opacity": 0.9,
          "shadow-blur": 12,
          "shadow-color": isDark ? "#facc15" : "#f59e0b",
          "transition-property": "border-width, shadow-blur, shadow-opacity",
          "transition-duration": "0.2s",
        } as any,
      },
      // Edge styles
      {
        selector: "edge",
        style: {
          width: "data(edgeSize)",
          "line-color": "data(edgeColor)",
          "target-arrow-color": "data(edgeColor)",
          "target-arrow-shape": (ele: cytoscape.EdgeSingular) =>
            (ele.data("showBaseArrow") as boolean) ? "triangle" : "none",
          "curve-style": "bezier",
          "arrow-scale": 1.0,
          "z-index-compare": "manual",
          "z-index": "data(zIndex)",
        },
      },
      // Edge labels - render on top so labels are visible
      {
        selector: "edge[showLabel]",
        style: {
          label: "data(label)",
          "font-size": (ele: cytoscape.EdgeSingular) => {
            const normalized = ele.data("normalizedWeight") as number;
            return 11 + normalized * 7; // 11-18px range
          },
          "font-weight": "bold",
          // Text color: dark in dark theme (light outline), light in light theme (dark outline)
          color: edgeLabelColor,
          // No background
          "text-background-opacity": 0,
          "text-margin-y": 0,
          "text-rotation": "autorotate",
          // Outline color matches edge color for seamless blend
          "text-outline-color": (ele: cytoscape.EdgeSingular) => {
            const baseColor = ele.data("baseColor") as string;
            return `rgb(${baseColor})`;
          },
          "text-outline-width": 4,
        } as any,
      },
      // Self-loop styles - direction is updated after layout and node release.
      {
        selector: "edge[source = target]",
        style: {
          "curve-style": "bezier",
          "loop-direction": "data(loopDirection)",
          "loop-sweep": "-90deg",
          "control-point-step-size": 80,
        } as any,
      },
      // Bidirectional edge curvature
      {
        selector: "edge[isBidirectional]",
        style: {
          "curve-style": "unbundled-bezier",
          "control-point-distances": [40],
          "control-point-weights": [0.5],
        },
      },
      // Filtered edges (hidden by edge threshold)
      {
        selector: "edge.filtered",
        style: {
          display: "none",
        },
      },
      // An explicitly focused edge outranks the edge filter (rule comes after
      // edge.filtered, so it wins) — focusing an edge by name must show it
      // even when the top-k/range filter would hide it.
      {
        selector: "edge.focus-visible",
        style: {
          display: "element",
        } as any,
      },
      // Dimmed edges in focus mode should not compete for attention/interactions.
      {
        selector: "edge.dimmed",
        style: {
          label: "",
          "text-background-opacity": 0,
          events: "no",
        } as any,
      },
      // Dimmed nodes/edges
      {
        selector: ".dimmed",
        style: {
          opacity: 0.1,
          "transition-property": "opacity",
          "transition-duration": "0.2s",
        } as any,
      },
      // Highlighted elements
      {
        selector: ".highlighted",
        style: {
          opacity: 1,
          "transition-property": "opacity",
          "transition-duration": "0.2s",
        } as any,
      },
      // AI overlay highlighted edges (from highlight pills)
      {
        selector: "edge.ai-overlay",
        style: {
          width: 12,
          "line-color": overlayColor,
          "target-arrow-color": overlayColor,
          "line-style": "dashed",
          "line-dash-pattern": [5, 4],
          "line-dash-offset": 0,
          "target-arrow-shape": "triangle",
          "arrow-scale": 1.5,
          opacity: 1,
          // An explicit highlight outranks the edge filter (this rule comes
          // after edge.filtered, so it wins and keeps the path visible)
          display: "element",
          "z-index": 9999,
        } as any,
      },
      // Path segments: a pure recolor — amber + dashed. Every size (width,
      // labels, arrowheads) is set inline with the node-focus formulas, so
      // a segment looks exactly like it did when it was picked.
      {
        selector: "edge.path-focus",
        style: {
          "line-color": overlayColor,
          "target-arrow-color": overlayColor,
          "line-style": "dashed",
          "line-dash-pattern": [5, 4],
          "line-dash-offset": 0,
          opacity: 1,
          display: "element",
          "z-index": 9999,
        } as any,
      },
    ];

    // Destroy previous instance
    cyRef.current?.destroy();

    // Create Cytoscape instance
    const cy = cytoscape({
      container,
      elements,
      style: stylesheet,
      layout: { name: "preset" }, // Use preset positions
      minZoom: 0.1,
      maxZoom: 3,
      wheelSensitivity: 0.5,
    });

    cyRef.current = cy;
    if (fitRef) fitRef.current = () => cy.fit(undefined, FIT_PADDING);
    // Expose cy on the container's root element so focusNode() can find it
    if (container?.parentElement) (container.parentElement as any).__cy = cy;

    // Track zoom/pan (user or programmatic) for persistence and recreation
    cy.on("viewport", () => {
      viewportRef.current = { zoom: cy.zoom(), pan: { ...cy.pan() } };
      if (viewportSaveTimeoutRef.current) clearTimeout(viewportSaveTimeoutRef.current);
      viewportSaveTimeoutRef.current = setTimeout(() => {
        if (viewportRef.current) onViewportChangeRef.current?.(viewportRef.current);
      }, 300);
    });

    // Initial edge threshold is applied by a dedicated effect below.

    // Apply fcose only when nodes had to fall back to hash positions, the
    // backend provided nothing, and its compute is not still in flight.
    // Backend semantic positions are final and never persisted here —
    // they are deterministic; only manual drags get saved.
    if (
      !hasSavedPositions &&
      requiresAutoLayout &&
      !usedApiPositions &&
      !isGraphLayoutLoading
    ) {
      runAutoLayout(cy);
    } else {
      recalculateSelfLoopDirections(cy);

      // Mirror current positions into the ref — but not while the layout
      // compute is in flight (those hash positions are transient).
      if (!isGraphLayoutLoading) {
        cy.nodes().forEach((node) => {
          const pos = node.position();
          positionsRef.current[node.id()] = { x: pos.x, y: pos.y };
        });
      }

      // Restore the saved viewport if there is one; otherwise fit to canvas
      // after the browser has laid out the container. When freshly arrived
      // backend positions were just applied, any remembered viewport refers
      // to the transient pre-layout graph — fit instead.
      //
      // The restore is synchronous: deferring it to a rAF paints one frame
      // at cytoscape's default zoom first, which reads as a zoom flicker on
      // every rebuild. Only fit() needs the container laid out, so only it
      // stays in a rAF.
      const savedViewport = usedApiPositions ? null : viewportRef.current;
      if (savedViewport) {
        cy.viewport({ zoom: savedViewport.zoom, pan: { ...savedViewport.pan } });
      } else {
        requestAnimationFrame(() => cy.fit(undefined, FIT_PADDING));
      }
    }

    // Event handlers
    let draggedNode: string | null = null;

    const releaseDrag = (nodeId: string | null = draggedNode) => {
      if (nodeId) {
        persistPositions();
      }

      isDraggingRef.current = false;
      draggedNode = null;
    };

    cy.on("grab", "node", (event) => {
      draggedNode = event.target.id();
      // Start drag tracking only after actual movement to avoid swallowing simple clicks.
      isDraggingRef.current = false;
      // Cytoscape's built-in shift-click selection causes all selected nodes to
      // move together on drag. Deselect everything so only the grabbed node moves.
      cy.elements().unselect();
    });

    cy.on("drag", "node", (event) => {
      isDraggingRef.current = true;
      const node = event.target;
      const pos = node.position();
      positionsRef.current[node.id()] = { x: pos.x, y: pos.y };
    });

    cy.on("free", "node", (event) => {
      const releasedNode = event.target as cytoscape.NodeSingular;
      const affectedNodeIds = new Set<string>([releasedNode.id()]);

      releasedNode.connectedEdges().forEach((edge: cytoscape.EdgeSingular) => {
        affectedNodeIds.add(edge.source().id());
        affectedNodeIds.add(edge.target().id());
      });

      recalculateSelfLoopDirections(cy, affectedNodeIds);
      releaseDrag(releasedNode.id());
    });

    // Node click (focus mode); Cmd/Ctrl+click assembles a PATH in click
    // order, rendered as path focus (amber route, softly dimmed rest).
    // Copy view link serializes it as focus.type === "path".
    cy.on("tap", "node", (event) => {
      if (isDraggingRef.current) return;
      const nodeId = event.target.id();
      const original = event.originalEvent as MouseEvent | undefined;
      const isMulti = (original?.metaKey || original?.ctrlKey) ?? false;
      if (isMulti) {
        // Continue from the current path, or start one from the currently
        // focused node. ALWAYS append — repeated nodes build loops and
        // cycles (A→B→B, A→B→A→C); removal is Cmd+double-click.
        const base =
          focusedPathRef.current ??
          (focusedNodesRef.current.length === 1
            ? [...focusedNodesRef.current]
            : []);
        const next = [...base, nodeId];
        if (next.length === 1) {
          setFocusedNodes(next);
          setFocusDirection("out");
          setFocusedPath(null);
          setPathSelecting(false);
        } else {
          setFocusedNodes([]);
          setFocusedPath(next);
          setPathSelecting(true);
        }
        setFocusedEdge(null);
      } else {
        setFocusedNodes([nodeId]);
        setFocusDirection("out");
        setFocusedEdge(null);
        setFocusedPath(null);
        setPathSelecting(false);
      }
      store.applyPathEdges([]);
      setActiveViewName(null);
      setColorPicker(null);
      setTooltip(null);
    });

    // Double-click switches the node focus to INCOMING transitions (a
    // single click shows outgoing; the intermediate single-tap state is the
    // same node's outgoing focus, so nothing is lost). Diff mode always
    // shows both directions — no switch there.
    cy.on("dbltap", "node", (event) => {
      if (isDraggingRef.current) return;
      const original = event.originalEvent as MouseEvent | undefined;
      if (original?.metaKey || original?.ctrlKey) {
        // Cmd+double-click removes the LAST node of the path — a
        // backspace-like undo. (Removing a mid-path node would silently
        // splice a gap: A→B→C turning into A→C is rarely what was meant.)
        // The gesture's own two Cmd+taps appended the clicked node twice:
        // the first two pops drop those, the third pops the real tail —
        // and only when the click was on that tail.
        const nodeId = event.target.id();
        const current = focusedPathRef.current;
        if (!current) return;
        const next = [...current];
        if (next[next.length - 1] === nodeId) next.pop();
        if (next[next.length - 1] === nodeId) next.pop();
        if (next[next.length - 1] === nodeId) next.pop();
        if (next.length === 0) {
          setFocusedNodes([]);
          setFocusedPath(null);
          setPathSelecting(false);
        } else if (next.length === 1) {
          setFocusedNodes(next);
          setFocusDirection("out");
          setFocusedPath(null);
          setPathSelecting(false);
        } else {
          setFocusedPath(next);
        }
        return;
      }
      if (isDifferential) return;
      setFocusedNodes([event.target.id()]);
      setFocusDirection("in");
      setFocusedEdge(null);
      setFocusedPath(null);
      setPathSelecting(false);
      store.applyPathEdges([]);
      setActiveViewName(null);
      setColorPicker(null);
      setTooltip(null);
    });

    // Edge hover (tooltip)
    cy.on("mouseover", "edge", (event) => {
      const edge = event.target;
      if (edge.hasClass("dimmed")) {
        setTooltip(null);
        return;
      }

      const source = edge.source().id();
      const target = edge.target().id();
      const weight = edge.data("weight") as number;
      const isBidirectional = edge.data("isBidirectional") as boolean;
      const breakdown = store.getDiffCellBreakdown(source, target);

      const renderedPosition = event.renderedPosition || event.position;

      setTooltip({
        type: "edge",
        id: edge.id(),
        from: source,
        to: target,
        forwardWeight: weight,
        group1Value: breakdown.group1Value,
        group2Value: breakdown.group2Value,
        diffValue: breakdown.diffValue,
        isBidirectional,
        position: { x: renderedPosition.x, y: renderedPosition.y },
      });
    });

    cy.on("mouseout", "edge", () => {
      setTooltip(null);
    });

    // Node hover (tooltip — diff mode only)
    cy.on("mouseover", "node", (event) => {
      if (!isDifferential) return;
      const node = event.target;
      if (node.hasClass("dimmed")) return;
      const eventId = node.id() as string;
      const breakdown = store.getNodeShareBreakdown(eventId);
      if (breakdown.diffValue == null) return;
      const pos = event.renderedPosition || event.position;
      setTooltip({ type: "node", eventId, ...breakdown, position: { x: pos.x, y: pos.y } });
    });

    cy.on("mouseout", "node", () => {
      setTooltip((t) => t?.type === "node" ? null : t);
    });

    // Edge click → edge-focus mode (dims everything else, fits the node pair).
    // Dimmed edges have events:"no", so this never fires for them; tapping a
    // highlighted edge while node-focused switches to edge focus. The edge
    // color picker lives in the toolbar while an edge is focused.
    cy.on("tap", "edge", (event) => {
      const edge = event.target;
      if (isDraggingRef.current || edge.hasClass("dimmed")) return;

      const source = edge.source().id();
      const target = edge.target().id();

      setFocusedNodes([]);
      setFocusedEdge({ source, target });
      setFocusedPath(null);
      setPathSelecting(false);
      store.applyPathEdges([]);
      setActiveViewName(null);
      setColorPicker(null);
      setTooltip(null);

      const pair = edge.union(edge.connectedNodes());
      cy.animate({
        fit: { eles: pair, padding: 100 },
        duration: 350,
        easing: "ease-out-cubic",
      });
    });

    // Click stage to clear focus / close color picker
    cy.on("tap", (event) => {
      if (event.target === cy) {
        setFocusedNodes([]);
        setFocusedEdge(null);
        setFocusedPath(null);
        setPathSelecting(false);
        store.applyPathEdges([]);
        setActiveViewName(null);
        setColorPicker(null);
        setTooltip(null);
      }
    });

    // Allocate labels right away so the first painted frame is complete
    // (edges are already created with their `filtered` classes above), and
    // re-apply focus dimming if a focus is active.
    allocateEdgeLabels(cy);
    if (focusProgressRef.current > 0) {
      applyFocusStateRef.current?.(cy, focusProgressRef.current);
    }

    // Signal the new cy generation so the filter/label effect re-runs
    // against it when the filter changes later.
    setGraphVersion((v) => v + 1);

    return () => {
      if (dashAnimRef.current !== null) {
        clearInterval(dashAnimRef.current);
        dashAnimRef.current = null;
      }
      cy.destroy();
      cyRef.current = null;
    };
  }, [
    containerRef,
    formatValue,
    persistPositions,
    runAutoLayout,
    allocateEdgeLabels,
    visibleEvents,
    visibleEvents.length,
    positionsVersion,
    isDark,
    isProbability,
    isDifferential,
    getEdgeColor,
    getNodeColor,
    hasSavedPositions,
    store,
    graphLayoutResult,
    isGraphLayoutLoading,
    edgeList,
    nodeBaseSizes,
  ]);

  React.useEffect(() => {
    const container = containerRef.current;
    if (!container || typeof ResizeObserver === "undefined") return;

    const observer = new ResizeObserver(() => {
      const cy = cyRef.current;
      if (!cy) return;

      if (resizeRafRef.current !== null) {
        cancelAnimationFrame(resizeRafRef.current);
      }

      resizeRafRef.current = requestAnimationFrame(() => {
        cy.resize();
      });
    });

    observer.observe(container);

    return () => {
      observer.disconnect();
      if (resizeRafRef.current !== null) {
        cancelAnimationFrame(resizeRafRef.current);
        resizeRafRef.current = null;
      }
    };
  }, []);

  // Apply AI overlay highlights from store
  React.useEffect(() => {
    const cy = cyRef.current;

    if (!cy) return;

    // Stop any running dash animation
    if (dashAnimRef.current !== null) {
      clearInterval(dashAnimRef.current);
      dashAnimRef.current = null;
    }

    // Clear existing overlay classes
    cy.elements(".ai-overlay").removeClass("ai-overlay");

    // Apply overlay to edges only (no node highlighting)
    const overlayEdges = store.overlayPathEdges;
    if (overlayEdges.size > 0) {
      overlayEdges.forEach((edgeKey) => {
        // edgeKey format: "from|to"
        const [from, to] = edgeKey.split("|");
        const edgeId = `${from}-${to}`;
        const edge = cy.getElementById(edgeId);
        if (edge.length > 0) {
          edge.addClass("ai-overlay");
        }
      });

      // Zoom in on highlighted edges
      const highlighted = cy.elements("edge.ai-overlay");
      if (highlighted.length > 0) {
        cy.animate({
          fit: { eles: highlighted, padding: 120 },
          duration: 450,
          easing: "ease-out-cubic",
        });
      }

      // Animate running dashes along highlighted edges
      let offset = 0;
      dashAnimRef.current = setInterval(() => {
        offset = (offset - 1) % 10;
        cy.elements("edge.ai-overlay").style("line-dash-offset", offset);
      }, 120);
    }
  }, [store.overlayPathEdges, store.overlayPathEdges.size]);

  // Apply focus state (focused nodes, edge, OR path) to elements
  const applyFocusState = React.useCallback(
    (cy: Core, progress: number) => {
      const activeFocusNodes = focusedNodes;
      const activeFocusEdge = focusedEdge;
      const activeFocusPath = focusedPath;
      const activeFocusDirection = focusDirection;
      const resetFocusVisuals = () => {
        // Also drop stale dimmed/highlighted classes: an element left
        // `highlighted` by the previous focus target would win the opacity
        // battle in the next one (both classes → dimmed, then re-lit by the
        // ".highlighted" pass) and appear stuck at full brightness.
        cy.elements().removeClass("dimmed highlighted");
        cy.elements().removeClass("focus-visible path-focus");
        // Clear transient inline opacity left by dimming animation.
        // Without this, some elements can stay visually dimmed after focus reset.
        cy.elements().forEach((element) => {
          element.style("opacity", "");
        });

        cy.edges().forEach((edge) => {
          const showLabel = edge.data("showLabel");
          const originalLabel = edge.data("label");
          const originalWidth = edge.data("edgeSize");

          edge.style({
            width: originalWidth,
            label: showLabel ? originalLabel : "",
            "font-size": "",
            "target-arrow-shape": "",
            "line-color": "",
            "target-arrow-color": "",
            color: "",
            "text-outline-color": "",
            "text-outline-width": "",
            "text-background-opacity": 0,
            "text-background-padding": 0,
            "text-background-color": "",
          });
        });

        cy.nodes().forEach((node) => {
          const baseSize =
            (node.data("nodeSize") as number) ?? NODE_BASE_MAX_SIZE;
          node.style({
            label: node.data("label"),
            "font-size": 14,
            width: baseSize,
            height: baseSize,
          });
        });
      };

      if (
        (activeFocusNodes.length === 0 &&
          !activeFocusEdge &&
          !activeFocusPath) ||
        progress <= 0
      ) {
        cy.elements().removeClass("dimmed highlighted");
        resetFocusVisuals();
        return;
      }

      // ── Path focus: dim everything outside the highlighted route (the
      // amber path-focus class marks the route itself). While the path is
      // being assembled by Cmd/Ctrl+clicks, the tip's visible outgoing
      // edges and their targets stay lit — the candidates for the next
      // click. ──
      if (activeFocusPath && activeFocusPath.length >= 2) {
        resetFocusVisuals();
        cy.elements().addClass("dimmed");

        let pathEles = cy.collection();
        activeFocusPath.forEach((nodeId, i) => {
          const node = cy.getElementById(nodeId);
          if (node.length > 0) pathEles = pathEles.union(node);
          if (i < activeFocusPath.length - 1) {
            const edge = cy.getElementById(
              `${nodeId}-${activeFocusPath[i + 1]}`,
            );
            if (edge.length > 0) {
              edge.addClass("path-focus");
              pathEles = pathEles.union(edge);
            }
          }
        });
        pathEles.removeClass("dimmed").addClass("highlighted");

        if (pathSelecting) {
          const tip = cy.getElementById(
            activeFocusPath[activeFocusPath.length - 1],
          );
          if (tip.length > 0) {
            const candidateEdges = tip
              .outgoers("edge")
              .filter(
                (edge: cytoscape.EdgeSingular) => !edge.hasClass("filtered"),
              );
            candidateEdges.removeClass("dimmed").addClass("highlighted");
            candidateEdges
              .targets()
              .removeClass("dimmed")
              .addClass("highlighted");

            // Re-weight the candidates against each other, exactly like the
            // outgoing edges of a node focus: thin-in-global-context edges
            // grow in the context of the tip node.
            let maxCandidate = 0;
            candidateEdges.forEach((edge: cytoscape.EdgeSingular) => {
              const w = Math.abs(edge.data("weight") as number);
              if (w > maxCandidate) maxCandidate = w;
            });
            candidateEdges.forEach((edge: cytoscape.EdgeSingular) => {
              const weight = Math.abs(edge.data("weight") as number);
              const relativeWeight =
                maxCandidate > 0 ? weight / maxCandidate : 0;
              const startWidth = edge.data("edgeSize") as number;
              const targetWidth = 2 + relativeWeight * 9;
              const baseValue = edge.data("baseValue") as string;
              const isFlipped =
                edge.source().position().x > edge.target().position().x;
              const showLabel =
                progress > 0.2 &&
                relativeWeight >= FOCUS_LABEL_MIN_VISIBLE_RATIO;
              edge.style({
                width: startWidth + (targetWidth - startWidth) * progress,
                label: showLabel
                  ? isFlipped
                    ? `← ${baseValue}`
                    : `${baseValue} →`
                  : "",
                "font-size": showLabel ? 13 + relativeWeight * 6 : "",
                "target-arrow-shape":
                  relativeWeight >= FOCUS_ARROW_MIN_VISIBLE_RATIO
                    ? "triangle"
                    : "none",
                "text-background-opacity": 0,
                "text-background-padding": 0,
              });
            });
          }
        }

        const dimOpacity = 1 - progress * store.focusDimStrength;
        cy.elements(".dimmed").forEach((element) => {
          element.style("opacity", dimOpacity);
        });
        cy.edges(".dimmed").forEach((dimmedEdge) => {
          dimmedEdge.style({
            label: "",
            "font-size": "",
            "target-arrow-shape": "none",
            "text-background-opacity": 0,
            "text-background-padding": 0,
          });
        });
        cy.elements(".highlighted").forEach((element) => {
          element.style("opacity", 1);
        });
        cy.nodes(".dimmed").forEach((nodeElement) => {
          nodeElement.style(
            "label",
            progress > 0.35 ? "" : nodeElement.data("label"),
          );
        });

        // Route weights along the highlighted edges. Each segment is
        // re-weighted the way a node focus would weight it: A→B's width is
        // its weight relative to A's strongest visible outgoing edge — the
        // same denominator the click-candidates of A use, so the segment
        // keeps the width it had when it was picked.
        const maxOutgoingCache = new Map<string, number>();
        const maxOutgoingOf = (node: cytoscape.NodeSingular): number => {
          const id = node.id();
          const cached = maxOutgoingCache.get(id);
          if (cached !== undefined) return cached;
          let max = 0;
          node.outgoers("edge").forEach((e: cytoscape.EdgeSingular) => {
            if (e.hasClass("filtered")) return;
            const w = Math.abs(e.data("weight") as number);
            if (w > max) max = w;
          });
          maxOutgoingCache.set(id, max);
          return max;
        };
        // Each segment keeps EXACTLY the look it had in the node focus it
        // was picked from (width, label, font size, arrowhead and their
        // visibility thresholds) — the class above only recolors it amber
        // and makes it dashed. The label pill just turns amber.
        const amberOutline = isDark ? "#fbbf24" : "#f59e0b";
        pathEles.edges().forEach((edge) => {
          const weight = Math.abs(edge.data("weight") as number);
          const maxOut = maxOutgoingOf(edge.source());
          const relativeWeight = maxOut > 0 ? weight / maxOut : 0;
          const startWidth = edge.data("edgeSize") as number;
          const targetWidth = 2 + relativeWeight * 9;
          const baseValue = edge.data("baseValue") as string;
          const isLoop = edge.source().id() === edge.target().id();
          const isFlipped =
            edge.source().position().x > edge.target().position().x;
          const showLabel =
            progress > 0.2 && relativeWeight >= FOCUS_LABEL_MIN_VISIBLE_RATIO;
          const showArrow =
            relativeWeight >= FOCUS_ARROW_MIN_VISIBLE_RATIO ||
            ((edge.data("showBaseArrow") as boolean) &&
              relativeWeight >= 0.14);
          edge.style({
            width: startWidth + (targetWidth - startWidth) * progress,
            label: showLabel
              ? isLoop
                ? baseValue
                : isFlipped
                  ? `← ${baseValue}`
                  : `${baseValue} →`
              : "",
            "font-size": showLabel ? 13 + relativeWeight * 9 : "",
            "target-arrow-shape": showArrow ? "triangle" : "none",
            "text-outline-color": amberOutline,
            "text-background-opacity": 0,
            "text-background-padding": 0,
          });
        });
        pathEles.nodes().forEach((nodeElement) => {
          nodeElement.style("font-size", 15);
        });
        return;
      }

      // ── Edge focus: dim everything except the edge and its endpoints ──
      if (activeFocusEdge) {
        const edge = cy.getElementById(
          `${activeFocusEdge.source}-${activeFocusEdge.target}`,
        );
        if (edge.length === 0) {
          cy.elements().removeClass("dimmed highlighted");
          resetFocusVisuals();
          return;
        }

        resetFocusVisuals();
        cy.elements().addClass("dimmed");
        const endpoints = edge.connectedNodes();
        // The focused edge must be visible even when the edge filter hides it
        edge.addClass("focus-visible");
        edge.removeClass("dimmed").addClass("highlighted");
        endpoints.removeClass("dimmed").addClass("highlighted");

        const dimOpacity = 1 - progress * store.focusDimStrength;
        cy.elements(".dimmed").forEach((element) => {
          element.style("opacity", dimOpacity);
        });
        cy.edges(".dimmed").forEach((dimmedEdge) => {
          dimmedEdge.style({
            label: "",
            "font-size": "",
            "target-arrow-shape": "none",
            "text-background-opacity": 0,
            "text-background-padding": 0,
          });
        });
        cy.elements(".highlighted").forEach((element) => {
          element.style("opacity", 1);
        });
        cy.nodes(".dimmed").forEach((nodeElement) => {
          nodeElement.style(
            "label",
            progress > 0.35 ? "" : nodeElement.data("label"),
          );
        });

        endpoints.forEach((nodeElement) => {
          const baseSize =
            (nodeElement.data("nodeSize") as number) ?? NODE_BASE_MAX_SIZE;
          const currentSize =
            baseSize + (NODE_FOCUS_ACTIVE_SIZE - baseSize) * progress;
          nodeElement.style({
            width: currentSize,
            height: currentSize,
            "font-size": 17,
          });
        });

        const isLoop = edge.data("isSelfLoop") as boolean;
        const baseValue = edge.data("baseValue") as string;
        const sourcePos = edge.source().position();
        const targetPos = edge.target().position();
        const isFlipped = sourcePos.x > targetPos.x;
        const label = isLoop
          ? baseValue
          : isFlipped
            ? `← ${baseValue}`
            : `${baseValue} →`;
        const startWidth = edge.data("edgeSize") as number;
        const targetWidth = Math.max(startWidth, 6);

        edge.style({
          width: startWidth + (targetWidth - startWidth) * progress,
          label: progress > 0.2 ? label : "",
          "font-size": 15,
          "target-arrow-shape": "triangle",
          "text-background-opacity": 0,
          "text-background-padding": 0,
        });
        return;
      }

      // ── Node focus ──
      const focusedSet = new Set(activeFocusNodes);

      // Union of the focused nodes' edges. Outside diff mode only ONE
      // direction is shown at a time (activeFocusDirection; self-loops
      // always shown) — diff mode keeps both directions.
      const allConnectedEdges = activeFocusNodes.reduce((acc, nodeId) => {
        const n = cy.getElementById(nodeId);
        if (n.length === 0) return acc;
        return acc.union(
          n.connectedEdges().filter((edge: cytoscape.EdgeSingular) => {
            if (edge.hasClass("filtered")) return false;
            if (isDifferential) return true;
            if (edge.source().id() === edge.target().id()) return true;
            return activeFocusDirection === "out"
              ? focusedSet.has(edge.source().id())
              : focusedSet.has(edge.target().id());
          }),
        ) as cytoscape.EdgeCollection;
      }, cy.collection() as cytoscape.EdgeCollection);
      const allConnectedNodes = allConnectedEdges.connectedNodes();

      // Classify edges relative to the focused set
      const incomingEdges = allConnectedEdges.filter(
        (edge: cytoscape.EdgeSingular) =>
          focusedSet.has(edge.target().id()) &&
          !focusedSet.has(edge.source().id()),
      ) as cytoscape.EdgeCollection;
      // Outgoing = any edge whose source is a focused node (excluding self-loops).
      // This includes edges to other focused nodes — they have a clear direction
      // and should be colored orange just like any other outgoing edge.
      const outgoingEdges = allConnectedEdges.filter(
        (edge: cytoscape.EdgeSingular) =>
          focusedSet.has(edge.source().id()) &&
          edge.source().id() !== edge.target().id(),
      ) as cytoscape.EdgeCollection;
      const selfLoops = allConnectedEdges.filter(
        (edge: cytoscape.EdgeSingular) =>
          edge.source().id() === edge.target().id(),
      ) as cytoscape.EdgeCollection;

      const connectedStrengthByNode = new Map<string, number>();
      allConnectedEdges.forEach((edge: cytoscape.EdgeSingular) => {
        const sourceId = edge.source().id();
        const targetId = edge.target().id();
        const weight = Math.abs(edge.data("weight") as number);

        if (focusedSet.has(sourceId) && !focusedSet.has(targetId)) {
          connectedStrengthByNode.set(
            targetId,
            (connectedStrengthByNode.get(targetId) ?? 0) + weight,
          );
        }
        if (focusedSet.has(targetId) && !focusedSet.has(sourceId)) {
          connectedStrengthByNode.set(
            sourceId,
            (connectedStrengthByNode.get(sourceId) ?? 0) + weight,
          );
        }
      });

      const maxConnectedStrength =
        connectedStrengthByNode.size > 0
          ? Math.max(...Array.from(connectedStrengthByNode.values()))
          : 0;

      const maxIncoming =
        incomingEdges.length > 0
          ? Math.max(
              ...incomingEdges.map((edge) =>
                Math.abs(edge.data("weight") as number),
              ),
            )
          : 0;
      const maxOutgoing =
        outgoingEdges.length > 0
          ? Math.max(
              ...outgoingEdges.map((edge) =>
                Math.abs(edge.data("weight") as number),
              ),
            )
          : 0;
      const maxVisibleWeight =
        allConnectedEdges.length > 0
          ? Math.max(
              ...allConnectedEdges.map((edge) =>
                Math.abs(edge.data("weight") as number),
              ),
            )
          : 0;
      const shouldShowFocusLabel = (weight: number) => {
        if (progress <= 0.2) return false;
        if (maxVisibleWeight <= 0) return false;
        return weight / maxVisibleWeight >= FOCUS_LABEL_MIN_VISIBLE_RATIO;
      };

      const createDirectionalLabel = (
        edge: cytoscape.EdgeSingular,
        baseValue: string,
      ) => {
        // Skip directional arrows in multi-select to avoid ambiguity
        if (activeFocusNodes.length > 1) return baseValue;
        const sourcePos = edge.source().position();
        const targetPos = edge.target().position();
        const isFlipped = sourcePos.x > targetPos.x;
        return isFlipped ? `← ${baseValue}` : `${baseValue} →`;
      };

      resetFocusVisuals();
      cy.elements().addClass("dimmed");
      activeFocusNodes.forEach((nodeId) => {
        cy.getElementById(nodeId).removeClass("dimmed").addClass("highlighted");
      });
      allConnectedEdges.removeClass("dimmed").addClass("highlighted");
      allConnectedNodes.removeClass("dimmed").addClass("highlighted");

      const dimOpacity = 1 - progress * store.focusDimStrength;
      cy.elements(".dimmed").forEach((element) => {
        element.style("opacity", dimOpacity);
      });
      cy.edges(".dimmed").forEach((edge) => {
        edge.style({
          label: "",
          "font-size": "",
          "target-arrow-shape": "none",
          "text-background-opacity": 0,
          "text-background-padding": 0,
        });
      });
      cy.elements(".highlighted").forEach((element) => {
        element.style("opacity", 1);
      });

      cy.nodes().forEach((nodeElement) => {
        const nodeId = nodeElement.id();
        const baseSize =
          (nodeElement.data("nodeSize") as number) ?? NODE_BASE_MAX_SIZE;

        let targetSize = NODE_FOCUS_DIMMED_SIZE;

        if (focusedSet.has(nodeId)) {
          targetSize = NODE_FOCUS_ACTIVE_SIZE;
        } else if (connectedStrengthByNode.has(nodeId)) {
          const strength = connectedStrengthByNode.get(nodeId) ?? 0;
          const relativeStrength =
            maxConnectedStrength > 0 ? strength / maxConnectedStrength : 0;
          targetSize =
            NODE_FOCUS_CONNECTED_MIN_SIZE +
            relativeStrength *
              (NODE_FOCUS_CONNECTED_MAX_SIZE - NODE_FOCUS_CONNECTED_MIN_SIZE);
        }

        const currentSize = baseSize + (targetSize - baseSize) * progress;
        nodeElement.style({ width: currentSize, height: currentSize });
      });

      incomingEdges.forEach((edge) => {
        const weight = Math.abs(edge.data("weight") as number);
        const relativeWeight = maxIncoming > 0 ? weight / maxIncoming : 0;
        const startWidth = edge.data("edgeSize") as number;
        const targetWidth = 2 + relativeWeight * 9;
        const currentWidth = startWidth + (targetWidth - startWidth) * progress;
        const baseValue = edge.data("baseValue") as string;
        const showFocusLabel = shouldShowFocusLabel(weight);
        const label = showFocusLabel
          ? createDirectionalLabel(edge, baseValue)
          : "";
        const showArrow =
          relativeWeight >= FOCUS_ARROW_MIN_VISIBLE_RATIO ||
          ((edge.data("showBaseArrow") as boolean) && relativeWeight >= 0.14);

        edge.style({
          width: currentWidth,
          label,
          "target-arrow-shape": showArrow ? "triangle" : "none",
          "font-size": showFocusLabel ? 13 + relativeWeight * 9 : "",
          "text-background-opacity": 0,
          "text-background-padding": 0,
        });
      });

      outgoingEdges.forEach((edge) => {
        const weight = Math.abs(edge.data("weight") as number);
        const relativeWeight = maxOutgoing > 0 ? weight / maxOutgoing : 0;
        const startWidth = edge.data("edgeSize") as number;
        const targetWidth = 2 + relativeWeight * 9;
        const currentWidth = startWidth + (targetWidth - startWidth) * progress;
        const baseValue = edge.data("baseValue") as string;
        const showFocusLabel = shouldShowFocusLabel(weight);
        const label = showFocusLabel
          ? createDirectionalLabel(edge, baseValue)
          : "";
        const showArrow =
          relativeWeight >= FOCUS_ARROW_MIN_VISIBLE_RATIO ||
          ((edge.data("showBaseArrow") as boolean) && relativeWeight >= 0.14);

        edge.style({
          width: currentWidth,
          label,
          "target-arrow-shape": showArrow ? "triangle" : "none",
          "font-size": showFocusLabel ? 13 + relativeWeight * 9 : "",
          "text-background-opacity": 0,
          "text-background-padding": 0,
        });
      });

      selfLoops.forEach((edge) => {
        const weight = Math.abs(edge.data("weight") as number);
        const baseValue = edge.data("baseValue") as string;
        const relativeWeight =
          maxVisibleWeight > 0 ? weight / maxVisibleWeight : 0;
        const showFocusLabel = shouldShowFocusLabel(weight);
        const label = showFocusLabel ? baseValue : "";
        const showArrow =
          relativeWeight >= FOCUS_ARROW_MIN_VISIBLE_RATIO ||
          ((edge.data("showBaseArrow") as boolean) && relativeWeight >= 0.14);
        edge.style({
          width: (edge.data("edgeSize") as number) + progress * 2,
          label,
          "target-arrow-shape": showArrow ? "triangle" : "none",
          "font-size": showFocusLabel ? 13 + relativeWeight * 9 : "",
          "text-background-opacity": 0,
          "text-background-padding": 0,
        });
      });

      // In multi-select mode, re-dim connected edges that didn't earn a focus
      // color class (gray/insignificant ones) so only meaningful transitions show.
      // Skip in diff mode — no focus classes are added there, so the check would
      // re-dim every connected edge and break the highlighting.
      if (activeFocusNodes.length > 1 && !isDifferential) {
        allConnectedEdges.forEach((edge: cytoscape.EdgeSingular) => {
          if (
            !edge.hasClass("focus-incoming") &&
            !edge.hasClass("focus-outgoing") &&
            !edge.hasClass("focus-loop")
          ) {
            edge.removeClass("highlighted").addClass("dimmed");
            edge.style({
              opacity: dimOpacity,
              label: "",
              "font-size": "",
              "target-arrow-shape": "none",
              "text-background-opacity": 0,
              "text-background-padding": 0,
            });
          }
        });
      }

      cy.nodes(".dimmed").forEach((nodeElement) => {
        nodeElement.style(
          "label",
          progress > 0.35 ? "" : nodeElement.data("label"),
        );
      });
      activeFocusNodes.forEach((nodeId) => {
        cy.getElementById(nodeId).style("font-size", 17);
      });
    },
    [
      focusedNodes,
      focusedEdge,
      focusedPath,
      focusDirection,
      pathSelecting,
      isDifferential,
    ],
  );
  applyFocusStateRef.current = applyFocusState;

  // Separate effect for edge filtering — avoids full graph recreation when
  // the filter changes, so focusedNodes and other visual state persist.
  // graphVersion keeps it in sync with cy rebuilds: a fresh instance carries
  // no `filtered` classes and no labels yet.
  React.useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.batch(() => {
      cy.edges().forEach((edge) => {
        const visible = isEdgeVisible(
          edge.source().id(),
          edge.target().id(),
          edge.data("normalizedWeight") as number,
        );
        if (visible) {
          edge.removeClass("filtered");
        } else {
          edge.addClass("filtered");
        }
      });
      allocateEdgeLabels(cy);
    });
    // Re-apply focus styling since the visible edge set changed
    if (focusProgressRef.current > 0) {
      applyFocusState(cy, focusProgressRef.current);
    }
  }, [isEdgeVisible, applyFocusState, allocateEdgeLabels, graphVersion]);

  // Focus animation
  React.useEffect(() => {
    if (typeof window === "undefined") return undefined;
    const cy = cyRef.current;
    if (!cy) return;

    if (focusAnimationFrameRef.current) {
      cancelAnimationFrame(focusAnimationFrameRef.current);
      focusAnimationFrameRef.current = null;
    }

    const target =
      focusedNodes.length > 0 || focusedEdge || focusedPath ? 1 : 0;

    const animate = () => {
      const current = focusProgressRef.current;
      const diff = target - current;

      if (Math.abs(diff) < 0.01) {
        focusProgressRef.current = target;
        applyFocusState(cy, target);
        focusAnimationFrameRef.current = null;
        return;
      }

      focusProgressRef.current = current + diff * 0.25;

      applyFocusState(cy, focusProgressRef.current);

      focusAnimationFrameRef.current = window.requestAnimationFrame(animate);
    };

    focusAnimationFrameRef.current = window.requestAnimationFrame(animate);

    return () => {
      if (focusAnimationFrameRef.current) {
        cancelAnimationFrame(focusAnimationFrameRef.current);
        focusAnimationFrameRef.current = null;
      }
    };
  }, [focusedNodes, focusedEdge, focusedPath, applyFocusState]);

  // Live-reflect the Focus Dimming slider while a node/edge/path is already
  // focused. Deliberately NOT routed through applyFocusState/the animation
  // effect above — restyling just the already-`.dimmed` elements directly
  // avoids re-triggering resetFocusVisuals/class churn or the rAF
  // cancel-and-reschedule dance on every slider tick during a drag.
  React.useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    if (focusProgressRef.current <= 0) return;
    const dimOpacity = 1 - focusProgressRef.current * store.focusDimStrength;
    cy.elements(".dimmed").forEach((element) => {
      element.style("opacity", dimOpacity);
    });
  }, [store.focusDimStrength]);

  // Apply the viewport requested by a view AFTER the commit it triggered has
  // fully settled — declared after the build/filter effects so it runs last
  // in the same commit (hidden-event changes rebuild the cy instance).
  // Double rAF: the build effect schedules its own fit in a rAF; running one
  // frame later guarantees the view's viewport wins the race.
  React.useEffect(() => {
    const pending = pendingViewViewportRef.current;
    if (!pending) return;
    pendingViewViewportRef.current = null;
    const cy = cyRef.current;
    if (!cy) return;

    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (cyRef.current !== cy) return;

        const viewport = pending.viewport;
        if (viewport && viewport !== "fit" && viewport !== "fit-focus") {
          cy.viewport({ zoom: viewport.zoom, pan: { ...viewport.pan } });
          return;
        }
        const focus = pending.focus;
        if (viewport !== "fit" && focus) {
          if (focus.type === "node") {
            fitNodeNeighborhood(
              cy,
              focus.id,
              isDifferential ? "both" : focus.direction === "in" ? "in" : "out",
            );
            return;
          }
          if (focus.type === "edge") {
            const edge = cy.getElementById(`${focus.source}-${focus.target}`);
            if (edge.length > 0) {
              cy.animate({
                fit: { eles: edge.union(edge.connectedNodes()), padding: 100 },
                duration: 350,
                easing: "ease-out-cubic",
              });
              return;
            }
          }
          if (focus.type === "path") {
            let eles = cy.collection();
            focus.nodes.slice(0, -1).forEach((from, i) => {
              const edge = cy.getElementById(`${from}-${focus.nodes[i + 1]}`);
              if (edge.length > 0) {
                eles = eles.union(edge).union(edge.connectedNodes());
              }
            });
            if (eles.length > 0) {
              cy.animate({
                fit: { eles, padding: 120 },
                duration: 350,
                easing: "ease-out-cubic",
              });
              return;
            }
          }
        }
        cy.fit(undefined, FIT_PADDING);
      });
    });
  });

  // ── Route badge: fetch backend stats for the focused path ──
  React.useEffect(() => {
    setPathStats(null);
    if (!host || !focusedPath || focusedPath.length < 2 || isDifferential) {
      return;
    }
    const nodes = focusedPath;
    // Debounce: while the user assembles the path click by click, compute
    // only after the clicks settle.
    const timer = window.setTimeout(() => {
      host
        .compute<PathStatsResult>("route_stats", { nodes })
        .then((result) => {
          // Ignore stale responses from a superseded path
          if (focusedPathRef.current === nodes) setPathStats(result);
        })
        .catch(() => {
          /* badge simply keeps its loading state */
        });
    }, 400);
    return () => window.clearTimeout(timer);
  }, [host, focusedPath, isDifferential]);

  // Reset the metric override when the edge weight changes — the default
  // follows the weight shown on the graph.
  React.useEffect(() => {
    setPathMetricOverride(null);
  }, [committedValueType]);

  // Apply the initial view (traitlet / URL hash) once, after the graph is
  // built and the backend layout has settled — fit-focus needs final
  // positions.
  const initialViewAppliedRef = React.useRef(false);
  React.useEffect(() => {
    if (initialViewAppliedRef.current) return;
    if (!initialView) return;
    if (graphVersion === 0 || isGraphLayoutLoading) return;
    initialViewAppliedRef.current = true;
    applyView(initialView);
  }, [initialView, graphVersion, isGraphLayoutLoading, applyView]);

  // Inline button style helper
  const btnStyle = (extra?: React.CSSProperties): React.CSSProperties => ({
    display: "flex",
    alignItems: "center",
    gap: 6,
    padding: "4px 10px",
    background: isDark ? "#1f2937" : "#f3f4f6",
    border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
    borderRadius: 6,
    cursor: "pointer",
    fontSize: 12,
    color: isDark ? "#f3f4f6" : "#111827",
    ...extra,
  });

  if (!store.hasData) {
    return (
      <div
        style={{
          display: "flex",
          height: "100%",
          width: "100%",
          alignItems: "center",
          justifyContent: "center",
          color: isDark ? "#9ca3af" : "#6b7280",
        }}
      >
        {store.isLoading ? "Loading..." : "No data available"}
      </div>
    );
  }

  return (
    <div
      style={{
        position: "relative",
        marginBottom: 8,
        display: "flex",
        height: "100%",
        minHeight: 0,
        width: "100%",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      <div
        style={{
          position: "relative",
          height: "100%",
          width: "100%",
          borderRadius: 8,
          border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
          background: isDark ? "#111827" : "#ffffff",
        }}
      >
        <div ref={containerRef} style={{ height: "100%", width: "100%" }} />
        <div
          style={{
            pointerEvents: "none",
            position: "absolute",
            inset: 0,
            zIndex: 15,
            background: isDark ? "#111827" : "#ffffff",
            transition: "opacity 0.2s",
            opacity: sceneMaskOpacity,
          }}
        />
      </div>

      <div style={{ position: "absolute", left: 16, top: 16, zIndex: 20 }}>
        <button
          onClick={() => setSearchOpen((current) => !current)}
          style={btnStyle()}
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="11" cy="11" r="8"/>
            <path d="m21 21-4.35-4.35"/>
          </svg>
          <span style={{ fontSize: 12 }}>Search event</span>
        </button>
        {searchOpen && (
          <div
            style={{
              position: "absolute",
              left: 0,
              top: 40,
              zIndex: 30,
              width: 360,
              borderRadius: 6,
              border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
              background: isDark ? "#1f2937" : "#ffffff",
              boxShadow: "0 4px 16px rgba(0,0,0,0.18)",
            }}
          >
            <SearchBar
              onSearch={() => undefined}
              onClear={() => undefined}
              onClose={() => setSearchOpen(false)}
              onSelect={focusNodeFromSearch}
              events={graphSearchEvents}
              showAllOnOpen
              isDark={isDark}
              onToggleVisibility={(id) => store.toggleEventVisibility(id)}
              onTogglePin={(id) => store.toggleEventPin(id)}
              getEventState={(id) => {
                const e = store.events.get(id);
                return { isHidden: e?.isHidden ?? false, isPinned: e?.isPinned ?? false };
              }}
              eventCounts={eventCounts}
              eventCountsG1={eventCountsG1}
              eventCountsG2={eventCountsG2}
            />
          </div>
        )}
      </div>

      {/* GraphView pills: named visual presets + return to Default */}
      {(views?.length ?? 0) > 0 && (
        <div
          style={{
            position: "absolute",
            top: 10,
            left: "50%",
            transform: "translateX(-50%)",
            zIndex: 20,
            display: "flex",
            gap: 6,
            flexWrap: "wrap",
            justifyContent: "center",
            maxWidth: "55%",
          }}
        >
          <button
            onClick={resetToDefaultView}
            style={btnStyle({
              padding: "3px 10px",
              borderRadius: 999,
              ...(activeViewName === null
                ? {
                    background: isDark ? "#374151" : "#fef3c7",
                    borderColor: isDark ? "#6b7280" : "#f59e0b",
                    fontWeight: 600,
                  }
                : {}),
            })}
          >
            Default
          </button>
          {views!
            .filter((view) => view.name)
            .map((view) => (
              <button
                key={view.name}
                onClick={() => applyView(view)}
                style={btnStyle({
                  padding: "3px 10px",
                  borderRadius: 999,
                  ...(activeViewName === view.name
                    ? {
                        background: isDark ? "#374151" : "#fef3c7",
                        borderColor: isDark ? "#6b7280" : "#f59e0b",
                        fontWeight: 600,
                      }
                    : {}),
                })}
              >
                {view.name}
              </button>
            ))}
        </div>
      )}

      {/* Route badge: stats of the focused path + metric selector */}
      {focusedPath &&
        focusedPath.length >= 2 &&
        !isDifferential &&
        host !== null &&
        (() => {
          const metric =
            pathMetricOverride ?? defaultPathMetric(committedValueType);
          let value: string;
          if (!pathStats) {
            value = "…";
          } else if (metric === "proba_out") {
            value = `${(pathStats.proba * 100).toFixed(2)}%`;
          } else if (metric === "unique_paths") {
            value = `${formatNumber(pathStats.unique_paths)} (${(
              pathStats.unique_paths_share * 100
            ).toFixed(1)}%)`;
          } else if (metric === "count") {
            value = formatNumber(pathStats.occurrences);
          } else if (metric === "avg_per_path") {
            value = pathStats.avg_per_path.toFixed(2);
          } else if (metric === "time_median") {
            value =
              pathStats.time_median != null
                ? formatTime(pathStats.time_median)
                : "—";
          } else {
            value =
              pathStats.time_q95 != null
                ? formatTime(pathStats.time_q95)
                : "—";
          }
          const options = (
            [
              "unique_paths",
              "count",
              "avg_per_path",
              "time_median",
              "time_q95",
              "proba_out",
            ] as PathMetric[]
          ).map((m) => (
            <option key={m} value={m}>
              {PATH_METRIC_LABELS[m]}
            </option>
          ));
          return (
            <div
              style={{
                position: "absolute",
                top: 52,
                right: 16,
                zIndex: 19,
                display: "flex",
                alignItems: "center",
                gap: 8,
                padding: "4px 10px",
                borderRadius: 6,
                border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
                background: isDark
                  ? "rgba(31,41,55,0.92)"
                  : "rgba(255,255,255,0.92)",
                fontSize: 12,
                color: isDark ? "#f3f4f6" : "#111827",
              }}
            >
              <span style={{ fontWeight: 600 }}>{value}</span>
              <select
                value={metric}
                onChange={(e) =>
                  setPathMetricOverride(e.target.value as PathMetric)
                }
                style={{
                  fontSize: 11,
                  color: isDark ? "#9ca3af" : "#6b7280",
                  background: "transparent",
                  border: "none",
                  outline: "none",
                  cursor: "pointer",
                }}
              >
                {options}
              </select>
            </div>
          );
        })()}

      {/* Contextual legend + coverage indicator */}
      <div style={{ position: "absolute", left: 16, bottom: 12, zIndex: 20 }}>
        <GraphLegend
          isDark={isDark}
          // Diff mode keeps its red/blue edge colors even in focus, so the
          // diff legend stays; the in/out focus legend is normal-mode only.
          mode={
            isDifferential
              ? "diff"
              : focusedNodes.length > 0
                ? "focus"
                : "normal"
          }
          valuesType={committedValueType}
          coverage={coverage}
          diffLabel1={diffLabels.value1Label}
          diffLabel2={diffLabels.value2Label}
          widgetId={sharedPreferencesId}
          focusDirection={focusDirection}
          // Exported HTML starts with the legend collapsed — reports embed
          // several widgets and an open legend eats canvas space.
          defaultCollapsed={host === null}
        />
      </div>

      {/* Ego view: modal neighborhood expansion of one event */}
      {egoNode && (
        <EgoView
          node={egoNode}
          edges={edgeList}
          counts={store.transitionCounts}
          formatValue={formatValue}
          isDark={isDark}
          isDifferential={isDifferential}
          getDiffBreakdown={(source, target) =>
            store.getDiffCellBreakdown(source, target)
          }
          diffLabels={diffLabels}
          onNavigate={setEgoNode}
          onClose={() => setEgoNode(null)}
        />
      )}

      {/* One-time hint explaining the default Auto edge filter */}
      {!edgeFilterHintDismissed && edgeFilter.mode === "topk" && (
        <div
          style={{
            position: "absolute",
            bottom: 50,
            right: 12,
            zIndex: 21,
            maxWidth: 240,
            padding: "8px 10px",
            borderRadius: 6,
            border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
            background: isDark ? "rgba(31,41,55,0.96)" : "rgba(255,255,255,0.96)",
            boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
            fontSize: 11,
            lineHeight: "15px",
            color: isDark ? "#d1d5db" : "#4b5563",
          }}
        >
          <div style={{ marginBottom: 6 }}>
            By default, the Edge filter shows only each node's top{" "}
            {DEFAULT_TOP_K} strongest edges — not the full graph. Switch to
            Manual for a custom weight range.
          </div>
          <button
            onClick={dismissEdgeFilterHint}
            style={{
              background: "none",
              border: "none",
              padding: 0,
              fontSize: 11,
              fontWeight: 600,
              color: isDark ? "#e5e7eb" : "#374151",
              cursor: "pointer",
              textDecoration: "underline",
            }}
          >
            Don't show again
          </button>
        </div>
      )}

      {/* Edge filter: per-node top-k ("auto") or a manual weight range */}
      <div
        style={{
          pointerEvents: "none",
          position: "absolute",
          bottom: 12,
          right: 12,
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}
      >
        <span
          style={{
            fontSize: 11,
            fontWeight: 500,
            color: isDark ? "#9ca3af" : "#6b7280",
          }}
        >
          Edge filter
        </span>
        <div
          style={{
            pointerEvents: "auto",
            display: "flex",
            alignItems: "center",
            gap: 8,
          }}
        >
          {edgeFilter.mode === "topk" ? (
            <div
              data-rete-tooltip={`showing top ${edgeFilter.k} strongest edges per node`}
              data-rete-tooltip-pos="top"
              style={btnStyle({
                cursor: "default",
                gap: 4,
                padding: "2px 6px",
              })}
            >
              <button
                onClick={() =>
                  handleEdgeFilterChange({
                    mode: "topk",
                    k: Math.max(TOP_K_MIN, edgeFilter.k - 1),
                  })
                }
                disabled={edgeFilter.k <= TOP_K_MIN}
                aria-label="Fewer edges per node"
                style={btnStyle({
                  padding: "0 6px",
                  opacity: edgeFilter.k <= TOP_K_MIN ? 0.4 : 1,
                })}
              >
                −
              </button>
              <span style={{ minWidth: 14, textAlign: "center" }}>
                {edgeFilter.k}
              </span>
              <button
                onClick={() =>
                  handleEdgeFilterChange({
                    mode: "topk",
                    k: Math.min(TOP_K_MAX, edgeFilter.k + 1),
                  })
                }
                disabled={edgeFilter.k >= TOP_K_MAX}
                aria-label="More edges per node"
                style={btnStyle({
                  padding: "0 6px",
                  opacity: edgeFilter.k >= TOP_K_MAX ? 0.4 : 1,
                })}
              >
                +
              </button>
            </div>
          ) : (
            <div style={{ width: 224 }}>
              {(() => {
                // Probabilities are stored as absolute values (normalized
                // against 1) only in normal mode; in diff mode |Δp| is
                // normalized against the graph maximum like any other type,
                // so the slider converts through maxWeight to keep showing
                // absolute percentages.
                const isAbsoluteScale = isProbability && !isDifferential;
                return (
                  <RangeSlider
                    min={0}
                    max={isAbsoluteScale ? 1 : maxWeight}
                    step={isProbability ? 0.001 : maxWeight > 1000 ? 10 : 1}
                    value={
                      isAbsoluteScale
                        ? edgeFilter.range
                        : (edgeFilter.range.map((v) => v * maxWeight) as [
                            number,
                            number,
                          ])
                    }
                    onChange={(newValues) => {
                      const range: [number, number] = isAbsoluteScale
                        ? newValues
                        : [newValues[0] / maxWeight, newValues[1] / maxWeight];
                      handleEdgeFilterChange({ mode: "range", range });
                    }}
                    variant="mini"
                    formatValue={(v) =>
                      isProbability
                        ? `${(v * 100).toFixed(1)}%`
                        : formatNumber(v)
                    }
                    // Log for probabilities too: most edges sit near zero,
                    // so a linear track wipes out too many edges per pixel
                    // of thumb travel. 0.005 keeps the usable range at
                    // 0.5%–100%; other types span exactly the data range
                    // (smallest nonzero weight .. max) so no part of the
                    // track is dead space.
                    scale="log"
                    logMin={
                      isProbability
                        ? 0.005
                        : minNonzeroWeight > 0
                          ? minNonzeroWeight
                          : undefined
                    }
                  />
                );
              })()}
            </div>
          )}
          <button
            onClick={() =>
              handleEdgeFilterChange(
                edgeFilter.mode === "topk"
                  ? { mode: "range", range: [0, 1] }
                  : { mode: "topk", k: DEFAULT_TOP_K },
              )
            }
            aria-label={
              edgeFilter.mode === "topk"
                ? "Auto: strongest edges per node. Click for manual weight range."
                : "Manual weight range. Click for auto (strongest edges per node)."
            }
            data-rete-tooltip={
              edgeFilter.mode === "topk"
                ? "Auto: strongest edges per node · click for manual range"
                : "Manual weight range · click for auto"
            }
            data-rete-tooltip-pos="top"
            style={btnStyle({ padding: "4px 8px" })}
          >
            {edgeFilter.mode === "topk" ? "Auto" : "Manual"}
          </button>
        </div>
      </div>

      <div
        style={{
          position: "absolute",
          right: 56,
          top: 10,
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}
      >
        {focusedNodes.length === 1 && (
          <button
            onClick={() => setEgoNode(focusedNodes[0])}
            aria-label={`Ego view: ${focusedNodes[0]}`}
            data-rete-tooltip="Ego view: expand neighborhood (in & out)"
            style={btnStyle({
              width: 32,
              height: 32,
              padding: 0,
              justifyContent: "center",
            })}
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              {/* "network", rotated 90° CCW and mirrored: two more nodes
                  added to the left of the center so it reads as one focal
                  node with neighbors on both sides, not just branching down. */}
              <rect x="9" y="9" width="6" height="6" rx="1"/>
              <rect x="18" y="2" width="4" height="4" rx="1"/>
              <rect x="18" y="18" width="4" height="4" rx="1"/>
              <rect x="2" y="2" width="4" height="4" rx="1"/>
              <rect x="2" y="18" width="4" height="4" rx="1"/>
              <path d="M15 9l3-3"/>
              <path d="M15 15l3 3"/>
              <path d="M9 9l-3-3"/>
              <path d="M9 15l-3 3"/>
            </svg>
          </button>
        )}
        {focusedNodes.length === 1 && (
          <button
            onClick={() => {
              const containerWidth = containerRef.current?.clientWidth ?? 0;
              const nodeId = focusedNodes[0];
              setColorPicker((current) => {
                if (current?.kind === "node" && current.nodeId === nodeId) {
                  return null;
                }
                return {
                  kind: "node",
                  nodeId,
                  x: Math.max(12, containerWidth - 176),
                  y: 48,
                };
              });
            }}
            aria-label={`Color node: ${focusedNodes[0]}`}
            data-rete-tooltip="Color node"
            style={btnStyle({
              width: 32,
              height: 32,
              padding: 0,
              justifyContent: "center",
            })}
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="13.5" cy="6.5" r="1.1" fill="currentColor" stroke="none"/>
              <circle cx="17.5" cy="10.5" r="1.1" fill="currentColor" stroke="none"/>
              <circle cx="8.5" cy="7.5" r="1.1" fill="currentColor" stroke="none"/>
              <circle cx="6.5" cy="12.5" r="1.1" fill="currentColor" stroke="none"/>
              <path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.93 0 1.65-.75 1.65-1.69 0-.44-.18-.83-.44-1.13-.29-.29-.44-.65-.44-1.12a1.64 1.64 0 0 1 1.67-1.67h1.99c3.05 0 5.56-2.5 5.56-5.56C22 6.01 17.46 2 12 2z"/>
            </svg>
          </button>
        )}
        {focusedEdge && (
          <button
            onClick={() => {
              const containerWidth = containerRef.current?.clientWidth ?? 0;
              const { source, target } = focusedEdge;
              setColorPicker((current) => {
                if (
                  current?.kind === "edge" &&
                  current.source === source &&
                  current.target === target
                ) {
                  return null;
                }
                return {
                  kind: "edge",
                  source,
                  target,
                  x: Math.max(12, containerWidth - 176),
                  y: 48,
                };
              });
            }}
            title={`Color edge: ${focusedEdge.source} → ${focusedEdge.target}`}
            style={btnStyle()}
          >
            Color Edge
          </button>
        )}
        <button
          onClick={() => {
            const view = buildCurrentView();
            // Static export: a shareable URL with the view in the hash.
            // Live widget: a JSON snippet for views=[...] / export links.
            const text =
              host === null
                ? `${window.location.href.split("#")[0]}#view=${encodeGraphView(view)}`
                : JSON.stringify(view);
            copyTextToClipboard(text);
            setCopiedView(true);
            window.setTimeout(() => setCopiedView(false), 1500);
          }}
          aria-label="Copy view link"
          data-rete-tooltip={
            host === null
              ? copiedView
                ? "Copied!"
                : "Copy view link (URL with current focus & filters)"
              : copiedView
                ? "Copied. Paste it to the views arg"
                : "Copy current view"
          }
          style={btnStyle({
            width: 32,
            height: 32,
            padding: 0,
            justifyContent: "center",
          })}
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M12.034 12.681a.498.498 0 0 1 .647-.647l9 3.5a.5.5 0 0 1-.033.943l-3.444 1.068a1 1 0 0 0-.66.66l-1.067 3.443a.5.5 0 0 1-.943.033z"/>
            <path d="M5 3a2 2 0 0 0-2 2"/>
            <path d="M19 3a2 2 0 0 1 2 2"/>
            <path d="M5 21a2 2 0 0 1-2-2"/>
            <path d="M9 3h1"/>
            <path d="M9 21h2"/>
            <path d="M14 3h1"/>
            <path d="M3 9v1"/>
            <path d="M21 9v2"/>
            <path d="M3 14v1"/>
          </svg>
        </button>
        <button
          onClick={() => cyRef.current?.fit(undefined, FIT_PADDING)}
          aria-label="Fit graph to canvas"
          data-rete-tooltip="Fit graph to canvas"
          style={btnStyle({
            width: 32,
            height: 32,
            padding: 0,
            justifyContent: "center",
          })}
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M3 7V5a2 2 0 0 1 2-2h2"/>
            <path d="M17 3h2a2 2 0 0 1 2 2v2"/>
            <path d="M21 17v2a2 2 0 0 1-2 2h-2"/>
            <path d="M7 21H5a2 2 0 0 1-2-2v-2"/>
          </svg>
        </button>
      </div>

      {tooltip && !colorPicker && (
        <div
          style={{
            pointerEvents: "none",
            position: "absolute",
            left: tooltip.position.x + (tooltipSide === "left" ? -12 : 12),
            top: tooltip.position.y + 12,
            transform: tooltipSide === "left" ? "translateX(-100%)" : undefined,
          }}
        >
          {tooltip.type === "node" ? (
            <DiffBreakdownTooltip
              title={<span>{tooltip.eventId}</span>}
              subtitle="share of event in group"
              segmentName={diffLabels.segmentName}
              value1Label={diffLabels.value1Label}
              value2Label={diffLabels.value2Label}
              group1Value={tooltip.group1Value}
              group2Value={tooltip.group2Value}
              diffValue={tooltip.diffValue}
              isDark={isDark}
            />
          ) : isDifferential ? (
            <DiffBreakdownTooltip
              title={
                <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span>{tooltip.from}</span>
                  <span style={{ color: isDark ? "#9ca3af" : "#6b7280" }}>→</span>
                  <span>{tooltip.to}</span>
                </span>
              }
              segmentName={diffLabels.segmentName}
              value1Label={diffLabels.value1Label}
              value2Label={diffLabels.value2Label}
              group1Value={tooltip.group1Value}
              group2Value={tooltip.group2Value}
              diffValue={tooltip.diffValue}
              isDark={isDark}
            />
          ) : tooltip.type === "edge" ? (
            <div
              style={{
                borderRadius: 6,
                border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
                background: isDark ? "#1f2937" : "#ffffff",
                padding: "8px 12px",
                fontSize: 12,
                boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ color: isDark ? "#9ca3af" : "#6b7280" }}>{tooltip.from}</span>
                <span>→</span>
                <span style={{ color: isDark ? "#9ca3af" : "#6b7280" }}>{tooltip.to}</span>
                <span
                  style={{
                    marginLeft: 4,
                    fontWeight: 500,
                    color: isDifferential
                      ? tooltip.forwardWeight > 0
                        ? "#ef4444"
                        : "#3b82f6"
                      : undefined,
                  }}
                >
                  {isDifferential && tooltip.forwardWeight > 0 ? "+" : ""}
                  {formatValue(tooltip.forwardWeight)}
                </span>
              </div>
            </div>
          ) : null}
        </div>
      )}

      {colorPicker && (
        <div
          style={{
            position: "absolute",
            zIndex: 50,
            display: "flex",
            flexDirection: "column",
            gap: 8,
            borderRadius: 8,
            border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
            background: isDark ? "#1f2937" : "#ffffff",
            padding: 8,
            boxShadow: "0 4px 16px rgba(0,0,0,0.18)",
            left: Math.min(
              colorPicker.x,
              (containerRef.current?.clientWidth ?? 0) - 160,
            ),
            top: Math.min(
              colorPicker.y,
              (containerRef.current?.clientHeight ?? 0) - 100,
            ),
          }}
        >
          <div
            style={{
              marginBottom: 4,
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 8,
            }}
          >
            <span
              style={{
                fontSize: 12,
                fontWeight: 500,
                color: isDark ? "#9ca3af" : "#6b7280",
              }}
            >
              {edgeColorPicker
                ? "Color Edge"
                : `Color Node${nodeColorPicker ? `: ${nodeColorPicker.nodeId}` : ""}`}
            </span>
            <button
              onClick={() => setColorPicker(null)}
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                width: 16,
                height: 16,
                background: "transparent",
                border: "none",
                cursor: "pointer",
                color: isDark ? "#9ca3af" : "#6b7280",
                padding: 0,
              }}
            >
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M18 6 6 18M6 6l12 12"/>
              </svg>
            </button>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(5, 1fr)",
              gap: 4,
            }}
          >
            {PALETTE_COLORS.map((color) => (
              <button
                key={color.name}
                style={{
                  width: 24,
                  height: 24,
                  borderRadius: "50%",
                  border: `1px solid ${isDark ? "#374151" : "#e5e7eb"}`,
                  backgroundColor: `rgb(${color.value})`,
                  cursor: "pointer",
                  transition: "transform 0.1s",
                  padding: 0,
                }}
                onClick={() => {
                  if (edgeColorPicker) {
                    setEdgeColor(
                      edgeColorPicker.source,
                      edgeColorPicker.target,
                      color.value,
                    );
                  } else if (nodeColorPicker) {
                    setNodeColor(nodeColorPicker.nodeId, color.value);
                  }
                  setColorPicker(null);

                  const cy = cyRef.current;
                  if (cy) {
                    if (edgeColorPicker) {
                      const edgeId = `${edgeColorPicker.source}-${edgeColorPicker.target}`;
                      const edge = cy.getElementById(edgeId);
                      if (edge.length > 0 && !isDifferential) {
                        const newColor = applyAlphaColor(
                          color.value,
                          edge.data("baseAlpha"),
                        );
                        edge.style("line-color", newColor);
                        edge.style("target-arrow-color", newColor);
                        edge.data("edgeColor", newColor);
                        edge.data("baseColor", color.value);
                      }
                    } else if (nodeColorPicker) {
                      const nodeColor = normalizeRgbColor(color.value);
                      const node = cy.getElementById(nodeColorPicker.nodeId);
                      if (node.length > 0) {
                        node.data("nodeColor", nodeColor);
                        node.data("nodeImage", getNodeImageDataUri(nodeColor));
                        node.style(
                          "background-image",
                          getNodeImageDataUri(nodeColor),
                        );
                      }
                    }
                  }
                }}
                title={color.name}
              />
            ))}
          </div>
          <button
            onClick={() => {
              if (edgeColorPicker) {
                removeEdgeColor(edgeColorPicker.source, edgeColorPicker.target);
              } else if (nodeColorPicker) {
                removeNodeColor(nodeColorPicker.nodeId);
              }
              setColorPicker(null);

              const cy = cyRef.current;
              if (cy) {
                if (edgeColorPicker) {
                  const edgeId = `${edgeColorPicker.source}-${edgeColorPicker.target}`;
                  const edge = cy.getElementById(edgeId);
                  if (edge.length > 0 && !isDifferential) {
                    const defaultColor = isDark
                      ? "156, 163, 175"
                      : "75, 85, 99";
                    const newColor = applyAlphaColor(
                      defaultColor,
                      edge.data("baseAlpha"),
                    );
                    edge.style("line-color", newColor);
                    edge.style("target-arrow-color", newColor);
                    edge.data("edgeColor", newColor);
                    edge.data("baseColor", defaultColor);
                  }
                } else if (nodeColorPicker) {
                  const node = cy.getElementById(nodeColorPicker.nodeId);
                  if (node.length > 0) {
                    node.data("nodeColor", DEFAULT_NODE_COLOR);
                    node.data(
                      "nodeImage",
                      getNodeImageDataUri(DEFAULT_NODE_COLOR),
                    );
                    node.style(
                      "background-image",
                      getNodeImageDataUri(DEFAULT_NODE_COLOR),
                    );
                  }
                }
              }
            }}
            style={btnStyle({ marginTop: 4, padding: "2px 8px", fontSize: 12, justifyContent: "center" })}
          >
            Reset
          </button>
        </div>
      )}

      {(store.isUpdating || isGraphLayoutLoading) && (
        <div
          style={{
            pointerEvents: "none",
            position: "absolute",
            inset: 0,
            zIndex: 10,
            display: "flex",
            height: "100%",
            width: "100%",
            alignItems: "center",
            justifyContent: "center",
            background: isDark ? "rgba(17,24,39,0.5)" : "rgba(255,255,255,0.5)",
            backdropFilter: "blur(4px)",
          }}
        >
          <div
            style={{
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              gap: 8,
            }}
          >
            <div
              style={{
                width: 32,
                height: 32,
                borderRadius: "50%",
                border: `2px solid ${isDark ? "#374151" : "#e5e7eb"}`,
                borderTopColor: "var(--retentioneering-yellow)",
                animation: "spin 0.8s linear infinite",
              }}
            />
            <div
              style={{
                fontSize: 12,
                fontWeight: 500,
                color: isDark ? "#9ca3af" : "#6b7280",
              }}
            >
              Updating graph...
            </div>
          </div>
        </div>
      )}

      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        [data-rete-tooltip] { position: relative; }
        [data-rete-tooltip]:hover::after {
          content: attr(data-rete-tooltip);
          position: absolute;
          top: calc(100% + 6px);
          right: 0;
          background: ${isDark ? "#1f2937" : "#ffffff"};
          color: ${isDark ? "#f3f4f6" : "#111827"};
          border: 1px solid ${isDark ? "#374151" : "#e5e7eb"};
          box-shadow: 0 2px 8px rgba(0,0,0,0.15);
          font-size: 11px;
          line-height: 1;
          padding: 5px 8px;
          border-radius: 4px;
          white-space: nowrap;
          z-index: 100;
          pointer-events: none;
        }
        [data-rete-tooltip][data-rete-tooltip-pos="top"]:hover::after {
          top: auto;
          bottom: calc(100% + 6px);
        }
      `}</style>
    </div>
  );
});
