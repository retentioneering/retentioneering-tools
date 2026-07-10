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
import { RangeSlider } from "./RangeSlider";
import { SearchBar } from "./SearchBar";
import { DiffBreakdownTooltip } from "./DiffBreakdownTooltip";

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
const FOCUS_COLOR_MIN_VISIBLE_RATIO = 0.18;
const DEFAULT_LOOP_DIRECTION = "-90deg";

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
  // persistence: edge weight filter [min, max], normalized to 0..1
  initialEdgeFilter?: [number, number] | null;
  onEdgeFilterChange?: (filter: [number, number]) => void;
  // persistence: canvas zoom/pan
  initialViewport?: StoredViewport | null;
  onViewportChange?: (viewport: StoredViewport) => void;
  // ref that receives a fit() function — call to fit the graph to the canvas
  fitRef?: React.MutableRefObject<(() => void) | undefined>;
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
}: TransitionGraphProps) {
  // internal state for uncontrolled valuesType
  const [internalValuesType, setInternalValuesType] = React.useState<MatrixValueType>(valuesTypeProp ?? DEFAULT_VALUE_TYPE);
  React.useEffect(() => { if (valuesTypeProp !== undefined) setInternalValuesType(valuesTypeProp); }, [valuesTypeProp]);
  const currentValuesType = valuesTypeProp !== undefined ? valuesTypeProp : internalValuesType;
  const handleValuesTypeChange = (v: MatrixValueType) => { setInternalValuesType(v); onValuesTypeChange?.(v); };

  const effectiveWidgetId = widgetId ?? "default";
  const { getEdgeColor, setEdgeColor, removeEdgeColor } = useEdgeColors(effectiveWidgetId);
  const { getNodeColor, setNodeColor, removeNodeColor } = useNodeColors(effectiveWidgetId);
  const {
    positions: savedPositions,
    savePositions,
    hasSavedPositions,
  } = useNodePositions(effectiveWidgetId);
  const { data: graphLayoutData, isLoading: isGraphLayoutLoading } =
    useGraphLayout(host);

  const [edgeThreshold, setEdgeThreshold] = React.useState(() =>
    initialEdgeFilter && initialEdgeFilter.length === 2
      ? `${initialEdgeFilter[0]},${initialEdgeFilter[1]}`
      : "0,1",
  );
  const handleEdgeThresholdChange = React.useCallback((value: [number, number]) => {
    setEdgeThreshold(`${value[0].toFixed(3)},${value[1].toFixed(3)}`);
    onEdgeFilterChange?.([value[0], value[1]]);
  }, [onEdgeFilterChange]);

  const requestedValueType = currentValuesType;
  const isDark =
    theme === "dark" ||
    (theme !== "light" &&
      typeof window !== "undefined" &&
      window.matchMedia("(prefers-color-scheme: dark)").matches);
  const neutralFocusColor = isDark ? "148, 163, 184" : "107, 114, 128";
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
  const sceneTransitionTimeoutRef = React.useRef<ReturnType<
    typeof setTimeout
  > | null>(null);
  const hasShownInitialSceneRef = React.useRef(false);
  const isDifferential =
    !!committedDiffSegment || committedMatrixType === "differential";
  const graphLayoutResult = graphLayoutData?.result ?? null;
  const tooltipSide = React.useMemo<"left" | "right">(() => {
    if (!tooltip) return "right";
    const containerWidth = containerRef.current?.clientWidth ?? 0;
    if (containerWidth <= 0) return "right";
    return tooltip.position.x > containerWidth * 0.62 ? "left" : "right";
  }, [tooltip]);
  const edgeColorPicker = colorPicker?.kind === "edge" ? colorPicker : null;
  const nodeColorPicker = colorPicker?.kind === "node" ? colorPicker : null;

  // Parse edge threshold from state parameter
  const edgeThresholdParsed = React.useMemo(() => {
    const parts = edgeThreshold.split(",").map((s) => parseFloat(s));
    if (parts.length === 2 && parts.every((n) => !isNaN(n))) {
      return [
        Math.max(0, Math.min(1, parts[0])),
        Math.max(0, Math.min(1, parts[1])),
      ] as [number, number];
    }
    return [0, 1] as [number, number];
  }, [edgeThreshold]);

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
    savePositions(positionsRef.current);       // localStorage
    onPositionsChange?.(positionsRef.current); // → Python traitlet / file
  }, [savePositions, onPositionsChange]);

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

  const focusNodeFromSearch = React.useCallback(
    (eventId: string) => {
      const cy = cyRef.current;
      if (!cy) return;

      const node = cy.getElementById(eventId);
      if (node.length === 0) return;

      cy.animate({
        center: { eles: node },
        zoom: Math.max(Math.min(cy.zoom() * 1.15, 2.2), 1.15),
        duration: 350,
        easing: "ease-out-cubic",
      });

      // Focus the node persistently instead of a temporary highlight
      setFocusedNodes([eventId]);
      setSearchOpen(false);
    },
    [setFocusedNodes],
  );

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

  // Build / rebuild Cytoscape graph
  React.useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    if (!store.hasData || visibleEvents.length === 0) {
      cyRef.current?.destroy();
      cyRef.current = null;
      return;
    }

    // Use logarithmic normalization for better handling of huge differences
    const normalizeWeight = (weight: number): number => {
      const magnitude = isDifferential ? Math.abs(weight) : weight;

      // For probability types (0-1), use absolute value directly
      if (isProbability) {
        return Math.max(0, Math.min(1, magnitude));
      }

      if (maxWeight <= 0) return 0;

      // Linear scale to maxWeight to ensure 0 is the baseline
      return magnitude / maxWeight;
    };

    // Collect all edges first to determine top 10
    const edgesToAdd: Array<{
      source: string;
      target: string;
      weight: number;
      hasBackward: boolean;
    }> = [];

    // Guard: cytoscape rejects empty-string element IDs; skip events that the
    // backend generated with an empty name (e.g. from null URL column values).
    const graphEvents = visibleEvents.filter((e) => e.id !== "");

    graphEvents.forEach((rowEvent) => {
      graphEvents.forEach((colEvent) => {
        const isSelfLoop = rowEvent.id === colEvent.id;

        const forwardValue = store.getMatrixValue(rowEvent.id, colEvent.id);
        if (!Number.isFinite(forwardValue) || forwardValue === 0) return;
        // Skip near-zero probability edges (< 1%) that add visual noise
        if (isProbability && Math.abs(forwardValue) < 0.01) return;

        const backwardValue = store.getMatrixValue(colEvent.id, rowEvent.id);
        const hasBackward =
          !isSelfLoop && Number.isFinite(backwardValue) && backwardValue !== 0;

        edgesToAdd.push({
          source: rowEvent.id,
          target: colEvent.id,
          weight: forwardValue,
          hasBackward,
        });
      });
    });

    // Sort by weight descending to find top 10 (use absolute weight for diff)
    edgesToAdd.sort((a, b) => Math.abs(b.weight) - Math.abs(a.weight));
    const top10Edges = new Set(
      edgesToAdd.slice(0, 10).map((e) => `${e.source}|${e.target}`),
    );

    // Prepare Cytoscape elements
    const elements: ElementDefinition[] = [];
    let positionsChanged = false;
    let requiresAutoLayout = false;

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

      // Priority: saved position > API position > hash-based position
      let position: StoredPosition;
      if (isValidPosition) {
        position = existing;
      } else if (hasApiPosition) {
        position = { x: apiPosition.x, y: apiPosition.y };
        positionsRef.current[event.id] = position;
        positionsChanged = true;
      } else {
        position = createInitialPosition(event.id);
        positionsRef.current[event.id] = position;
        positionsChanged = true;
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
      const forwardNormalized = normalizeWeight(edge.weight);
      const isSelfLoop = edge.source === edge.target;

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

      // Only show label if it's in the top 10
      const showLabel = top10Edges.has(`${edge.source}|${edge.target}`);

      const baseValue = isDifferential
        ? (edge.weight > 0 ? "+" : "") + formatValue(edge.weight)
        : formatValue(edge.weight);

      // Base label for static view (top 10).
      const label = showLabel ? baseValue : "";

      elements.push({
        group: "edges",
        data: {
          id: `${edge.source}-${edge.target}`,
          source: edge.source,
          target: edge.target,
          label,
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
          showLabel,
          loopDirection: DEFAULT_LOOP_DIRECTION,
          zIndex: showLabel ? 999 : 1, // Higher z-index for labeled edges
        },
      });
    });

    // Create stylesheet
    const labelColor = isDark ? "#f3f4f6" : "#1f2937";
    const outlineColor = isDark ? "#1f2937" : "#ffffff";
    // Edge label text color: dark in dark theme (light outline), light in light theme (dark outline)
    const edgeLabelColor = isDark ? "#1f2937" : "#f3f4f6";
    const overlayColor = isDark ? "#fbbf24" : "#f59e0b"; // amber for AI overlay
    // Focus palette: peach-orange + cool opposite tone, loops stay neutral gray.
    const incomingFocusColor = isDark ? "167, 139, 250" : "99, 102, 241";
    const outgoingFocusColor = isDark ? "251, 146, 60" : "234, 88, 12";
    const loopFocusColor = isDark ? "156, 163, 175" : "107, 114, 128";
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
      {
        selector: "edge.focus-incoming",
        style: {
          "line-color": `rgb(${incomingFocusColor})`,
          "target-arrow-color": `rgb(${incomingFocusColor})`,
          "text-outline-color": `rgb(${incomingFocusColor})`,
          "z-index": 9996,
        },
      },
      {
        selector: "edge.focus-outgoing",
        style: {
          "line-color": `rgb(${outgoingFocusColor})`,
          "target-arrow-color": `rgb(${outgoingFocusColor})`,
          "text-outline-color": `rgb(${outgoingFocusColor})`,
          "z-index": 9997,
        },
      },
      {
        selector: "edge.focus-loop",
        style: {
          "line-color": `rgb(${loopFocusColor})`,
          "target-arrow-color": `rgb(${loopFocusColor})`,
          "text-outline-color": `rgb(${loopFocusColor})`,
          "z-index": 9995,
        },
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
          "line-style": "dashed",
          "line-dash-pattern": [5, 4],
          "line-dash-offset": 0,
          "target-arrow-shape": "triangle",
          "arrow-scale": 1.5,
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
    if (fitRef) fitRef.current = () => cy.fit(undefined, 12);
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

    // Apply layout if no saved positions
    if (!hasSavedPositions && requiresAutoLayout) {
      const layout = cy.layout({
        name: "fcose",
        randomize: true,
        animate: false,
        fit: true,
        padding: 80,
        nodeSeparation: 150,
        nodeRepulsion: 50000,
        idealEdgeLength: 200,
        edgeElasticity: 0.45,
        nestingFactor: 0.1,
        gravity: 0.1,
        gravityRange: 3.8,
      } as any);

      layout.run();

      // Wait for layout to finish
      layout.on("layoutstop", () => {
        // Enforce Left-to-Right orientation
        const anchorNode = "session_start";
        const hasAnchor = cy.getElementById(anchorNode).length > 0;
        const effectiveStartNode = hasAnchor
          ? anchorNode
          : visibleEvents[0]?.id;

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

        // Update positions ref after layout
        cy.nodes().forEach((node) => {
          const pos = node.position();
          positionsRef.current[node.id()] = { x: pos.x, y: pos.y };
        });

        if (positionsChanged) {
          persistPositions();
        }

        // Fit after auto-layout completes
        requestAnimationFrame(() => cy.fit(undefined, 12));
      });
    } else {
      recalculateSelfLoopDirections(cy);

      // Update positions ref from current positions
      cy.nodes().forEach((node) => {
        const pos = node.position();
        positionsRef.current[node.id()] = { x: pos.x, y: pos.y };
      });

      if (positionsChanged) {
        persistPositions();
      }

      // Restore the saved viewport if there is one; otherwise fit to canvas
      // after the browser has laid out the container.
      const savedViewport = viewportRef.current;
      requestAnimationFrame(() => {
        if (savedViewport) cy.viewport({ zoom: savedViewport.zoom, pan: { ...savedViewport.pan } });
        else cy.fit(undefined, 12);
      });
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

    // Node click (focus mode); Shift+click toggles multi-select
    cy.on("tap", "node", (event) => {
      if (isDraggingRef.current) return;
      const nodeId = event.target.id();
      const isShift =
        (event.originalEvent as MouseEvent | undefined)?.shiftKey ?? false;
      if (isShift) {
        setFocusedNodes((prev) =>
          prev.includes(nodeId)
            ? prev.filter((id) => id !== nodeId)
            : [...prev, nodeId],
        );
      } else {
        setFocusedNodes([nodeId]);
      }
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

    // Edge click (color picker)
    cy.on("tap", "edge", (event) => {
      const edge = event.target;
      if (isDraggingRef.current || edge.hasClass("dimmed")) return;

      const source = edge.source().id();
      const target = edge.target().id();

      const renderedPosition = event.renderedPosition || event.position;

      setColorPicker({
        kind: "edge",
        x: renderedPosition.x,
        y: renderedPosition.y,
        source,
        target,
      });
      setTooltip(null);
    });

    // Click stage to close color picker
    cy.on("tap", (event) => {
      if (event.target === cy) {
        setFocusedNodes([]);
        setColorPicker(null);
        setTooltip(null);
      }
    });

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
    maxWeight,
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

  // Apply node focus state to elements
  const applyFocusState = React.useCallback(
    (cy: Core, progress: number) => {
      const activeFocusNodes = focusedNodes;
      const resetFocusVisuals = () => {
        cy.elements().removeClass("focus-incoming focus-outgoing focus-loop");
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

      if (activeFocusNodes.length === 0 || progress <= 0) {
        cy.elements().removeClass("dimmed highlighted");
        resetFocusVisuals();
        return;
      }

      const focusedSet = new Set(activeFocusNodes);

      // Union of all edges connected to any focused node
      const allConnectedEdges = activeFocusNodes.reduce((acc, nodeId) => {
        const n = cy.getElementById(nodeId);
        if (n.length === 0) return acc;
        return acc.union(
          n
            .connectedEdges()
            .filter(
              (edge: cytoscape.EdgeSingular) => !edge.hasClass("filtered"),
            ),
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

      const dimOpacity = 1 - progress * 0.9;
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
        const isSignificantEdge =
          relativeWeight >= FOCUS_COLOR_MIN_VISIBLE_RATIO;

        if (isSignificantEdge && !isDifferential) {
          edge.addClass("focus-incoming");
        }

        edge.style({
          width: currentWidth,
          label,
          "target-arrow-shape": showArrow ? "triangle" : "none",
          "font-size": showFocusLabel ? 13 + relativeWeight * 9 : "",
          ...(isSignificantEdge || isDifferential
            ? {}
            : {
                "line-color": `rgb(${neutralFocusColor})`,
                "target-arrow-color": `rgb(${neutralFocusColor})`,
                "text-outline-color": `rgb(${neutralFocusColor})`,
              }),
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
        const isSignificantEdge =
          relativeWeight >= FOCUS_COLOR_MIN_VISIBLE_RATIO;

        if (isSignificantEdge && !isDifferential) {
          edge.addClass("focus-outgoing");
        }

        edge.style({
          width: currentWidth,
          label,
          "target-arrow-shape": showArrow ? "triangle" : "none",
          "font-size": showFocusLabel ? 13 + relativeWeight * 9 : "",
          ...(isSignificantEdge || isDifferential
            ? {}
            : {
                "line-color": `rgb(${neutralFocusColor})`,
                "target-arrow-color": `rgb(${neutralFocusColor})`,
                "text-outline-color": `rgb(${neutralFocusColor})`,
              }),
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
        edge.addClass("focus-loop");
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
    [focusedNodes, neutralFocusColor, isDifferential],
  );

  // Separate effect for edge threshold filtering — avoids full graph recreation
  // when the slider moves, so focusedNodes and other visual state persist.
  React.useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.edges().forEach((edge) => {
      const normalized = edge.data("normalizedWeight") as number;
      if (
        normalized < edgeThresholdParsed[0] ||
        normalized > edgeThresholdParsed[1]
      ) {
        edge.addClass("filtered");
      } else {
        edge.removeClass("filtered");
      }
    });
    // Re-apply focus styling since the visible edge set changed
    if (focusProgressRef.current > 0) {
      applyFocusState(cy, focusProgressRef.current);
    }
  }, [edgeThresholdParsed, applyFocusState]);

  // Focus animation
  React.useEffect(() => {
    if (typeof window === "undefined") return undefined;
    const cy = cyRef.current;
    if (!cy) return;

    if (focusAnimationFrameRef.current) {
      cancelAnimationFrame(focusAnimationFrameRef.current);
      focusAnimationFrameRef.current = null;
    }

    const target = focusedNodes.length > 0 ? 1 : 0;

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
  }, [focusedNodes, applyFocusState]);

  const handleResetLayout = React.useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;

    // Get API-computed positions if available
    const apiPositions = graphLayoutData?.result ?? {};

    // Reset to API positions if available, otherwise use hash-based positions
    const newPositions: Record<string, StoredPosition> = {};

    cy.nodes().forEach((node) => {
      const nodeId = node.id();
      const apiPosition = apiPositions[nodeId];
      if (
        apiPosition &&
        Number.isFinite(apiPosition.x) &&
        Number.isFinite(apiPosition.y)
      ) {
        newPositions[nodeId] = { x: apiPosition.x, y: apiPosition.y };
      } else {
        newPositions[nodeId] = createInitialPosition(nodeId);
      }
    });

    // Animate to new positions
    cy.nodes().forEach((node) => {
      const newPos = newPositions[node.id()];
      node.animate({
        position: newPos,
        duration: 500,
        easing: "ease-out",
      });
    });

    // Update positions ref and save after animation
    setTimeout(() => {
      positionsRef.current = newPositions;
      persistPositions();
      cy.fit(undefined, 80);
    }, 550);
  }, [persistPositions, graphLayoutData]);

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

      {/* Edge threshold slider */}
      <div
        style={{
          pointerEvents: "none",
          position: "absolute",
          bottom: 12,
          right: 12,
          display: "flex",
          alignItems: "center",
        }}
      >
        <div style={{ pointerEvents: "auto", width: 224 }}>
          <RangeSlider
            min={0}
            max={isProbability ? 1 : maxWeight}
            step={isProbability ? 0.001 : maxWeight > 1000 ? 10 : 1}
            value={
              isProbability
                ? edgeThresholdParsed
                : (edgeThresholdParsed.map((v) => v * maxWeight) as [
                    number,
                    number,
                  ])
            }
            onChange={(newValues) => {
              if (isProbability) {
                handleEdgeThresholdChange(newValues);
              } else {
                handleEdgeThresholdChange([
                  newValues[0] / maxWeight,
                  newValues[1] / maxWeight,
                ]);
              }
            }}
            variant="mini"
            formatValue={(v) =>
              isProbability ? `${(v * 100).toFixed(1)}%` : formatNumber(v)
            }
            scale={isProbability ? "linear" : "log"}
          />
        </div>
      </div>

      <div
        style={{
          position: "absolute",
          right: 56,
          top: 16,
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}
      >
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
            title={`Color node: ${focusedNodes[0]}`}
            style={btnStyle()}
          >
            Color Node
          </button>
        )}
        <button
          onClick={() => cyRef.current?.fit(undefined, 30)}
          title="Fit graph to canvas"
          style={btnStyle({ padding: "4px 8px" })}
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="15 3 21 3 21 9"/>
            <polyline points="9 21 3 21 3 15"/>
            <line x1="21" y1="3" x2="14" y2="10"/>
            <line x1="3" y1="21" x2="10" y2="14"/>
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
              segmentName={diffSegment || "segment"}
              value1Label={diffValue1 != null && diffValue1 !== "" ? String(diffValue1) : "group1"}
              value2Label={diffValue2 != null && diffValue2 !== "" ? String(diffValue2) : "group2"}
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
              segmentName={diffSegment || "segment"}
              value1Label={diffValue1 != null && diffValue1 !== "" ? String(diffValue1) : "group1"}
              value2Label={diffValue2 != null && diffValue2 !== "" ? String(diffValue2) : "group2"}
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
      `}</style>
    </div>
  );
});
