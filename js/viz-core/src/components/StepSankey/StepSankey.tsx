import * as React from "react";
import { observer } from "mobx-react-lite";
import { StepMatrixStore } from "../../stores/StepMatrixStore";
import { PatternStore } from "../../stores/PatternStore";
import { StepColumn } from "./StepColumn";
import { ConnectionLayer } from "./ConnectionLayer";
import { GapSeparator } from "./GapSeparator";
import { DiffBreakdownTooltip } from "../TransitionGraph/DiffBreakdownTooltip";

const COLUMN_WIDTH = 70;
const COLUMN_GAP = 40;

export interface StepSankeyProps {
  store: StepMatrixStore;
  pathPattern?: string;
  onPatternChange?: (pattern: string) => void;
  maxSteps?: number;
  /** Frontend-only: how many variable columns to show around each anchor.
   *  0 (default) means show all computed steps (= maxSteps). */
  stepWindow?: number;
  onStepWindowChange?: (w: number) => void;
  diffSegment?: string | null;
  diffValue1?: string | null;
  diffValue2?: string | null;
  theme?: "dark" | "light" | "auto";
  // persistence: horizontal scroll position (px)
  initialScrollX?: number;
  onScrollXChange?: (x: number) => void;
}

// Inline helper: checks if a diff breakdown has group values
const hasDiffBreakdown = (vs: {
  group1Value: number | null;
  group2Value: number | null;
  diffValue: number | null;
}) => vs.group1Value !== null && vs.group2Value !== null;

const DEFAULT_DISPLAY_PATTERN = "path_start->.*->path_end";

export const StepSankey = observer(({
  store,
  pathPattern = "",
  onPatternChange,
  maxSteps = 3,
  stepWindow: stepWindowProp,
  onStepWindowChange,
  diffSegment = null,
  diffValue1 = null,
  diffValue2 = null,
  initialScrollX,
  onScrollXChange,
}: StepSankeyProps) => {
  const isDiff = !!diffSegment;

  const stepWindow = (stepWindowProp && stepWindowProp > 0) ? stepWindowProp : maxSteps;

  // displayPattern is used only for rendering the matrix layout.
  // When no real pattern is set we fall back to the default two-block view.
  const displayPattern = pathPattern || DEFAULT_DISPLAY_PATTERN;

  // PatternStore uses only the REAL pattern (empty = no anchors yet).
  // This prevents the fallback path_end from leaking into user edits.
  const patternStore = React.useMemo(
    () => new PatternStore(pathPattern || null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  // Sync external pathPattern prop → patternStore (real pattern only)
  React.useEffect(() => {
    patternStore.load(pathPattern || null);
  }, [pathPattern, patternStore]);

  // Emit pattern changes to parent
  const commitPattern = React.useCallback(() => {
    const s = patternStore.asString ?? "";
    onPatternChange?.(s);
  }, [patternStore, onPatternChange]);
  const [hoveredDiffNode, setHoveredDiffNode] = React.useState<{
    eventId: string;
    stepIndex: number;
    matrixIndex: number;
    position: { x: number; y: number };
    group1Value: number | null;
    group2Value: number | null;
    diffValue: number | null;
  } | null>(null);

  // Parse pattern to find anchors (non-wildcard events)
  // e.g. "path_start->.*->main->.*->path_end" -> ["path_start", "main", "path_end"]
  // Parse pattern segments
  const patternSegments = React.useMemo(() => {
    if (store.isLoadingPreview) {
      return [["path_start"], ["main"], ["path_end"]];
    }

    return displayPattern.split("->").reduce((acc, token) => {
      if (token === ".*") {
        acc.push([]);
      } else {
        if (acc.length === 0) acc.push([]);
        if (token && token !== "") acc[acc.length - 1].push(token);
      }
      return acc;
    }, [] as string[][]);
  }, [displayPattern, store.isLoadingPreview]);

  // Parse pattern to find anchors (non-wildcard events)
  const patternAnchors = React.useMemo(() => {
    return patternSegments.map((tokens, index) => {
      if (tokens.length === 0) return "";
      // If it's the last segment and not the only segment, use the last token (End anchor)
      if (index === patternSegments.length - 1 && patternSegments.length > 1) {
        return tokens[tokens.length - 1];
      }
      // Otherwise use the first token (Start anchor)
      return tokens[0];
    });
  }, [patternSegments]);

  // Determine if a specific column in a matrix corresponds to a fixed event in the pattern
  const getFixedEventId = (
    matrixIdx: number,
    colIndex: number,
  ): string | null => {
    const segment = patternSegments[matrixIdx];
    if (!segment || segment.length === 0) return null;

    // Check if this matrix is End-aligned (Last segment, and not the only one)
    const isEndAligned =
      matrixIdx === patternSegments.length - 1 && patternSegments.length > 1;

    if (isEndAligned) {
      // Anchored at end (step 0 = last token)
      const tokenIndex = colIndex + segment.length - 1;
      if (tokenIndex >= 0 && tokenIndex < segment.length) {
        return segment[tokenIndex];
      }
    } else {
      // Anchored at start (step 0 = first token)
      const tokenIndex = colIndex;
      if (tokenIndex >= 0 && tokenIndex < segment.length) {
        return segment[tokenIndex];
      }
    }
    return null;
  };

  // Determine which event is the center (step 0) per matrix
  const getMatrixCenterEventId = (matrixIdx: number) => {
    // Use the anchor defined in the pattern for this matrix position
    if (patternAnchors[matrixIdx]) {
      return patternAnchors[matrixIdx];
    }

    // Fallback: heuristic based on matrix index (should rarely be reached if pattern matches data)
    return matrixIdx === 0 ? "path_start" : "path_end";
  };

  // Ref for scrolling to center
  const scrollContainerRef = React.useRef<HTMLDivElement>(null);

  // Last known horizontal scroll — restored on data changes and persisted
  // through onScrollXChange (debounced).
  const scrollXRef = React.useRef<number>(initialScrollX ?? 0);
  const scrollSaveTimeoutRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);
  const onScrollXChangeRef = React.useRef(onScrollXChange);
  onScrollXChangeRef.current = onScrollXChange;
  const handleScroll = React.useCallback((e: React.UIEvent<HTMLDivElement>) => {
    scrollXRef.current = (e.target as HTMLDivElement).scrollLeft;
    if (scrollSaveTimeoutRef.current) clearTimeout(scrollSaveTimeoutRef.current);
    scrollSaveTimeoutRef.current = setTimeout(() => {
      onScrollXChangeRef.current?.(scrollXRef.current);
    }, 300);
  }, []);

  // Global mouse X position for fisheye effect across all columns
  const [mouseX, setMouseX] = React.useState<number | null>(null);
  const matrixContainerRef = React.useRef<HTMLDivElement>(null);
  // Refs for measuring block heights
  const blockRefs = React.useRef<Map<number, HTMLDivElement>>(new Map());
  const [blockHeights, setBlockHeights] = React.useState<Map<number, number>>(
    new Map(),
  );

  // Calculate matrix start X position (no centering offset needed with fit-content)
  const getMatrixStartX = (matrixIdx: number): number => {
    let startX = 0;
    for (let i = 0; i < matrixIdx; i++) {
      startX +=
        matrixColumns[i].columns.length * COLUMN_WIDTH +
        (matrixColumns[i].columns.length - 1) * COLUMN_GAP;
      startX += 64; // Separator width
    }
    return startX;
  };

  // Calculate proximity for a column based on X distance
  const getColumnProximity = (
    columnIndex: number,
    matrixStartX: number,
  ): number => {
    if (mouseX === null) return 0;
    const columnCenterX =
      matrixStartX +
      columnIndex * (COLUMN_WIDTH + COLUMN_GAP) +
      COLUMN_WIDTH / 2;
    const distance = Math.abs(mouseX - columnCenterX);
    const threshold = 150; // Pixel radius for X proximity
    if (distance >= threshold) return 0;
    const proximity = 1 - distance / threshold;
    return Math.pow(proximity, 1.5); // Smooth easing
  };

  const clearHoveredDiffNode = React.useCallback(() => {
    setHoveredDiffNode(null);
  }, []);

  const handleNodeHover = React.useCallback(
    (payload: {
      eventId: string;
      value: number;
      stepIndex: number;
      matrixIndex: number;
      anchorRect: DOMRect;
    }) => {
      if (!isDiff) return;

      const breakdown = store.getDiffBreakdownByStep(
        payload.eventId,
        payload.stepIndex,
        payload.matrixIndex,
      );
      if (!hasDiffBreakdown(breakdown)) {
        setHoveredDiffNode(null);
        return;
      }

      setHoveredDiffNode({
        eventId: payload.eventId,
        stepIndex: payload.stepIndex,
        matrixIndex: payload.matrixIndex,
        position: {
          x: payload.anchorRect.right + 12,
          y: payload.anchorRect.top + payload.anchorRect.height / 2,
        },
        ...breakdown,
      });
    },
    [isDiff, store],
  );

  // Group columns by matrix - each matrix has its own set of columns
  const matrixColumns = React.useMemo(() => {
    return store.matrices.map((matrix, matrixIndex) => {
      const sorted = [...matrix.columns].sort((a, b) => a - b);
      const segment = patternSegments[matrixIndex];
      const numFixedEvents = segment?.length ?? 0;

      const isFirstSegment = matrixIndex === 0;
      const isLastSegment = matrixIndex === patternSegments.length - 1;
      const isMiddleSegment = !isFirstSegment && !isLastSegment;

      if (isLastSegment && patternSegments.length > 1) {
        // End-aligned: fixed events are at -(numFixedEvents-1), ..., -1, 0
        const firstFixedCol = -(numFixedEvents - 1);
        const lastFixedCol = 0;

        const filtered = sorted.filter((col) => {
          if (col >= firstFixedCol && col <= lastFixedCol) return true; // Fixed columns only
          if (col < firstFixedCol && col >= firstFixedCol - stepWindow) return true; // Variable before
          if (col > lastFixedCol && col <= lastFixedCol + stepWindow) return true; // Variable after
          return false;
        });

        return { matrixIndex, columns: filtered };
      } else if (isMiddleSegment) {
        // Center-aligned: fixed events at 0, 1, ..., numFixedEvents-1
        const lastFixedCol = numFixedEvents - 1;

        const filtered = sorted.filter((col) => {
          if (col >= 0 && col <= lastFixedCol) return true;
          if (col < 0 && col >= -stepWindow) return true;
          if (col > lastFixedCol && col <= lastFixedCol + stepWindow) return true;
          return false;
        });

        return { matrixIndex, columns: filtered };
      } else {
        // Start-aligned (first segment): fixed events are at 0, 1, ..., numFixedEvents-1
        const lastFixedCol = numFixedEvents - 1;
        const firstVariableCol = lastFixedCol + 1;

        const filtered = sorted.filter((col) => {
          if (col >= 0 && col <= lastFixedCol) return true; // Fixed columns only
          if (col < 0 && col >= -stepWindow) return true; // Variable before
          if (col >= firstVariableCol && col < firstVariableCol + stepWindow) return true; // Variable after
          return false;
        });

        return { matrixIndex, columns: filtered };
      }
    });
  }, [store.matrices, stepWindow, patternSegments]);

  // Measure block heights
  React.useEffect(() => {
    const updateHeights = () => {
      const heights = new Map<number, number>();
      blockRefs.current.forEach((el, idx) => {
        if (el) {
          heights.set(idx, el.offsetHeight);
        }
      });
      setBlockHeights(heights);
    };

    updateHeights();

    const observers: ResizeObserver[] = [];
    blockRefs.current.forEach((el) => {
      if (el) {
        const observer = new ResizeObserver(updateHeights);
        observer.observe(el);
        observers.push(observer);
      }
    });

    return () => {
      observers.forEach((obs) => obs.disconnect());
    };
  }, [matrixColumns]);

  // On load, restore the saved horizontal position (left edge by default).
  React.useEffect(() => {
    if (!scrollContainerRef.current || matrixColumns.length === 0) return;

    scrollContainerRef.current.scrollTo({
      left: scrollXRef.current,
      behavior: "auto",
    });
  }, [matrixColumns]);

  React.useEffect(() => {
    if (!isDiff && hoveredDiffNode) {
      setHoveredDiffNode(null);
    }
  }, [isDiff, hoveredDiffNode]);

  if (!store.hasData) {
    return (
      <div
        style={{
          display: "flex",
          height: "100%",
          width: "100%",
          alignItems: "center",
          justifyContent: "center",
          color: "#6b7280",
        }}
      >
        No data available
      </div>
    );
  }

  return (
    <div
      ref={scrollContainerRef}
      onScroll={handleScroll}
      style={{
        position: "relative",
        display: "flex",
        flex: 1,
        flexDirection: "column",
        overflow: "auto",
        padding: 16,
        background: "#ffffff",
        borderRadius: 8,
      }}
    >
      <div
        ref={matrixContainerRef}
        style={{
          position: "relative",
          margin: "0 auto",
          display: "flex",
          flexDirection: "row",
          alignItems: "stretch",
          borderRadius: 8,
          border: "1px solid #e5e7eb",
          width: "fit-content",
        }}
        onMouseMove={(e) => {
          if (matrixContainerRef.current) {
            const rect = matrixContainerRef.current.getBoundingClientRect();
            setMouseX(e.clientX - rect.left);
          }
        }}
        onMouseLeave={() => setMouseX(null)}
      >
        {/* Render each matrix separately with gaps between them */}
        {matrixColumns.map((matrixGroup, matrixIdx) => {
          return (
            <React.Fragment key={matrixIdx}>
              {/* Matrix block container */}
              <div
                ref={(el) => {
                  if (el) blockRefs.current.set(matrixIdx, el);
                  else blockRefs.current.delete(matrixIdx);
                }}
                style={{
                  position: "relative",
                  display: "flex",
                  flexDirection: "row",
                  alignSelf: "stretch",
                  paddingBottom: 16,
                  paddingTop: 16,
                  gap: COLUMN_GAP,
                }}
              >
                {/* Connection Layer for this matrix */}
                <ConnectionLayer
                  store={store}
                  columns={matrixGroup.columns}
                  matrixIndex={matrixGroup.matrixIndex}
                  columnWidth={COLUMN_WIDTH}
                  columnGap={COLUMN_GAP}
                  isDiff={isDiff}
                  centerEventId={getMatrixCenterEventId(
                    matrixGroup.matrixIndex,
                  )}
                  getFixedEventId={(colIdx) =>
                    getFixedEventId(matrixGroup.matrixIndex, colIdx)
                  }
                />

                {/* Columns within this matrix */}
                {matrixGroup.columns.map((colIndex, i) => {
                  const fixedId = getFixedEventId(matrixGroup.matrixIndex, colIndex);
                  const segment = patternSegments[matrixGroup.matrixIndex];
                  const isEndAligned = matrixGroup.matrixIndex === patternSegments.length - 1 && patternSegments.length > 1;
                  const tokenIndex = isEndAligned ? colIndex + (segment?.length ?? 1) - 1 : colIndex;

                  // Build menu props for fixed nodes so users can edit the pattern
                  const buildMenuProps = () => {
                    if (!fixedId || !onPatternChange) return undefined;
                    const isPathStart = fixedId === "path_start";
                    const isPathEnd   = fixedId === "path_end";
                    const allEvents = store.allEventIds;

                    // Map token position to patternStore segment index
                    let storeIdx = 0;
                    for (let m = 0; m < matrixGroup.matrixIndex; m++) {
                      const seg = patternSegments[m];
                      let len = (seg?.length ?? 0);
                      if (m === 0 && seg?.[0] === "path_start") len--;
                      storeIdx += Math.max(len, 0) + 1; // +1 for wildcard
                    }
                    const segOffset = matrixGroup.matrixIndex === 0 && segment?.[0] === "path_start"
                      ? tokenIndex - 1
                      : tokenIndex;
                    storeIdx += segOffset;

                    const commit = () => commitPattern();

                    if (isPathStart) return {
                      eventOptions: allEvents,
                      onAddNext: (ev: string) => { patternStore.addLeft(0, ev); commit(); },
                      onAddWildcardNext: (ev: string) => { patternStore.addLeftWithWildcard(0, ev); commit(); },
                    };
                    if (isPathEnd) return {
                      eventOptions: allEvents,
                      onAddPrevious: (ev: string) => { patternStore.addLeft(storeIdx, ev); commit(); },
                      onAddWildcardPrevious: (ev: string) => { patternStore.addWithTrailingGap(storeIdx, ev); commit(); },
                    };
                    return {
                      eventOptions: allEvents,
                      onReplace: (ev: string) => { patternStore.replace(storeIdx, ev); commit(); },
                      onAddPrevious: (ev: string) => { patternStore.addLeft(storeIdx, ev); commit(); },
                      onAddWildcardPrevious: (ev: string) => { patternStore.addLeftWithWildcard(storeIdx, ev); commit(); },
                      onAddNext: (ev: string) => { patternStore.addRight(storeIdx, ev); commit(); },
                      onAddWildcardNext: (ev: string) => { patternStore.addRightWithWildcard(storeIdx, ev); commit(); },
                      onDelete: () => { patternStore.delete(storeIdx); commit(); },
                    };
                  };

                  return (
                    <StepColumn
                      key={colIndex}
                      store={store}
                      stepIndex={colIndex}
                      matrixIndex={matrixGroup.matrixIndex}
                      isCenter={colIndex === 0}
                      isDiff={isDiff}
                      isFirstColumn={i === 0}
                      isLastColumn={i === matrixGroup.columns.length - 1}
                      fixedEventId={fixedId}
                      fixedNodeMenuProps={buildMenuProps()}
                      proximity={getColumnProximity(i, getMatrixStartX(matrixIdx))}
                      columnWidth={COLUMN_WIDTH}
                      onNodeHover={handleNodeHover}
                      onNodeLeave={clearHoveredDiffNode}
                    />
                  );
                })}
              </div>

              {/* GapSeparator between matrices — clickable to insert/delete events */}
              {matrixIdx < matrixColumns.length - 1 && (() => {
                // Calculate the wildcard index in patternStore for this gap
                let wcIdx = 0;
                for (let m = 0; m <= matrixIdx; m++) {
                  const seg = patternSegments[m];
                  let len = seg?.length ?? 0;
                  if (m === 0 && seg?.[0] === "path_start") len--;
                  wcIdx += Math.max(len, 0);
                  if (m < matrixIdx) wcIdx += 1; // wildcard between segments
                }

                return (
                  <GapSeparator
                    key={`gap-${matrixIdx}`}
                    eventOptions={store.allEventIds}
                    onInsert={onPatternChange ? (ev) => { patternStore.insertIntoWildcard(wcIdx, ev); commitPattern(); } : undefined}
                    onDelete={onPatternChange ? () => { patternStore.delete(wcIdx); commitPattern(); } : undefined}
                  />
                );
              })()}
            </React.Fragment>
          );
        })}
      </div>
      {hoveredDiffNode && (
        <div
          style={{
            pointerEvents: "none",
            position: "fixed",
            zIndex: 50,
            left: hoveredDiffNode.position.x,
            top: hoveredDiffNode.position.y,
            transform: "translateY(-50%)",
          }}
        >
          <DiffBreakdownTooltip
            isDark={false}
            title={
              <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span>{hoveredDiffNode.eventId}</span>
                <span style={{ color: "#6b7280" }}>@</span>
                <span>step {hoveredDiffNode.stepIndex}</span>
              </span>
            }
            segmentName={diffSegment || "segment"}
            value1Label={diffValue1 != null && diffValue1 !== "" ? String(diffValue1) : "group1"}
            value2Label={diffValue2 != null && diffValue2 !== "" ? String(diffValue2) : "group2"}
            group1Value={hoveredDiffNode.group1Value}
            group2Value={hoveredDiffNode.group2Value}
            diffValue={hoveredDiffNode.diffValue}
          />
        </div>
      )}
    </div>
  );
});
