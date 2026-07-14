import * as React from "react";
import { observer } from "mobx-react-lite";
import { StepMatrixStore } from "../../stores/StepMatrixStore";
import { StepNode, MIN_DISPLAY_VALUE } from "./StepNode";

export interface FixedNodeMenuProps {
  eventOptions: string[];
  onAddPrevious?: (event: string) => void;
  onAddNext?: (event: string) => void;
  onReplace?: (event: string) => void;
  onAddWildcardPrevious?: (event: string) => void;
  onAddWildcardNext?: (event: string) => void;
  onDelete?: () => void;
}

export interface StepColumnProps {
  store: StepMatrixStore;
  stepIndex: number;
  matrixIndex: number;
  isCenter: boolean;
  isDiff: boolean;
  isFirstColumn?: boolean;
  isLastColumn?: boolean;
  fixedEventId?: string | null;
  fixedNodeMenuProps?: FixedNodeMenuProps;
  proximity: number;
  columnWidth: number;
  onNodeHover?: (payload: {
    eventId: string;
    value: number;
    stepIndex: number;
    matrixIndex: number;
    anchorRect: DOMRect;
  }) => void;
  onNodeLeave?: () => void;
}

export const StepColumn = observer(({
  store,
  stepIndex,
  matrixIndex,
  isCenter,
  isDiff,
  isFirstColumn = false,
  isLastColumn = false,
  fixedEventId,
  fixedNodeMenuProps,
  proximity,
  columnWidth,
  scrollContainerRef,
  onNodeHover,
  onNodeLeave,
}: StepColumnProps) => {
  const isExpanded = store.expandedColumns.has(stepIndex);

  // Get Top events
  // We use a limit of 6 for standard, and the store handles 3+3 for diff internally if we pass isDiff
  const { top, rest } = store.getTopEvents(stepIndex, 6, isDiff, matrixIndex);

  // Calculate max value for heatmap in this column
  // In diff mode, we might want max absolute value across the column
  const allValues = [...top, ...rest].map((e) =>
    store.getMatrixValue(e.id, stepIndex, matrixIndex),
  );
  const maxAbsValue = Math.max(...allValues.map(Math.abs));

  // If this column is fixed, show ONLY the fixed event
  // Otherwise show top events (expanded or not)
  let allEvents: typeof top;

  if (fixedEventId) {
    // Fixed column: show ONLY the fixed event
    const fixedEvent = [...top, ...rest].find(
      (e) => e.id === fixedEventId,
    ) ?? {
      id: fixedEventId,
      isPinned: false,
      isHidden: false,
      population: 0,
    };
    allEvents = [fixedEvent];
  } else {
    // Regular column: show top events
    allEvents = isExpanded ? [...top, ...rest] : top;
  }

  const isAboveThreshold = (e: (typeof top)[0]) => {
    // Always show fixed event, even if value is 0
    if (fixedEventId && e.id === fixedEventId) return true;
    return (
      Math.abs(store.getMatrixValue(e.id, stepIndex, matrixIndex)) >=
      MIN_DISPLAY_VALUE
    );
  };
  const displayedEvents = allEvents.filter(isAboveThreshold);
  const nonZeroRest = rest.filter(isAboveThreshold);

  // Highlighted cells for this column that are NOT rendered (value below threshold)
  const displayedEventIds = new Set(displayedEvents.map((e) => e.id));
  const missingHighlightedEventIds: string[] = [];
  for (const key of store.overlayHighlightedCells) {
    const parts = key.split("→");
    if (
      parts.length === 3 &&
      parts[1] === String(matrixIndex) &&
      parts[2] === String(stepIndex) &&
      !displayedEventIds.has(parts[0])
    ) {
      missingHighlightedEventIds.push(parts[0]);
    }
  }
  const hasMore = !store.isLoadingPreview && nonZeroRest.length > 0;

  // Determine flatSide for nodes based on position
  const isFixedColumn = !!fixedEventId;

  const getFlatSide = (): "left" | "right" | "both" | "none" => {
    if (isFixedColumn) {
      // Fixed column: flat on both sides since ribbons go both ways (or just one side if at edge of matrix)
      if (isFirstColumn) return "right"; // Only ribbons going right
      if (isLastColumn) return "left"; // Only ribbons going left
      return "both";
    }

    if (stepIndex > 0) return "left";
    if (stepIndex < 0) return "right";

    return "none";
  };

  const flatSide = getFlatSide();

  return (
    <div
      style={{
        position: "relative",
        zIndex: 10,
        display: "flex",
        flexShrink: 0,
        flexDirection: "column",
        width: columnWidth,
      }}
    >
      {/* Header (Step Number) */}
      <div
        style={{
          marginBottom: 8,
          display: "flex",
          height: 20,
          alignItems: "center",
          justifyContent: "center",
          textAlign: "center",
          fontSize: 12,
          fontWeight: 500,
          color: isCenter ? "#111827" : "#6b7280",
        }}
      >
        {isCenter ? (
          <span style={{ fontWeight: "bold", color: "#111827" }}>{stepIndex}</span>
        ) : (
          stepIndex
        )}
      </div>

      {/* Nodes - stacked vertically */}
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: 9,
        }}
      >
        {displayedEvents.map((event) => {
          // Check if this node is highlighted by AI
          const highlightKey = `${event.id}→${matrixIndex}→${stepIndex}`;
          const isHighlighted =
            store.overlayHighlightedCells.has(highlightKey);

          return (
            <StepNode
              key={event.id}
              event={event}
              value={store.getMatrixValue(event.id, stepIndex, matrixIndex)}
              maxValue={maxAbsValue}
              isDiff={isDiff}
              isCenter={fixedEventId === event.id} // Treat fixed events as "Center" for visuals
              flatSide={flatSide}
              proximity={proximity}
              isHighlighted={isHighlighted}
              onHover={(payload) =>
                onNodeHover?.({
                  ...payload,
                  stepIndex,
                  matrixIndex,
                })
              }
              onHoverEnd={onNodeLeave}
              onContextMenu={(e) => { e.preventDefault(); }}
              menuProps={fixedEventId === event.id ? fixedNodeMenuProps : undefined}
              isLoadingPreview={store.isLoadingPreview}
            />
          );
        })}
      </div>

      {/* Missing highlighted event placeholders */}
      {missingHighlightedEventIds.map((eventId) => (
        <div
          key={`missing-${eventId}`}
          title={`"${eventId}" is not visible at this step (value too small)`}
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: 2,
          }}
        >
          <div
            style={{
              display: "flex",
              flexShrink: 0,
              alignItems: "center",
              justifyContent: "center",
              border: "1px dashed rgba(107, 114, 128, 0.4)",
              fontSize: 9,
              color: "rgba(107, 114, 128, 0.5)",
              width: 40,
              height: 40,
            }}
          >
            —
          </div>
          <span
            style={{
              textAlign: "center",
              fontSize: 10,
              color: "rgba(107, 114, 128, 0.4)",
            }}
          >
            {eventId}
          </span>
        </div>
      ))}

      {/* More / Less Button */}
      {hasMore && (
        <button
          style={{
            marginTop: 8,
            height: 20,
            width: "100%",
            fontSize: 10,
            color: "#6b7280",
            background: "none",
            border: "none",
            cursor: "pointer",
            padding: "0 4px",
          }}
          onMouseEnter={(e) => { (e.currentTarget as HTMLButtonElement).style.color = "#111827"; }}
          onMouseLeave={(e) => { (e.currentTarget as HTMLButtonElement).style.color = "#6b7280"; }}
          onClick={() => store.toggleColumnExpansion(stepIndex)}
        >
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 4 }}>
            {isExpanded ? (
              <><span style={{ fontSize: 12, lineHeight: 1 }}>–</span> Less</>
            ) : (
              <><span style={{ fontSize: 12, lineHeight: 1 }}>+</span> {nonZeroRest.length} more</>
            )}
          </div>
        </button>
      )}
    </div>
  );
});
