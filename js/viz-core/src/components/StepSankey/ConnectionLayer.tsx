import * as React from "react";
import { observer } from "mobx-react-lite";
import { StepMatrixStore } from "../../stores/StepMatrixStore";
import {
  getHeatmapColor,
  NODE_SIZE,
  MIN_DISPLAY_VALUE,
  calculateNodeHeight,
} from "./StepNode";
import { layoutColumn, LABEL_HEIGHT, GAP } from "./layout-utils";

export interface Event {
  id: string;
  isPinned: boolean;
  isHidden: boolean;
  population: number;
}

export interface ConnectionLayerProps {
  store: StepMatrixStore;
  columns: number[]; // e.g. [-1, 0, 1, 2]
  matrixIndex: number;
  columnWidth: number;
  columnGap: number;
  isDiff: boolean;
  centerEventId?: string; // To ensure ribbons attach to the center event even if value is 0
  getFixedEventId?: (colIndex: number) => string | null; // Function to check if a column is fixed
}

const NODE_HEIGHT = NODE_SIZE; // 40
const CENTER_NODE_HEIGHT = 80; // Taller for vertical rectangle
const HEADER_HEIGHT = 28; // Header height (h-5 = 20px + mb-2 = 8px)
const BLOCK_PADDING_TOP = 16; // pt-4 = 16px padding top of matrix block

export const ConnectionLayer = observer(({
  store,
  columns,
  matrixIndex,
  columnWidth,
  columnGap,
  isDiff,
  centerEventId,
  getFixedEventId,
}: ConnectionLayerProps) => {
  // Calculate ribbon opacity based on source and target values
  // For diff: bright = far from 0 (both + and -), dim = close to 0
  // For non-diff: bright = high values, dim = low values
  const calcRibbonOpacity = (
    sourceVal: number,
    targetVal: number,
    sourceMax: number,
    targetMax: number,
  ): number => {
    const minOpacity = 0.08;
    const maxOpacity = 0.7;

    // Use absolute values for ratio calculation
    const srcAbs = Math.abs(sourceVal);
    const tgtAbs = Math.abs(targetVal);

    // Calculate ratios (0 to 1)
    const srcRatio = sourceMax > 0 ? srcAbs / sourceMax : 0;
    const tgtRatio = targetMax > 0 ? tgtAbs / targetMax : 0;

    // Average the two ratios
    const avgRatio = (srcRatio + tgtRatio) / 2;

    // Map to opacity range
    return minOpacity + avgRatio * (maxOpacity - minOpacity);
  };

  const renderConnections = () => {
    const connections: React.ReactNode[] = [];
    const normalizeBoundaryId = (eventId: string) =>
      eventId.trim().toLowerCase();
    const isPathEnd = (eventId: string) =>
      normalizeBoundaryId(eventId) === "path_end";
    const isPathStart = (eventId: string) =>
      normalizeBoundaryId(eventId) === "path_start";
    const canBeSource = (eventId: string) => !isPathEnd(eventId);
    const canBeTarget = (eventId: string) => !isPathStart(eventId);
    const getMaxAbsValue = (events: Event[], colIndex: number) =>
      events.reduce((max, event) => {
        const value = Math.abs(
          store.getMatrixValue(event.id, colIndex, matrixIndex),
        );
        return Math.max(max, value);
      }, 0);

    for (let i = 0; i < columns.length - 1; i++) {
      const sourceColIndex = columns[i];
      const targetColIndex = columns[i + 1];

      // Skip if columns are not adjacent (shouldn't happen in this layout)
      if (Math.abs(targetColIndex - sourceColIndex) !== 1) continue;

      const sourceFixedId = getFixedEventId?.(sourceColIndex) ?? null;
      const targetFixedId = getFixedEventId?.(targetColIndex) ?? null;

      const isSourceCenter = sourceColIndex === 0;
      const isTargetCenter = targetColIndex === 0;

      // Treat as "fixed/center" logic if it IS the literal center OR if it has a fixed ID
      const useSourceRibbons = isSourceCenter || !!sourceFixedId;
      const useTargetRibbons = isTargetCenter || !!targetFixedId;

      // Get events for source and target
      const sourceEvents = store.getTopEvents(
        sourceColIndex,
        6,
        isDiff,
        matrixIndex,
      );
      const targetEvents = store.getTopEvents(
        targetColIndex,
        6,
        isDiff,
        matrixIndex,
      );

      // Combine top and rest if expanded?
      const sourceExpanded = store.expandedColumns.has(sourceColIndex);
      const targetExpanded = store.expandedColumns.has(targetColIndex);

      const sourceListRaw: Event[] = sourceExpanded
        ? [...sourceEvents.top, ...sourceEvents.rest]
        : sourceEvents.top;
      const targetListRaw: Event[] = targetExpanded
        ? [...targetEvents.top, ...targetEvents.rest]
        : targetEvents.top;

      // For fixed columns: show ONLY the fixed event
      // For regular columns: filter by threshold
      const filterEvents = (
        list: Event[],
        colIdx: number,
        fixedId: string | null,
      ) => {
        // If column is fixed, return ONLY the fixed event
        if (fixedId) {
          const fixedEvent = list.find((e) => e.id === fixedId) ?? {
            id: fixedId,
            isPinned: false,
            isHidden: false,
            population: 0,
          };
          return [fixedEvent];
        }

        // For center column (step 0) without explicit fixedId, ensure centerEventId is present
        if (colIdx === 0 && centerEventId) {
          const fixedEvent = list.find((e) => e.id === centerEventId) ?? {
            id: centerEventId,
            isPinned: false,
            isHidden: false,
            population: 0,
          };
          return [fixedEvent];
        }

        // Regular column: filter by threshold
        return list.filter(
          (e) =>
            Math.abs(store.getMatrixValue(e.id, colIdx, matrixIndex)) >=
            MIN_DISPLAY_VALUE,
        );
      };

      const sourceList = filterEvents(
        sourceListRaw,
        sourceColIndex,
        sourceFixedId,
      );
      const targetList = filterEvents(
        targetListRaw,
        targetColIndex,
        targetFixedId,
      );
      const sourceConnectable = sourceList.filter((event) =>
        canBeSource(event.id),
      );
      const targetConnectable = targetList.filter((event) =>
        canBeTarget(event.id),
      );

      // X positions - squares are centered in columns
      const sourceColumnCenter =
        i * (columnWidth + columnGap) + columnWidth / 2;
      const targetColumnCenter =
        (i + 1) * (columnWidth + columnGap) + columnWidth / 2;

      // Center node is 30px wide, regular node is NODE_SIZE (40px)
      const CENTER_HALF_WIDTH = 15;
      const REGULAR_HALF_WIDTH = NODE_SIZE / 2;

      const startX =
        sourceColumnCenter +
        (isSourceCenter ? CENTER_HALF_WIDTH : REGULAR_HALF_WIDTH);
      const endX =
        targetColumnCenter -
        (isTargetCenter ? CENTER_HALF_WIDTH : REGULAR_HALF_WIDTH);

      // Calculate max values for color intensity
      const sourceMaxAbs = getMaxAbsValue(sourceConnectable, sourceColIndex);
      const targetMaxAbs = getMaxAbsValue(targetConnectable, targetColIndex);

      // Compute layout for source and target columns
      // Pass the fixedEventId so only that specific event gets center-style height
      // For column 0, use centerEventId if no explicit fixedId
      const sourceEffectiveFixedId =
        sourceFixedId ?? (isSourceCenter ? centerEventId : null) ?? null;
      const targetEffectiveFixedId =
        targetFixedId ?? (isTargetCenter ? centerEventId : null) ?? null;
      const sourceLayout = layoutColumn(sourceList, sourceEffectiveFixedId);
      const targetLayout = layoutColumn(targetList, targetEffectiveFixedId);

      // 1. Ribbons (Center/Fixed -> Neighbor)
      if (useSourceRibbons) {
        // Special case: Both source AND target are fixed (single event each)
        // Draw a direct full-height ribbon between them
        const bothFixed =
          useSourceRibbons &&
          useTargetRibbons &&
          sourceConnectable.length === 1 &&
          targetConnectable.length === 1;

        sourceConnectable.forEach((sourceEvent) => {
          const sourceMetrics = sourceLayout.get(sourceEvent.id);
          if (!sourceMetrics) return;

          // Determine specific startX for this node (Center vs Regular)
          const isFixedNode =
            sourceEvent.id === sourceFixedId ||
            (isSourceCenter && sourceEvent.id === centerEventId);
          const halfW = isFixedNode ? CENTER_HALF_WIDTH : REGULAR_HALF_WIDTH;
          const thisStartX = sourceColumnCenter + halfW;

          const totalTargetValue = targetConnectable.reduce(
            (sum, t) =>
              sum +
              Math.abs(
                store.getMatrixValue(t.id, targetColIndex, matrixIndex),
              ),
            0,
          );

          let accumulatedSourceY = 0; // relative to source node top

          // Pre-calculate target distribution to fill entire target node height
          const targetDistributions = targetConnectable
            .map((targetEvent) => {
              const targetVal = Math.abs(
                store.getMatrixValue(
                  targetEvent.id,
                  targetColIndex,
                  matrixIndex,
                ),
              );
              const targetMetrics = targetLayout.get(targetEvent.id);
              if (!targetMetrics) return null;
              const ratio =
                totalTargetValue > 0 ? targetVal / totalTargetValue : 0;
              return { targetEvent, targetMetrics, targetVal, ratio };
            })
            .filter(Boolean) as Array<{
            targetEvent: Event;
            targetMetrics: { top: number; bottom: number; height: number };
            targetVal: number;
            ratio: number;
          }>;

          let accumulatedTargetY = 0; // relative to target node top

          targetDistributions.forEach(
            ({ targetEvent, targetMetrics, targetVal, ratio }) => {
              // Determine endX for target
              const isTargetFixedNode =
                targetEvent.id === targetFixedId ||
                (isTargetCenter && targetEvent.id === centerEventId);
              const targetHalfW = isTargetFixedNode
                ? CENTER_HALF_WIDTH
                : REGULAR_HALF_WIDTH;
              const thisEndX = targetColumnCenter - targetHalfW;

              // Calculate slice of source node height
              // Special case: if both are fixed, use full height
              let ribbonH: number;
              if (bothFixed) {
                ribbonH = sourceMetrics.height;
              } else {
                ribbonH = ratio * sourceMetrics.height;
              }

              // If ribbonH is too small, skip
              if (ribbonH < 0.5) return;

              const sourceYStart = sourceMetrics.top + accumulatedSourceY;
              const sourceYEnd = sourceYStart + ribbonH;
              accumulatedSourceY += ribbonH;

              // Target side: distribute across full target node height proportionally
              // Exception: if source is center/fixed event (black), each ribbon fills full target height
              let targetRibbonH: number;
              let targetYStart: number;
              let targetYEnd: number;
              if (bothFixed) {
                targetRibbonH = targetMetrics.height;
                targetYStart = targetMetrics.top;
                targetYEnd = targetMetrics.bottom;
              } else if (isFixedNode) {
                // Exception: center/fixed event -> target: each ribbon fills full target height
                targetRibbonH = targetMetrics.height;
                targetYStart = targetMetrics.top;
                targetYEnd = targetMetrics.bottom;
              } else {
                targetRibbonH = ratio * targetMetrics.height;
                targetYStart = targetMetrics.top + accumulatedTargetY;
                targetYEnd = targetYStart + targetRibbonH;
                accumulatedTargetY += targetRibbonH;
              }

              // Draw Ribbon
              const path = `
                  M ${thisStartX} ${sourceYStart}
                  C ${thisStartX + columnGap / 2} ${sourceYStart}, ${thisEndX - columnGap / 2} ${targetYStart}, ${thisEndX} ${targetYStart}
                  L ${thisEndX} ${targetYEnd}
                  C ${thisEndX - columnGap / 2} ${targetYEnd}, ${thisStartX + columnGap / 2} ${sourceYEnd}, ${thisStartX} ${sourceYEnd}
                  Z
                `;

              // Calculate opacity based on values
              const sourceVal = store.getMatrixValue(
                sourceEvent.id,
                sourceColIndex,
                matrixIndex,
              );
              const opacity = calcRibbonOpacity(
                sourceVal,
                targetVal,
                sourceMaxAbs,
                targetMaxAbs,
              );
              const ribbonColor = `rgba(156, 163, 175, ${opacity})`;

              connections.push(
                <path
                  key={`ribbon-${matrixIndex}-${sourceColIndex}-${targetColIndex}-${sourceEvent.id}-${targetEvent.id}`}
                  d={path}
                  fill={ribbonColor}
                  stroke="none"
                  style={{ transition: "opacity 200ms" }}
                />,
              );
            },
          );
        });
      } else if (useTargetRibbons) {
        // Fan In to Center/Fixed
        // Iterate Target (Center) events
        targetConnectable.forEach((targetEvent) => {
          const targetMetrics = targetLayout.get(targetEvent.id);
          if (!targetMetrics) return;

          const isTargetFixedNode =
            targetEvent.id === targetFixedId ||
            (isTargetCenter && targetEvent.id === centerEventId);
          const targetHalfW = isTargetFixedNode
            ? CENTER_HALF_WIDTH
            : REGULAR_HALF_WIDTH;
          const thisEndX = targetColumnCenter - targetHalfW;

          const totalSourceValue = sourceConnectable.reduce(
            (sum, s) =>
              sum +
              Math.abs(
                store.getMatrixValue(s.id, sourceColIndex, matrixIndex),
              ),
            0,
          );
          let accumulatedTargetY = 0;

          sourceConnectable.forEach((sourceEvent) => {
            const sourceVal = Math.abs(
              store.getMatrixValue(
                sourceEvent.id,
                sourceColIndex,
                matrixIndex,
              ),
            );
            const sourceMetrics = sourceLayout.get(sourceEvent.id);
            if (!sourceMetrics) return;

            // Determine startX
            const isSourceFixedNode =
              sourceEvent.id === sourceFixedId ||
              (isSourceCenter && sourceEvent.id === centerEventId);
            const sourceHalfW = isSourceFixedNode
              ? CENTER_HALF_WIDTH
              : REGULAR_HALF_WIDTH;
            const thisStartX = sourceColumnCenter + sourceHalfW;

            const ratio =
              totalSourceValue > 0 ? sourceVal / totalSourceValue : 0;
            const ribbonH = ratio * targetMetrics.height;

            if (ribbonH < 0.5) return;

            const sourceYStart = sourceMetrics.top;
            const sourceYEnd = sourceMetrics.bottom;

            // Target side: distribute across full target node height proportionally
            const targetYStart = targetMetrics.top + accumulatedTargetY;
            const targetYEnd = targetYStart + ribbonH;
            accumulatedTargetY += ribbonH;

            const path = `
                   M ${thisStartX} ${sourceYStart}
                   C ${thisStartX + columnGap / 2} ${sourceYStart}, ${thisEndX - columnGap / 2} ${targetYStart}, ${thisEndX} ${targetYStart}
                   L ${thisEndX} ${targetYEnd}
                   C ${thisEndX - columnGap / 2} ${targetYEnd}, ${thisStartX + columnGap / 2} ${sourceYEnd}, ${thisStartX} ${sourceYEnd}
                   Z
                 `;

            // Calculate opacity based on values
            const targetVal = store.getMatrixValue(
              targetEvent.id,
              targetColIndex,
              matrixIndex,
            );
            const opacity = calcRibbonOpacity(
              sourceVal,
              targetVal,
              sourceMaxAbs,
              targetMaxAbs,
            );
            const ribbonColor = `rgba(156, 163, 175, ${opacity})`;

            connections.push(
              <path
                key={`ribbon-${matrixIndex}-${sourceColIndex}-${targetColIndex}-${sourceEvent.id}-${targetEvent.id}`}
                d={path}
                fill={ribbonColor}
                stroke="none"
                style={{ transition: "opacity 200ms" }}
              />,
            );
          });
        });
      } else {
        // Sankey ribbons: fully connected, equal width
        // Each source event connects to ALL target events with equal-width ribbons
        const numTargets = targetConnectable.length;
        const numSources = sourceConnectable.length;
        if (numTargets === 0 || numSources === 0) continue;

        // Track accumulated Y position for each target node (across all sources)
        const targetAccumulatedY = new Map<string, number>();
        targetConnectable.forEach((t) => targetAccumulatedY.set(t.id, 0));

        sourceConnectable.forEach((sourceEvent) => {
          const sourceMetrics = sourceLayout.get(sourceEvent.id);
          if (!sourceMetrics) return;

          // Divide source node height equally among all target connections
          const ribbonHeight = sourceMetrics.height / numTargets;

          targetConnectable.forEach((targetEvent, tIdx) => {
            const targetMetrics = targetLayout.get(targetEvent.id);
            if (!targetMetrics) return;

            // Source: slice of the source node (equal division by targets)
            const sourceYStart = sourceMetrics.top + tIdx * ribbonHeight;
            const sourceYEnd = sourceYStart + ribbonHeight;

            // Target: divide target node equally among all sources
            const targetRibbonH = targetMetrics.height / numSources;
            const accY = targetAccumulatedY.get(targetEvent.id) ?? 0;
            const targetYStart = targetMetrics.top + accY;
            const targetYEnd = targetYStart + targetRibbonH;
            targetAccumulatedY.set(targetEvent.id, accY + targetRibbonH);

            // Bezier curve ribbon path
            const path = `
               M ${startX} ${sourceYStart}
               C ${startX + columnGap / 2} ${sourceYStart}, ${endX - columnGap / 2} ${targetYStart}, ${endX} ${targetYStart}
               L ${endX} ${targetYEnd}
               C ${endX - columnGap / 2} ${targetYEnd}, ${startX + columnGap / 2} ${sourceYEnd}, ${startX} ${sourceYEnd}
               Z
             `;

            // Calculate opacity based on values
            const sourceVal = store.getMatrixValue(
              sourceEvent.id,
              sourceColIndex,
              matrixIndex,
            );
            const targetValRaw = store.getMatrixValue(
              targetEvent.id,
              targetColIndex,
              matrixIndex,
            );
            const opacity = calcRibbonOpacity(
              sourceVal,
              targetValRaw,
              sourceMaxAbs,
              targetMaxAbs,
            );
            const ribbonColor = `rgba(156, 163, 175, ${opacity})`;

            connections.push(
              <path
                key={`sankey-${matrixIndex}-${sourceColIndex}-${targetColIndex}-${sourceEvent.id}-${targetEvent.id}`}
                d={path}
                fill={ribbonColor}
                stroke="none"
                style={{ transition: "opacity 200ms" }}
              />,
            );
          });
        });
      }
    }

    return connections;
  };

  const connections = renderConnections();

  return (
    <svg
      style={{
        pointerEvents: "none",
        position: "absolute",
        left: 0,
        top: BLOCK_PADDING_TOP + HEADER_HEIGHT,
        height: "100%",
        width: "100%",
        overflow: "visible",
      }}
    >
      {connections}
    </svg>
  );
});
