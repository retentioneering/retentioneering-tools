import { calculateNodeHeight } from "./StepNode";

export interface Event {
  id: string;
  isPinned: boolean;
  isHidden: boolean;
  population: number;
}

export const LABEL_HEIGHT = 16;
export const GAP = 9;

export interface LayoutMetrics {
  top: number;
  bottom: number;
  height: number;
  center: number;
}

// Helper to layout a column and get Y positions for each event
// fixedEventId: if provided, only this event gets center-style height; others get regular height
export const layoutColumn = (
  events: Event[],
  fixedEventId: string | null,
): Map<string, LayoutMetrics> => {
  const layout = new Map<string, LayoutMetrics>();
  let currentY = 0;

  events.forEach((event) => {
    // Only the fixed event gets center-style height
    const isThisEventFixed = fixedEventId !== null && event.id === fixedEventId;
    const height = calculateNodeHeight(event.id, isThisEventFixed);
    const blockHeight = height + LABEL_HEIGHT + GAP;

    layout.set(event.id, {
      top: currentY,
      bottom: currentY + height,
      height: height,
      center: currentY + height / 2,
    });

    currentY += blockHeight;
  });

  return layout;
};
