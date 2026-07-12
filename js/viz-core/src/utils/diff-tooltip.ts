export interface DiffBreakdownValues {
  group1Value: number | null;
  group2Value: number | null;
  diffValue: number | null;
}

const FMT = new Intl.NumberFormat("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 4 });

function norm(v: number | null | undefined): number | null {
  if (typeof v !== "number" || !Number.isFinite(v)) return null;
  return Object.is(v, -0) ? 0 : v;
}

export function formatExactNumber(v: number | null | undefined): string {
  const n = norm(v);
  return n === null ? "-" : FMT.format(n);
}

export function formatSignedExactNumber(v: number | null | undefined): string {
  const n = norm(v);
  if (n === null) return "-";
  return `${n > 0 ? "+" : ""}${FMT.format(n)}`;
}

export function resolveDiffValue(vs: DiffBreakdownValues): number | null {
  const d = norm(vs.diffValue);
  if (d !== null) return d;
  const g1 = norm(vs.group1Value);
  const g2 = norm(vs.group2Value);
  return g1 === null || g2 === null ? null : g1 - g2;
}

export interface DiffLabels {
  segmentName: string | null;
  value1Label: string;
  value2Label: string;
}

// The 3-element (segment_col, value1, value2) diff form always supplies all
// three; the 2-element (path_ids1, path_ids2) form never has a third
// element, so a missing value2 reliably signals path-id-group diff mode —
// there's no segment/value to name, so callers get generic group labels
// instead of leaking raw path IDs.
export function resolveDiffLabels(
  segment: string | null | undefined,
  value1: string | null | undefined,
  value2: string | null | undefined,
): DiffLabels {
  if (value2 == null || value2 === "") {
    return { segmentName: null, value1Label: "Group 1", value2Label: "Group 2" };
  }
  return {
    segmentName: segment || "segment",
    value1Label: value1 != null && value1 !== "" ? String(value1) : "group1",
    value2Label: String(value2),
  };
}
