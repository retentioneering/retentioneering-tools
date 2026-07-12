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
