export function formatTime(seconds: number | null): string {
  if (seconds === null || seconds === 0) return "-";
  const sign = seconds < 0 ? "-" : "";
  const abs = Math.abs(seconds);
  const units = [
    { u: "y",  s: 365 * 24 * 3600 },
    { u: "mo", s: 30 * 24 * 3600 },
    { u: "d",  s: 24 * 3600 },
    { u: "h",  s: 3600 },
    { u: "m",  s: 60 },
    { u: "s",  s: 1 },
    { u: "ms", s: 0.001 },
  ];
  for (const { u, s } of units) {
    const v = abs / s;
    if (v >= 1) {
      const f = v >= 100 ? Math.round(v).toString() : v >= 10 ? v.toFixed(1).replace(/\.0$/, "") : v.toFixed(2).replace(/\.00$/, "");
      return `${sign}${f}${u}`;
    }
  }
  return `${sign}${(abs * 1000).toFixed(3)}ms`;
}
