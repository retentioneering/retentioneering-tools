export function formatNumber(value: number | null): string {
  if (value === null) return "N/A";
  const isInteger = Number.isInteger(value);
  if (Math.abs(value) < 1000) {
    const formatted = isInteger ? value.toString() : value.toFixed(2);
    return Number(formatted) === 0 ? "0" : formatted;
  }
  if (Math.abs(value) < 1_000_000) {
    const k = value / 1000;
    return `${isInteger ? k.toFixed(1) : k.toFixed(2)}k`.replace(".0k", "k");
  }
  if (Math.abs(value) < 1_000_000_000) {
    const m = value / 1_000_000;
    return `${isInteger ? m.toFixed(1) : m.toFixed(2)}m`.replace(".0m", "m");
  }
  const b = value / 1_000_000_000;
  return `${isInteger ? b.toFixed(1) : b.toFixed(2)}b`.replace(".0b", "b");
}

export function formatPopulation(value: number | null): string {
  if (value === null) return "-";
  return new Intl.NumberFormat("en-US").format(Math.round(value));
}
