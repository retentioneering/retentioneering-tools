// Pure Python-code rendering shared by the live widget and regression tests.

function pyRepr(v: any): string {
  if (v === null || v === undefined) return "None";
  if (typeof v === "boolean") return v ? "True" : "False";
  if (typeof v === "number") return String(v);
  if (typeof v === "string") return `'${v.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
  if (Array.isArray(v)) return `[${v.map(pyRepr).join(", ")}]`;
  if (typeof v === "object") return `{${Object.entries(v).map(([k, val]) => `${pyRepr(k)}: ${pyRepr(val)}`).join(", ")}}`;
  return JSON.stringify(v);
}

export function buildAddClustersCode(streamVarName: string, name: string, features: any[], method: string, scaler: string, params: Record<string, any>, pathCol: string, rename: Record<string, string>): string {
  // Prefer the successful analysis snapshot over controls edited since Apply.
  const usedMethod = (params.method as string) || method;
  const usedScaler = params.scaler !== undefined ? params.scaler : (scaler || null);
  const usedFeatures = params.features !== undefined ? params.features : features;
  const usedPathCol = params.path_col !== undefined ? params.path_col : pathCol;
  const methodArgs = (params.method_args ?? {}) as Record<string, any>;

  const lines = [`    name=${pyRepr(name)},`, `    features=${pyRepr(usedFeatures)},`, `    method=${pyRepr(usedMethod)},`];
  if (Object.keys(methodArgs).length > 0) lines.push(`    method_args=${pyRepr(methodArgs)},`);
  lines.push(`    scaler=${pyRepr(usedScaler)},`);
  if (params.nmf_components !== undefined && params.nmf_components !== null) {
    lines.push(`    nmf_components=${pyRepr(params.nmf_components)},`);
  }
  if (usedPathCol !== undefined && usedPathCol !== "") lines.push(`    path_col=${pyRepr(usedPathCol)},`);

  let code = `${streamVarName}_new = ${streamVarName}.add_clusters(\n${lines.join("\n")}\n)`;
  if (Object.keys(rename).length > 0) {
    code += `.rename_segment_levels(\n    ${pyRepr(name)},\n    ${pyRepr(rename)},\n)`;
  }
  return code;
}
