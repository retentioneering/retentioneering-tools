import * as React from "react";
import { formatExactNumber, formatSignedExactNumber, resolveDiffValue } from "../../utils/diff-tooltip";

interface Props {
  title?: React.ReactNode;
  subtitle?: string;
  segmentName: string;
  value1Label: string;
  value2Label: string;
  group1Value: number | null;
  group2Value: number | null;
  diffValue: number | null;
  isDark?: boolean;
}

export function DiffBreakdownTooltip({ title, subtitle, segmentName, value1Label, value2Label, group1Value, group2Value, diffValue, isDark = true }: Props) {
  const resolved = resolveDiffValue({ group1Value, group2Value, diffValue });
  const bg = isDark ? "#1f2937" : "#fff";
  const border = isDark ? "#374151" : "#e5e7eb";
  const text = isDark ? "#f3f4f6" : "#111827";
  const muted = isDark ? "#9ca3af" : "#6b7280";
  const diffColor = resolved === null || resolved === 0 ? text : resolved > 0 ? "#ef4444" : "#3b82f6";

  return (
    <div style={{ minWidth: 280, maxWidth: 420, background: bg, border: `1px solid ${border}`, borderRadius: 12, padding: "12px 16px", boxShadow: "0 10px 15px rgba(0,0,0,0.3)", fontSize: 13 }}>
      {title && <div style={{ marginBottom: subtitle ? 2 : 8, fontWeight: 600, color: text }}>{title}</div>}
      {subtitle && <div style={{ marginBottom: 8, fontSize: 11, color: muted }}>{subtitle}</div>}
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        <Row label={`${segmentName}: ${value1Label}`} value={formatExactNumber(group1Value)} labelColor="#ef4444" text={text} />
        <Row label={`${segmentName}: ${value2Label}`} value={formatExactNumber(group2Value)} labelColor="#3b82f6" text={text} />
        <div style={{ borderTop: `1px solid ${border}`, margin: "2px 0" }} />
        <div style={{ display: "flex", justifyContent: "space-between", gap: 16 }}>
          <span style={{ fontWeight: 500, color: text }}>diff ({value1Label} − {value2Label})</span>
          <span style={{ fontFamily: "monospace", fontWeight: 600, color: diffColor }}>{formatSignedExactNumber(resolved)}</span>
        </div>
      </div>
    </div>
  );
}

function Row({ label, value, labelColor, text }: { label: string; value: string; labelColor: string; text: string }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", gap: 16 }}>
      <span style={{ color: labelColor }}>{label}</span>
      <span style={{ fontFamily: "monospace", color: text }}>{value}</span>
    </div>
  );
}
