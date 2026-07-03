"use client";
import * as React from "react";
import * as SliderPrimitive from "@radix-ui/react-slider";

interface RangeSliderProps {
  min: number;
  max: number;
  step?: number;
  value: [number, number];
  onChange: (value: [number, number]) => void;
  formatValue?: (value: number) => string;
  variant?: "default" | "mini";
  scale?: "linear" | "log";
}

export function RangeSlider({
  min, max, step = 0.01, value, onChange,
  formatValue = (v) => v.toFixed(2),
  variant = "default", scale = "linear",
}: RangeSliderProps) {
  const [local, setLocal] = React.useState<[number, number]>(value);
  const debounceRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);

  const toSlider = React.useCallback((v: number) => {
    if (scale === "log" && max > 0) {
      if (min <= 0 && v <= 0) return 0;
      const effMin = min > 0 ? min : max * 0.0001;
      const effV = v <= 0 ? effMin : v;
      const pos = (Math.log(effV) - Math.log(effMin)) / (Math.log(max) - Math.log(effMin));
      return min <= 0 ? pos * 0.99 + 0.01 : pos;
    }
    return (v - min) / (max - min);
  }, [scale, min, max]);

  const fromSlider = React.useCallback((pos: number) => {
    if (scale === "log" && max > 0) {
      if (min <= 0 && pos <= 0.01) return 0;
      const effMin = min > 0 ? min : max * 0.0001;
      const adj = min <= 0 ? (pos - 0.01) / 0.99 : pos;
      return Math.exp(Math.log(effMin) + adj * (Math.log(max) - Math.log(effMin)));
    }
    return min + pos * (max - min);
  }, [scale, min, max]);

  const sliderVal = React.useMemo(() => [toSlider(value[0]), toSlider(value[1])] as [number, number], [value, toSlider]);
  React.useEffect(() => { setLocal(sliderVal); }, [sliderVal]);

  const handleChange = React.useCallback((v: number[]) => {
    setLocal([v[0], v[1]]);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => onChange([fromSlider(v[0]), fromSlider(v[1])]), 300);
  }, [onChange, fromSlider]);

  const handleCommit = React.useCallback((v: number[]) => {
    if (debounceRef.current) { clearTimeout(debounceRef.current); debounceRef.current = null; }
    onChange([fromSlider(v[0]), fromSlider(v[1])]);
  }, [onChange, fromSlider]);

  React.useEffect(() => () => { if (debounceRef.current) clearTimeout(debounceRef.current); }, []);

  const normStep = scale === "log" ? 0.001 : step / (max - min);

  const thumbStyle: React.CSSProperties = variant === "mini"
    ? { display: "block", width: 8, height: 16, borderRadius: 2, background: "#475569", border: "1px solid #94a3b8", cursor: "ew-resize", outline: "none" }
    : { display: "block", width: 16, height: 16, borderRadius: "50%", background: "#475569", border: "1px solid #94a3b8", cursor: "ew-resize", outline: "none" };

  return (
    <div style={{ flexGrow: 1 }}>
      {variant === "default" && (
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4, fontSize: 12, color: "#6b7280" }}>
          <span>{formatValue(fromSlider(local[0]))}</span>
          <span>–</span>
          <span>{formatValue(fromSlider(local[1]))}</span>
        </div>
      )}
      <SliderPrimitive.Root
        style={{ position: "relative", display: "flex", height: 20, width: "100%", alignItems: "center", userSelect: "none", touchAction: "none" }}
        value={local}
        onValueChange={handleChange}
        onValueCommit={handleCommit}
        min={0} max={1} step={normStep}
        minStepsBetweenThumbs={1}
      >
        <SliderPrimitive.Track style={{ position: "relative", height: 4, flexGrow: 1, borderRadius: 9999, background: "#e2e8f0" }}>
          <SliderPrimitive.Range style={{ position: "absolute", height: "100%", borderRadius: 9999, background: "#94a3b8" }} />
        </SliderPrimitive.Track>
        <SliderPrimitive.Thumb style={thumbStyle} aria-label="Min" />
        <SliderPrimitive.Thumb style={thumbStyle} aria-label="Max" />
      </SliderPrimitive.Root>
    </div>
  );
}
