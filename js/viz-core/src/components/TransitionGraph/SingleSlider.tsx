"use client";
import * as React from "react";
import * as SliderPrimitive from "@radix-ui/react-slider";

interface SingleSliderProps {
  min: number;
  max: number;
  value: number;
  onChange: (value: number) => void;
  scale?: "linear" | "log";
  /** Lower bound of the log scale when min <= 0 (defaults to max * 0.0001);
   *  see RangeSlider's logMin for the full rationale. */
  logMin?: number;
}

/** Single-thumb sibling of RangeSlider — same track/thumb look, same
 *  log-scale math, but fully controlled with no internal debounce: callers
 *  that need to throttle an expensive onChange (backend recompute) do it
 *  themselves, so a live-feedback consumer (e.g. focus dim strength) isn't
 *  forced to lag behind the thumb during a drag. */
export function SingleSlider({
  min, max, value, onChange, scale = "linear", logMin,
}: SingleSliderProps) {
  const toSlider = React.useCallback((v: number) => {
    if (scale === "log" && max > 0) {
      const effMin = min > 0 ? min : (logMin ?? max * 0.0001);
      if (!(max > effMin)) return (v - min) / (max - min || 1);
      if (min <= 0 && v <= 0) return 0;
      const effV = v <= effMin ? effMin : v;
      const pos = (Math.log(effV) - Math.log(effMin)) / (Math.log(max) - Math.log(effMin));
      return min <= 0 ? pos * 0.99 + 0.01 : pos;
    }
    return (v - min) / (max - min || 1);
  }, [scale, min, max, logMin]);

  const fromSlider = React.useCallback((pos: number) => {
    if (scale === "log" && max > 0) {
      const effMin = min > 0 ? min : (logMin ?? max * 0.0001);
      if (!(max > effMin)) return min + pos * (max - min);
      if (min <= 0 && pos <= 0.01) return 0;
      const adj = min <= 0 ? (pos - 0.01) / 0.99 : pos;
      return Math.exp(Math.log(effMin) + adj * (Math.log(max) - Math.log(effMin)));
    }
    // Linear values in this app are always whole steps (percentages, step
    // counts) — round so drag imprecision never surfaces as "19.9999998".
    return Math.round(min + pos * (max - min));
  }, [scale, min, max, logMin]);

  const normStep = scale === "log" ? 0.001 : 1 / (max - min || 1);

  return (
    <SliderPrimitive.Root
      style={{ position: "relative", display: "flex", height: 20, width: "100%", alignItems: "center", userSelect: "none", touchAction: "none" }}
      value={[toSlider(value)]}
      onValueChange={([v]) => onChange(fromSlider(v))}
      min={0} max={1} step={normStep}
    >
      <SliderPrimitive.Track style={{ position: "relative", height: 4, flexGrow: 1, borderRadius: 9999, background: "#e2e8f0" }}>
        <SliderPrimitive.Range style={{ position: "absolute", height: "100%", borderRadius: 9999, background: "#94a3b8" }} />
      </SliderPrimitive.Track>
      <SliderPrimitive.Thumb style={{
        display: "block", width: 8, height: 16, borderRadius: 2,
        background: "#475569", border: "1px solid #94a3b8",
        cursor: "ew-resize", outline: "none",
      }} />
    </SliderPrimitive.Root>
  );
}
