/**
 * Single anywidget entry-point for all Retentioneering widgets.
 * The Python side sets `widget_type` traitlet to choose which component to render.
 *
 * This file is one of only two places in the whole JS workspace that build a
 * `WidgetHost` from something host-specific (the other is AnywidgetHost.ts
 * itself): `render()` wraps the live anywidget model via `anywidgetHost()`;
 * `renderStatic()` wraps the exported data blob via `staticHost()`. Every
 * per-widget render function below only ever sees the resulting `WidgetHost`.
 */
import { anywidgetHost, type AnyWidgetModel } from "./AnywidgetHost";
import { staticHost } from "./StaticHost";
export { restHost } from "./RestHost";
export { reactiveHost } from "./ReactiveHost";
import type { RenderContext } from "./widget-utils";
import { render as renderTransitionGraph }  from "./index";
import { render as renderStepSankey }       from "./step_sankey";
import { render as renderStepMatrix }       from "./step_matrix";
import { render as renderFunnel }           from "./funnel";
import { render as renderSegmentOverview }  from "./segment_overview";
import { render as renderClusterAnalysis }  from "./cluster_analysis";

/** Apply a GraphView (object, or a name from the widget's `views` list) to
 *  a rendered transition graph. No-op for widgets that don't expose the
 *  GraphView pipeline. */
export function applyView(view: unknown, el: HTMLElement) {
  const applier = (
    el as HTMLElement & { __applyGraphView?: (v: unknown) => void }
  ).__applyGraphView;
  if (applier) applier(view);
}

/** Focus a node or an edge in a transition graph after renderStatic.
 *  eventRef can be:
 *    "basket"           → focus node (dim the rest, fit its neighborhood)
 *    "basket->purchase" → focus edge (dim the rest, fit the pair)
 *  Prefers the GraphView pipeline when the widget exposes it; the legacy
 *  cy-based path remains as a fallback for older embeds.
 */
export function focusNode(eventRef: string, el: HTMLElement) {
  const applier = (
    el as HTMLElement & { __applyGraphView?: (v: unknown) => void }
  ).__applyGraphView;
  if (applier) {
    const arrowIdx = eventRef.indexOf("->");
    if (arrowIdx !== -1) {
      applier({
        focus: {
          type: "edge",
          source: eventRef.slice(0, arrowIdx).trim(),
          target: eventRef.slice(arrowIdx + 2).trim(),
        },
      });
    } else {
      applier({ focus: { type: "node", event: eventRef } });
    }
    return;
  }
  const findCy = (node: Element): any => {
    if ((node as any).__cy) return (node as any).__cy;
    for (let i = 0; i < node.children.length; i++) {
      const found = findCy(node.children[i]);
      if (found) return found;
    }
    return null;
  };
  const cy = findCy(el);
  if (!cy) return;

  const arrowIdx = eventRef.indexOf("->");
  if (arrowIdx !== -1) {
    // Edge reference
    const source = eventRef.slice(0, arrowIdx).trim();
    const target = eventRef.slice(arrowIdx + 2).trim();
    const edge = cy.edges(`[source = "${source}"][target = "${target}"]`);
    if (!edge || !edge.length) return;

    // Stop any previous edge animation on this cy instance
    if ((cy as any).__stopEdgeAnim) (cy as any).__stopEdgeAnim();

    cy.animate({ fit: { eles: edge, padding: 120 } }, { duration: 400 });

    // Use the edge's own colour so dashes match the edge
    const origColor      = edge.style("line-color") as string;
    const origArrowColor = edge.style("target-arrow-color") as string;
    const origWidth      = (edge.numericStyle("width") as number) || 2;
    edge.style({
      "line-style": "dashed",
      "line-dash-pattern": [10, 6],
      "line-dash-offset": 0,
      "line-color": origColor,
      "target-arrow-color": origArrowColor,
      "width": Math.max(origWidth, 2),
    });

    let offset = 0;
    const iv = setInterval(() => {
      offset = (offset + 1) % 32;   // slower: +1 step
      edge.style("line-dash-offset", -offset);
    }, 60);                          // slower: 60 ms interval

    const stopAnim = () => {
      clearInterval(iv);
      edge.removeStyle();
      cy.off("tap", tapHandler);
      (cy as any).__stopEdgeAnim = null;
    };
    // Stop when user clicks empty canvas
    const tapHandler = (event: any) => { if (event.target === cy) stopAnim(); };
    cy.on("tap", tapHandler);
    (cy as any).__stopEdgeAnim = stopAnim;
  } else {
    // Node reference — fit the node together with its visible neighborhood so
    // none of its edges end outside the viewport (fitting just the node
    // over-zooms and cuts the focused edges off).
    const node = cy.getElementById(eventRef);
    if (node && node.length) {
      const edges = node
        .connectedEdges()
        .filter((edge: any) => !edge.hasClass("filtered"));
      const neighborhood = edges.union(edges.connectedNodes()).union(node);
      cy.animate({ fit: { eles: neighborhood, padding: 80 } }, { duration: 400 });
      node.emit("tap");
    }
  }
}

/** Scroll to a row or specific cell in a static step matrix.
 *  eventRef can be:
 *    "basket"    → scroll to row "basket"
 *    "basket@4"  → expand step_window if needed, scroll to cell at step 4
 */
export function scrollToEvent(eventRef: string, el: HTMLElement) {
  let eventName = eventRef;
  let step: number | null = null;

  const atIdx = eventRef.indexOf("@");
  if (atIdx !== -1) {
    eventName = eventRef.slice(0, atIdx).trim();
    const parsed = parseInt(eventRef.slice(atIdx + 1), 10);
    if (!isNaN(parsed)) step = parsed;
  }

  // Use __matrixApi when available (step matrix — also expands step_window)
  const matrixApi = (el as any).__matrixApi;
  if (matrixApi && step !== null) {
    matrixApi.scrollToCell(eventName, step);
    return;
  }

  // Use __segmentApi when available (segment overview)
  const segApi = (el as any).__segmentApi;
  if (segApi) {
    if (step !== null) {
      // step is actually a segment value encoded via parseInt — won't match, skip
    } else if (atIdx !== -1) {
      // eventRef was "metric@segment" — step is null means segment wasn't a number
      const segValue = eventRef.slice(atIdx + 1).trim();
      segApi.focusCell(eventName, segValue);
    } else {
      segApi.focusAny(eventName);
    }
    return;
  }

  // DOM fallback
  const rows = el.querySelectorAll("tr[data-event]");
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i] as HTMLElement;
    if (row.dataset.event === eventName) {
      if (step !== null) {
        const cell = row.querySelector(`td[data-step="${step}"]`) as HTMLElement | null;
        if (cell) {
          cell.scrollIntoView({ block: "nearest", inline: "center", behavior: "smooth" });
          const prev = cell.style.background;
          cell.style.background = "#fef3c7";
          setTimeout(() => { cell.style.background = prev; }, 900);
          return;
        }
      }
      row.scrollIntoView({ block: "nearest", behavior: "smooth" });
      const td = row.querySelector("td") as HTMLElement | null;
      if (td) {
        const prev = td.style.background;
        td.style.background = "#fef3c7";
        setTimeout(() => { td.style.background = prev; }, 900);
      }
      break;
    }
  }
}

/** Dispatch by `widget_type` to the right per-widget renderer. Shared by both render() and renderStatic(). */
function dispatch(ctx: RenderContext) {
  const type = (ctx.host.get("widget_type") as string) ?? "transition_graph";
  if (type === "step_sankey")       return renderStepSankey(ctx);
  if (type === "step_matrix")       return renderStepMatrix(ctx);
  if (type === "funnel")            return renderFunnel(ctx);
  if (type === "segment_overview")  return renderSegmentOverview(ctx);
  if (type === "cluster_analysis")  return renderClusterAnalysis(ctx);
  return renderTransitionGraph(ctx);
}

/** Static-HTML-export entry point (ADR-0010) — no kernel, no live model. */
export function renderStatic(data: Record<string, unknown>, el: HTMLElement) {
  return dispatch({ host: staticHost(data), el, isStatic: true });
}

/**
 * Generic entry point for any `WidgetHost` implementation (e.g. `restHost`
 * against a platform backend) — the third transport alongside anywidget
 * (`render`) and static export (`renderStatic`). Same `dispatch()`, just
 * without assuming which host built it.
 */
export function renderWithHost(host: RenderContext["host"], el: HTMLElement, isStatic = false) {
  return dispatch({ host, el, isStatic });
}

/** anywidget's ESM entry point — called with the live model by the anywidget runtime. */
export function render(ctx: { model: AnyWidgetModel; el: HTMLElement; isStatic?: boolean }) {
  return dispatch({ host: anywidgetHost(ctx.model), el: ctx.el, isStatic: ctx.isStatic });
}
