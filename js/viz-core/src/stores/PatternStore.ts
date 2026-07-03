import { makeAutoObservable } from "mobx";

/** Manages the step sankey path pattern as a mutable list of tokens.
 *  Wildcards are represented as ".*".
 *  path_start is never stored — it is prepended in asString. */
export class PatternStore {
  segments: string[] = [];

  constructor(raw?: string | null) {
    makeAutoObservable(this);
    this.load(raw);
  }

  /** Full pattern string sent to Python, e.g. "path_start->.*->cart->.*->path_end" */
  get asString(): string | null {
    if (this.segments.length === 0) return null;
    return ["path_start", ...this.segments].join("->");
  }

  // ── mutations ──────────────────────────────────────────────────────────────

  addLeft(idx: number, token: string)              { this.segments.splice(idx, 0, token);       this._normalize(); }
  addRight(idx: number, token: string)             { this.segments.splice(idx + 1, 0, token);   this._normalize(); }
  replace(idx: number, token: string)              { if (idx >= 0 && idx < this.segments.length) this.segments[idx] = token; this._normalize(); }
  delete(idx: number)                              { if (idx >= 0 && idx < this.segments.length) this.segments.splice(idx, 1); this._normalize(); }
  addLeftWithWildcard(idx: number, token: string)  { this.segments.splice(idx, 0, ".*", token); this._normalize(); }
  addRightWithWildcard(idx: number, token: string) { this.segments.splice(idx + 1, 0, ".*", token); this._normalize(); }
  /** Insert token followed by a gap at idx: gives [..., token, ".*", anchor].
   *  Used for "Gap + Event" from path_end so the new event gets its own matrix. */
  addWithTrailingGap(idx: number, token: string)  { this.segments.splice(idx, 0, token, ".*"); this._normalize(); }

  insertIntoWildcard(idx: number, token: string) {
    if (idx >= 0 && idx < this.segments.length && this.segments[idx] === ".*") {
      this.segments.splice(idx + 1, 0, token, ".*");
      this._normalize();
    }
  }

  // ── load / parse ───────────────────────────────────────────────────────────

  load(raw?: string | null) {
    if (!raw) { this.segments = []; return; }
    const toks = raw.split("->").filter(Boolean);
    let start = toks[0] === "path_start" ? 1 : 0;
    let end   = toks[toks.length - 1] === ".*" ? toks.length - 1 : toks.length;
    this.segments = toks.slice(start, end);
    this._normalize();
  }

  get logicalMatrices(): string[][] {
    const groups: string[][] = [];
    let bucket: string[] = [];
    for (const tok of this.segments) {
      if (tok === ".*") { groups.push(bucket); bucket = []; }
      else               bucket.push(tok);
    }
    groups.push(bucket);
    return groups;
  }

  private _normalize() {
    const out: string[] = [];
    for (const tok of this.segments) {
      if (tok === ".*" && out[out.length - 1] === ".*") continue;
      out.push(tok);
    }
    if (out[out.length - 1] === ".*") out.pop();
    this.segments = out;
  }
}
