/** mulberry32 — tiny deterministic PRNG. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Run `fn` with Math.random temporarily replaced by a seeded PRNG.
 *
 * The fcose layout draws Math.random() inside cose-base even with
 * randomize:false (tree reduction / constraint relaxation), so swapping the
 * PRNG for the duration of the synchronous layout.run() is what makes the
 * layout bit-for-bit reproducible. Only safe for synchronous `fn`.
 */
export function withSeededRandom<T>(seed: number, fn: () => T): T {
  const original = Math.random;
  Math.random = mulberry32(seed);
  try {
    return fn();
  } finally {
    Math.random = original;
  }
}
