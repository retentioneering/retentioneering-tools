# ADR-0014: Agent-facing documentation surface

Status: Accepted (recorded 2026-07)

## Context

ADR-0013 made docstrings the source of truth for reference docs and pushed
conceptual depth — mental model, toy examples, figures, pitfalls — out of the
docstring and into the page template, so `help()` and the MCP tool listing stay
short. That trade has a consequence nobody had to pay until agents started
writing retentioneering code: the conceptual layer exists **only** on the docs
site. An agent using the library's own MCP server (ADR-0009) sees the short
docstrings and nothing else, and an agent that has not installed the library at
all sees whatever it remembers — which is 3.x, since that is what the public
internet is full of (ADR-0012 lists what no longer exists).

The docs are also small: 37 pages, ~200 KB of markdown, roughly 50k tokens for
the entire corpus. That number decides the design more than anything else.

## Decision

- **Two plain-text artifacts, generated with the rest of the build.**
  `render_pages.py` writes `docs/build/llms.txt` (annotated table of contents,
  per [llmstxt.org](https://llmstxt.org)) and `docs/build/llms-full.txt` (the
  whole corpus in one file). They are build output like every other page — the
  descriptions in `llms.txt` are each page's own opening sentence, and
  `GUIDE_DESCRIPTIONS` overrides only the pages that open on a heading or a
  hook. A guide page missing from the `GUIDES` order list fails the build
  rather than silently vanishing from the index.
- **`llms-full.txt` is the corpus, and everything downstream reads it** rather
  than re-walking `docs/build/`. Site-relative links become absolute, and
  `<DemoWidget>` tags — a live widget on the site, meaningless to a reader —
  are replaced by the python they run. One artifact, so the site, the text
  files and the MCP server cannot disagree about what a page says.
- **A documentation MCP server at `https://retentioneering.com/docs/mcp`**,
  implemented in retentioneering-web (`app/docs/mcp/route.ts`), stateless and
  unauthenticated because it only ever serves public text. Three read-only
  tools: `search_docs`, `get_doc_page`, `list_doc_pages`. It is distinct from
  the library's own MCP server (ADR-0009), which is local, needs a kernel and
  operates on the user's data; the guide page at `/docs/mcp-server` explains
  the split. The `/docs/mcp` path was freed for it — the guide that used to
  live there moved, with no redirect left behind.
- **Search is lexical, not semantic.** Term frequency with field boosts over
  sections split at `##`. At 37 pages of one library's vocabulary, a query is
  almost always a method or concept name that appears literally in the text;
  embeddings would add an index to build, a model to call and non-determinism,
  for recall we do not need.
- **Streamable HTTP is implemented directly**, not through a transport
  library. `@modelcontextprotocol/sdk`'s server transport is built on Node's
  `req`/`res`, which App Router route handlers do not have, and the npm package
  named `mcp-handler` is not the Vercel adapter it appears to be. For three
  read-only tools the protocol surface is `initialize` + `tools/list` +
  `tools/call`.

## Consequences

- Adding a guide page means adding it to `GUIDES` in `render_pages.py` — the
  build says so if you forget.
- Every docs page is prerendered, so their content reads happen at build time;
  `/docs/mcp` is the one route that reads `content/docs/` at **runtime**, and
  needs `outputFileTracingIncludes` in `next.config.ts` to have the file in its
  deployment at all. Without it the route deploys fine and every tool call
  comes back empty.
- The section anchors in search results are produced by re-implementing
  github-slugger's rules; they match because our headings are plain prose.
  A heading with unusual punctuation could yield a link that lands on the
  right page but not the right section.
- Because the corpus fits in a context window, `llms-full.txt` and the MCP
  server are genuinely alternative front doors rather than a fallback and a
  real answer. If the docs grow several times over, that stops being true and
  the search tool becomes the primary path.
