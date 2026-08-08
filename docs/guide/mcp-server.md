# MCP Server (beta version)

retentioneering ships a built-in [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server that exposes your eventstream to any MCP-compatible AI agent. Once connected, the agent can analyse your behavioural data by calling [tools](#available-tools) — you simply ask questions in plain language.

The MCP server runs **locally** in the Jupyter kernel, so it requires a local Python environment. It will not work in cloud notebooks such as Google Colab.

There is a second, unrelated MCP server that serves *this documentation* rather than your data, needs no installation, and is described at the bottom of this page: [Documentation MCP server](#documentation-mcp-server).

## Starting the server

You can either start the server with no context so the agent will have to upload the eventstream itself to analyse it:
```python
import retentioneering as rete

rete.mcp.serve(port=8765)
```

or with context to analyse an eventstream that is already in the kernel's memory:

```python
import retentioneering as rete

stream = rete.datasets.load_ecom()
rete.mcp.serve(stream, context={
    "description": "E-commerce store. Main KPI — purchase conversion.",
    "events": {
        "purchase":    "Completed a purchase",
        "add_to_cart": "Added an item to cart",
    }
  },
  port=8765
)
```

The `stream` argument is the [Eventstream](/docs/eventstream) you want the agent to analyse. It stays in the kernel's memory — the agent reads from it via tool calls without copying data anywhere. The optional `context` dict adds semantic information that the agent uses to write better analysis.

## Connecting an agent

Any agent that supports MCP over SSE can connect using the server URL `http://localhost:8765/sse` (adjust the port if you changed it). Setup instructions for popular agents:

- [Claude Code](https://docs.anthropic.com/en/docs/claude-code/mcp) — run `claude mcp add --scope user --transport sse retentioneering http://localhost:8765/sse`
- [Codex](https://developers.openai.com/codex/mcp) — add the server URL to Settings → MCP Servers → Add Server (Streamable HTTP)
- [Cursor](https://cursor.com/docs/mcp) — add the server under Settings → Tools & MCPs → New MCP Server

  ```json
  {
    "mcpServers": {
      "retentioneering": {
        "url": "http://localhost:8765/sse"
      }
    }
  }
  ```

## Example questions

Once connected, you can ask the agent questions like:

- What are the most common user flows in this data?
- Why was there a spike in conversion around January 20-22?
- Why do users drop-off between `basket` and `shipping_details` in the checkout funnel?
- How do new and loyal users behave differently?
- Build a transition graph and analyse the main bottlenecks.

The agent will call the appropriate tools, build visualisations, and generate a self-contained HTML report with interactive charts and clickable annotations.

See also [Agent Skills](/docs/agent-skills) — instruction packages for coding agents (Claude Code, Codex) that run analyses directly in your own code, rather than over MCP.

## Available tools

| Tool | Description |
|---|---|
| `load_data(path, schema, context)` | Load an eventstream from a local CSV and set it as the session's base stream — required first call if `serve()` was started with no `stream` (data-agnostic mode); also usable later to switch datasets. |
| `describe()` | Schema, event list, path counts, timestamp range. |
| `reset_base_stream()` | Reset the active stream to the original eventstream passed to `serve()`. |
| `playbook(scenario)` | Step-by-step recipes for common analysis patterns. |
| `describe_tool(tool)` | Full parameter reference for any preprocessor or analysis tool. |
| `update_base_stream(preprocessors)` | Filter or transform the stream for the session. |
| `add_transition_graph(label, ...)` | Compute a transition graph and register it as a report tab. |
| `add_step_matrix(label, ...)` | Compute a step matrix and register it as a report tab. |
| `add_segment_overview(label, ...)` | Compute a segment overview and register it as a report tab. |
| `get_conversion_rate(start_event, end_event, within, ...)` | How often one event is followed by another, with the denominator and the baseline lift. Returns numbers, not a tab — the agent quotes them in backticks. |
| `check_analysis(analysis)` | Validate analysis text before export. |
| `export_report(title, analysis, path)` | Generate a self-contained HTML report. |

## Documentation MCP server

Everything above describes the server that runs **on your machine, over your
data**. There is a second one that runs **on our side, over this
documentation**:

```
https://retentioneering.com/docs/mcp
```

It needs no installation, no kernel and no data — connect it to any MCP client
and the agent can search and read these pages while it writes retentioneering
code.

Connecting is the same procedure as [above](#connecting-an-agent), with the
URL instead of `localhost` — for example, in Claude Code:

```bash
claude mcp add --scope user --transport http retentioneering-docs https://retentioneering.com/docs/mcp
```

Three tools are available: `search_docs(query, limit)` returns the most relevant
documentation sections with deep links, `get_doc_page(path)` returns one page in
full, and `list_doc_pages()` returns the table of contents.

If you would rather not connect a server at all, the same content is published
as plain text following the [llms.txt convention](https://llmstxt.org):
[`/llms.txt`](https://retentioneering.com/llms.txt) is the annotated table of
contents, and [`/llms-full.txt`](https://retentioneering.com/llms-full.txt) is
the entire documentation as one file (~200 KB, roughly 50k tokens — small
enough to hand to an agent wholesale).
