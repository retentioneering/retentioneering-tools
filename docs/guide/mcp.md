# MCP Server (beta version)

retentioneering ships a built-in [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server that exposes your eventstream to any MCP-compatible AI agent. Once connected, the agent can analyse your behavioural data by calling [tools](#available-tools) — you simply ask questions in plain language.

The MCP server runs **locally** in the Jupyter kernel, so it requires a local Python environment. It will not work in cloud notebooks such as Google Colab.

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

## Available tools

| Tool | Description |
|---|---|
| `describe()` | Schema, event list, path counts, timestamp range. |
| `reset_base_stream()` | Reset the active stream to the original eventstream passed to `serve()`. |
| `playbook(scenario)` | Step-by-step recipes for common analysis patterns. |
| `describe_tool(tool)` | Full parameter reference for any preprocessor. |
| `update_base_stream(preprocessors)` | Filter or transform the stream for the session. |
| `add_transition_graph(label, ...)` | Compute a transition graph and register it as a report tab. |
| `add_step_matrix(label, ...)` | Compute a step matrix and register it as a report tab. |
| `add_segment_overview(label, ...)` | Compute a segment overview and register it as a report tab. |
| `check_analysis(analysis)` | Validate analysis text before export. |
| `export_report(title, analysis, path)` | Generate a self-contained HTML report. |
