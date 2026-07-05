# Data Processors

Data processors transform an [Eventstream](/docs/eventstream) and return a new one. They are available as methods on the `Eventstream` object. In the examples throughout this section, `stream` refers to an `Eventstream` instance.

Processors can be chained:

```python
stream = (
    Eventstream(df)
    .add_start_end_events()
    .filter_events(by_column={"column": "event", "values": ["bot_visit"], "exclude": True})
    .rename_events({"btn_clk": "button_click"})
)
```

Each processor returns a new `Eventstream`, so the original is never modified.
