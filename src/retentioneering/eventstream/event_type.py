from dataclasses import dataclass, field, fields


@dataclass
class Event:
    name: str
    index: int
    type: str


@dataclass
class EventTypes:
    """The event types a row can carry, and the rank each one sorts at.

    ``index`` is a *sort rank*, not an id: rows sharing a timestamp are ordered
    by it (``schema.subindex``), so the scale says what stands before what
    within one instant. It is deliberately not injective — ``raw`` and
    ``collapsed`` share a rank because collapsing an event does not move it, and
    the distinct type only records where the row came from.

    The scale leaves a slot on either side of ``raw`` because both directions
    are needed: a marker that opens something (``synthetic``) has to precede the
    event it marks, while a marker that closes one (``churn``) has to follow it
    and still stay inside the path's own boundaries.
    """

    PATH_START: Event = field(
        default_factory=lambda: Event(name="path_start", type="path_start", index=0)
    )
    SYNTHETIC_EVENT: Event = field(
        default_factory=lambda: Event(name="synthetic", type="synthetic", index=1)
    )
    RAW_EVENT: Event = field(
        default_factory=lambda: Event(name="raw", type="raw", index=2)
    )
    COLLAPSED_EVENT: Event = field(
        default_factory=lambda: Event(name="collapsed", type="collapsed", index=2)
    )
    CHURN_EVENT: Event = field(
        default_factory=lambda: Event(name="churn", type="churn", index=3)
    )
    PATH_END: Event = field(
        default_factory=lambda: Event(name="path_end", type="path_end", index=4)
    )

    def get_order(self):
        # Keyed by `name`, but `Eventstream.__init__` maps the *event_type*
        # column through it — every entry must keep `name == type`.
        res = {}
        for f in fields(self):
            event_item = getattr(self, f.name)
            res[event_item.name] = event_item.index
        return res
