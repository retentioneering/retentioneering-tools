from dataclasses import dataclass, field, fields


@dataclass
class Event:
    name: str
    index: int
    type: str


@dataclass
class EventTypes:
    PATH_START: Event = field(
        default_factory=lambda: Event(name="path_start", type="path_start", index=0)
    )
    RAW_EVENT: Event = field(
        default_factory=lambda: Event(name="raw", type="raw", index=1)
    )
    COLLAPSED_EVENT: Event = field(
        default_factory=lambda: Event(name="collapsed", type="collapsed", index=1)
    )
    SYNTHETIC_EVENT: Event = field(
        default_factory=lambda: Event(name="synthetic", type="synthetic", index=2)
    )
    PATH_END: Event = field(
        default_factory=lambda: Event(name="path_end", type="path_end", index=3)
    )

    def get_order(self):
        res = {}
        for f in fields(self):
            event_item = getattr(self, f.name)
            res[event_item.name] = event_item.index
        return res
