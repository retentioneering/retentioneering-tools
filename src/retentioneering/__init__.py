from retentioneering import datasets, mcp
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import EventstreamSchema

try:
    from importlib.metadata import version

    __version__ = version("retentioneering")
except Exception:
    __version__ = "unknown"

__all__ = ["Eventstream", "EventstreamSchema", "__version__", "datasets", "mcp"]
