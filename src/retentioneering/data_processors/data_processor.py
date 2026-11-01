from typing import Tuple
import pandas as pd
from retentioneering.eventstream.schema import EventstreamSchema


class DataProcessor:
    #: Whether the processor can leave rows out of event order (ADR-0017):
    #: add, merge or re-read rows. Such output is checked, and re-sorted if
    #: needed, when wrapped into an Eventstream. A processor that only drops
    #: rows or edits columns of the frame it was given sets this to False and
    #: costs nothing; the test suite still checks it (`ordering.STRICT`).
    may_break_order = True

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        raise NotImplementedError
