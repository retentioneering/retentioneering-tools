from typing import Tuple
import pandas as pd
from retentioneering.eventstream.schema import EventstreamSchema


class DataProcessor:
    def apply(self, df: pd.DataFrame, schema: EventstreamSchema) -> Tuple[pd.DataFrame, EventstreamSchema]:
        raise NotImplementedError
