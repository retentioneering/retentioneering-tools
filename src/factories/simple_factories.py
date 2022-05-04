from typing import Callable, Optional

from numpy import mat
from eventstream.eventstream import Eventstream, DELETE_COL_NAME
from eventstream.schema import RawDataSchema, EventstreamSchema
from typing import Callable
import pandas as pd

Filter = Callable[[pd.DataFrame, EventstreamSchema], pd.Series]

def simple_group(event_name: str, filter: Filter, event_type: Optional[str] = "group_alias"):
  def factory(eventstream: Eventstream):
    events = eventstream.to_dataframe()
    mathed_events_q = filter(events, eventstream.schema)
    matched_events = events[mathed_events_q].copy()

    if event_type is not None:
      matched_events[eventstream.schema.event_type] = event_type

    matched_events[eventstream.schema.event_name] = event_name
    matched_events["ref"] = matched_events[eventstream.schema.event_id]

    return Eventstream(
      raw_data = matched_events,
      raw_data_schema = eventstream.schema.to_raw_data_schema(),
      relations=[{ "raw_col": "ref", "evenstream": eventstream }],    
    )

  return factory

def delete_events(filter: Filter):
  def factory(eventstream: Eventstream):
    events = eventstream.to_dataframe()
    mathed_events_q = filter(events, eventstream.schema)
    matched_events = events[mathed_events_q].copy()

    matched_events["ref"] = matched_events[eventstream.schema.event_id]
    
    eventstream = Eventstream(
      raw_data = matched_events,
      raw_data_schema = eventstream.schema.to_raw_data_schema(),
      relations=[{ "raw_col": "ref", "evenstream": eventstream }],    
    )
    eventstream.soft_delete(eventstream.to_dataframe())
    return eventstream

  return factory

