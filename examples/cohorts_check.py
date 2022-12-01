import sys
sys.path.insert(0, '..')
from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema
import pandas as pd
from src.tooling.cohorts import Cohorts

def data():
    df = pd.DataFrame([
        [1, "event1", "2021-12-28 00:01:00"],
        [2, "event1", "2021-12-28 00:01:03"],
        [1, "event1", "2022-01-03 00:01:04"],
        [2, "event2", "2022-01-04 00:01:05"],
        [3, "event1", "2022-01-05 00:02:00"],
        [4, "event1", "2022-01-06 00:03:00"],
        [5, "event1", "2022-01-07 00:07:00"],
        [6, "event2", "2022-01-08 00:08:00"],
        [1, "event2", "2022-01-13 00:01:00"],
        [2, "event3", "2022-01-15 00:01:00"],
        [1, "event3", "2022-02-03 00:01:00"],
        [2, "event4", "2022-02-04 00:01:00"],
        [3, "event2", "2022-02-05 00:02:00"],
        [4, "event4", "2022-02-06 00:03:00"],
        [5, "event3", "2022-02-07 00:07:00"],
        [6, "event3", "2022-02-08 00:08:00"],
        [7, "event1", "2022-02-06 00:03:00"],
        [8, "event1", "2022-02-07 00:07:00"],
        [9, "event2", "2022-02-08 00:08:00"],
        [7, "event2", "2022-03-06 00:03:15"],
        [9, "event1", "2022-03-08 00:08:53"],
        [10, "event2", "2022-03-03 00:01:00"],
        [2, "event5", "2022-04-04 00:01:00"],
        [3, "event4", "2022-04-05 00:02:00"],
        [8, "event3", "2022-04-07 00:07:00"],
        [10, "event4", "2022-04-03 00:01:00"],
    ],

        columns=["user_id", "event", "timestamp"],
    )
    raw_data_schema = RawDataSchema(
        event_name="event", event_timestamp="timestamp", user_id="user_id")

    source = Eventstream(
        raw_data=df,
        raw_data_schema=raw_data_schema,
        schema=EventstreamSchema()
    )
    return source

cohorts = Cohorts(eventstream=data())

cohorts.fit_cohorts(
                 cohort_start_unit="D",
                 cohort_period=(3,"D"), # 3 недели
                  average=False,
                  cut_bottom=0,
                  cut_right=0,
                  cut_diagonal=0)

print(cohorts.values)