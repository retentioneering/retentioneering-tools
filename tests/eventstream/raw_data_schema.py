from dataclasses import asdict

from retentioneering.eventstream import RawDataSchema


class TestRawDataSchema:
    def test_create_schema(self):
        schema = RawDataSchema()
        schema_data = asdict(schema)
        assert {
            "custom_cols": [],
            "event_name": "event",
            "event_timestamp": "timestamp",
            "event_type": None,
            "user_id": "user_id",
        } == schema_data

    def test_create_schema__custom_field(self):
        schema_input_data = {"event_name": "action"}
        schema = RawDataSchema(**schema_input_data)
        schema_data = asdict(schema)
        assert {
            "custom_cols": [],
            "event_name": "action",
            "event_timestamp": "timestamp",
            "event_type": None,
            "user_id": "user_id",
        } == schema_data

    def test_create_schema__custom_cols(self):
        schema_input_data = {"custom_cols": [{"raw_data_col": "col1", "custom_col": "col1"}]}
        schema = RawDataSchema(**schema_input_data)
        schema_data = asdict(schema)
        assert {
            "custom_cols": [{"raw_data_col": "col1", "custom_col": "col1"}],
            "event_name": "event",
            "event_timestamp": "timestamp",
            "event_type": None,
            "user_id": "user_id",
        } == schema_data
