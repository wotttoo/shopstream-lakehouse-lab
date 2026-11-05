from pyspark.sql.types import LongType, StringType

from common.schema import EVENT_FIELDS, EVENT_SCHEMA


def test_schema_has_six_fields_in_order():
    assert EVENT_SCHEMA.fieldNames() == [
        "event_id", "user_id", "event_type", "product_id", "amount", "event_time",
    ]
    assert EVENT_FIELDS == EVENT_SCHEMA.fieldNames()


def test_event_time_is_string_and_amount_is_long():
    fields = {field.name: field.dataType for field in EVENT_SCHEMA.fields}
    assert fields["event_time"] == StringType()
    assert fields["amount"] == LongType()


def test_schema_has_no_batch_or_send_time():
    assert "batch_no" not in EVENT_SCHEMA.fieldNames()
    assert "send_time" not in EVENT_SCHEMA.fieldNames()
