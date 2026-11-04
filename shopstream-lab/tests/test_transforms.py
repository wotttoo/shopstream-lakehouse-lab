"""Test cho common/transforms.py.

Ngoại lệ duy nhất của quy ước unit test: dùng Spark local (không cần Kafka, mạng
hay jar ngoài). Spark khởi động không có cấu hình Delta.
"""

import datetime as dt

import pytest
from pyspark.sql import SparkSession

from common.transforms import count_rejected, keep_valid, parse_events


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_transforms")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def bronze_rows(spark, values):
    rows = [(value, 0, offset, dt.datetime(2026, 1, 15, 10, 0, 0)) for offset, value in enumerate(values)]
    return spark.createDataFrame(rows, "value string, kafka_partition int, kafka_offset long, kafka_timestamp timestamp")


GOOD = ('{"event_id": "e1", "user_id": "U001", "event_type": "purchase", "product_id": "P01", '
        '"amount": 120000, "event_time": "2026-01-15T10:03:27Z"}')
NO_ID = ('{"user_id": "U002", "event_type": "view", "product_id": "P02", "amount": null, '
         '"event_time": "2026-01-15T10:04:00Z"}')
BAD_TIME = ('{"event_id": "e3", "user_id": "U003", "event_type": "view", "product_id": "P03", '
            '"amount": null, "event_time": "not-a-time"}')
NOT_JSON = "this is not json"


def test_parse_converts_event_time_to_utc_timestamp(spark):
    parsed = parse_events(bronze_rows(spark, [GOOD]))
    row = parsed.collect()[0]
    assert row.event_id == "e1"
    assert row.amount == 120000
    assert row.is_valid
    # Phiên làm việc UTC nên giờ hiển thị đúng 10:03:27
    as_text = parsed.selectExpr("date_format(event_time, 'yyyy-MM-dd HH:mm:ss') AS t").collect()[0].t
    assert as_text == "2026-01-15 10:03:27"


def test_parse_flags_missing_id_and_bad_time_as_rejected(spark):
    parsed = parse_events(bronze_rows(spark, [GOOD, NO_ID, BAD_TIME, NOT_JSON]))
    assert count_rejected(parsed) == 3
    valid = keep_valid(parsed)
    assert [row.event_id for row in valid.collect()] == ["e1"]
    assert "is_valid" not in valid.columns

