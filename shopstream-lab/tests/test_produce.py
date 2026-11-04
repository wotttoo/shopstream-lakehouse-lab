"""Unit test cho producer/produce.py, không cần Kafka."""

import importlib.util
import json
from pathlib import Path

from common import config

spec = importlib.util.spec_from_file_location(
    "produce", Path(config.PROJECT_DIR) / "producer" / "produce.py"
)
produce = importlib.util.module_from_spec(spec)
spec.loader.exec_module(produce)

SAMPLE_LINES = [
    {"event_id": "e1", "user_id": "U001", "event_type": "view", "product_id": "P01",
     "amount": None, "event_time": "2026-01-15T10:00:01Z", "batch_no": 1,
     "send_time": "2026-01-15T10:00:02.000Z"},
    {"event_id": "e2", "user_id": "U002", "event_type": "purchase", "product_id": "P02",
     "amount": 120000, "event_time": "2026-01-15T10:06:00Z", "batch_no": 2,
     "send_time": "2026-01-15T10:06:01.500Z"},
    {"event_id": "e1", "user_id": "U001", "event_type": "view", "product_id": "P01",
     "amount": None, "event_time": "2026-01-15T10:00:01Z", "batch_no": 2,
     "send_time": "2026-01-15T10:05:10.000Z"},
]


def test_value_has_no_batch_no_or_send_time():
    for line in SAMPLE_LINES:
        _, value = produce.to_kafka_message(line)
        event = json.loads(value)
        assert "batch_no" not in event
        assert "send_time" not in event
        assert list(event) == ["event_id", "user_id", "event_type", "product_id",
                               "amount", "event_time"]


def test_value_keeps_event_fields_unchanged():
    _, value = produce.to_kafka_message(SAMPLE_LINES[1])
    assert json.loads(value) == {
        "event_id": "e2", "user_id": "U002", "event_type": "purchase",
        "product_id": "P02", "amount": 120000, "event_time": "2026-01-15T10:06:00Z",
    }


def test_key_is_user_id():
    key, _ = produce.to_kafka_message(SAMPLE_LINES[0])
    assert key == b"U001"


def test_select_batch_filters_and_keeps_order():
    batch_two = produce.select_batch(SAMPLE_LINES, 2)
    assert [line["event_id"] for line in batch_two] == ["e2", "e1"]
    assert len(produce.select_batch(SAMPLE_LINES, None)) == 3
    assert produce.select_batch(SAMPLE_LINES, 5) == []


def test_load_events_reads_jsonl_in_order(tmp_path):
    events_file = tmp_path / "events.jsonl"
    events_file.write_text("".join(json.dumps(line) + "\n" for line in SAMPLE_LINES))
    assert produce.load_events(events_file) == SAMPLE_LINES
