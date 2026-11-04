"""Unit test cho data/generate_events.py. Không cần Kafka, Spark hay mạng.

Kỳ vọng lấy từ đặc tả (mục 8 của CLAUDE.md) hoặc ví dụ tính tay.
"""

import importlib.util
import json
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from common import config

GENERATOR_PATH = Path(config.PROJECT_DIR) / "data" / "generate_events.py"
spec = importlib.util.spec_from_file_location("generate_events", GENERATOR_PATH)
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)

START = pd.Timestamp("2026-01-15T10:00:00Z")
END = pd.Timestamp("2026-01-15T11:00:00Z")


@pytest.fixture(scope="module")
def base_events():
    rng = np.random.default_rng(42)
    return gen.generate_base_events(rng)


@pytest.fixture(scope="module")
def records():
    return gen.generate_records(42)


@pytest.fixture(scope="module")
def output_dir(tmp_path_factory):
    directory = tmp_path_factory.mktemp("generated")
    gen.write_outputs(directory, 42)
    return directory


@pytest.fixture(scope="module")
def lines(output_dir):
    with open(output_dir / "events.jsonl", encoding="utf-8") as events_file:
        return [json.loads(line) for line in events_file]


# ---------------------------------------------------------------- task 10
def test_base_events_count_users_products(base_events):
    assert len(base_events) == 2000
    users = {event["user_id"] for event in base_events}
    products = {event["product_id"] for event in base_events}
    assert users == {f"U{n:03d}" for n in range(1, 51)}
    assert products == {f"P{n:02d}" for n in range(1, 21)}


def test_base_events_type_ratio_near_70_20_10(base_events):
    types = pd.Series([event["event_type"] for event in base_events])
    shares = types.value_counts(normalize=True)
    assert set(shares.index) == {"view", "add_to_cart", "purchase"}
    assert abs(shares["view"] - 0.70) <= 0.03
    assert abs(shares["add_to_cart"] - 0.20) <= 0.03
    assert abs(shares["purchase"] - 0.10) <= 0.03


def test_base_events_amount_rules(base_events):
    for event in base_events:
        if event["event_type"] == "purchase":
            assert 50_000 <= event["amount"] <= 500_000
            assert event["amount"] % 1_000 == 0
        else:
            assert event["amount"] is None


def test_base_events_time_within_one_hour(base_events):
    for event in base_events:
        event_time = gen.ms_to_timestamp(event["event_time_ms"])
        assert START <= event_time < END


def test_base_events_ids_are_seeded_unique_uuids(base_events):
    ids = [event["event_id"] for event in base_events]
    assert len(set(ids)) == len(ids)
    for event_id in ids[:20]:
        assert str(uuid.UUID(event_id)) == event_id
    again = gen.generate_base_events(np.random.default_rng(42))
    assert again == base_events


# ---------------------------------------------------------------- task 11
def split_originals_and_duplicates(records):
    originals = [record for record in records
                 if not record["is_duplicate"] and record["delay_group"] != "watermark_push"]
    duplicates = [record for record in records if record["is_duplicate"]]
    return originals, duplicates


def test_duplicates_rate_about_10_percent(records):
    originals, duplicates = split_originals_and_duplicates(records)
    rate = len(duplicates) / len(originals)
    assert 0.09 <= rate <= 0.11
    # Mỗi event_id trùng tối đa một bản thêm
    duplicate_ids = [record["event_id"] for record in duplicates]
    assert len(set(duplicate_ids)) == len(duplicate_ids)


def test_duplicates_about_one_fifth_shifted_by_few_seconds(records):
    originals, duplicates = split_originals_and_duplicates(records)
    by_id = {record["event_id"]: record for record in originals}
    shifted = [record for record in duplicates
               if record["event_time_ms"] != by_id[record["event_id"]]["event_time_ms"]]
    share = len(shifted) / len(duplicates)
    assert 0.15 <= share <= 0.25
    for record in shifted:
        shift_ms = abs(record["event_time_ms"] - by_id[record["event_id"]]["event_time_ms"])
        assert 1_000 <= shift_ms <= 5_000


def test_duplicates_keep_event_id_and_other_fields(records):
    originals, duplicates = split_originals_and_duplicates(records)
    by_id = {record["event_id"]: record for record in originals}
    for record in duplicates:
        original = by_id[record["event_id"]]
        for field in ("user_id", "event_type", "product_id", "amount"):
            assert record[field] == original[field]


def test_duplicates_sent_after_original(records):
    position = {}
    for index, record in enumerate(records):
        position.setdefault(record["event_id"], index)
    for index, record in enumerate(records):
        if record["is_duplicate"]:
            assert position[record["event_id"]] < index


# ---------------------------------------------------------------- task 12
def originals_only(records):
    return [record for record in records
            if not record["is_duplicate"] and record["delay_group"] != "watermark_push"]


def test_late_within_rate_and_delay_range(records):
    originals = originals_only(records)
    within = [record for record in originals if record["delay_group"] == "late_within"]
    assert 0.13 <= len(within) / len(originals) <= 0.17
    for record in within:
        assert 60_000 <= record["delay_ms"] <= 240_000


def test_late_beyond_rate_delay_and_purchases(records):
    originals = originals_only(records)
    beyond = [record for record in originals if record["delay_group"] == "late_beyond"]
    assert 0.02 <= len(beyond) / len(originals) <= 0.03
    assert sum(record["event_type"] == "purchase" for record in beyond) >= 15
    for record in beyond:
        assert 12 * 60_000 <= record["delay_ms"] <= 20 * 60_000


def test_late_send_time_is_event_time_plus_delay(records):
    for record in originals_only(records):
        assert record["send_time_ms"] == record["event_time_ms"] + record["delay_ms"]


def test_late_on_time_events_have_delay_under_one_minute(records):
    for record in originals_only(records):
        if record["delay_group"] == "on_time":
            assert 0 <= record["delay_ms"] < 60_000
