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
