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


# ---------------------------------------------------------------- task 13
def test_batches_cover_1_to_12_and_match_send_window(lines):
    assert {line["batch_no"] for line in lines} == set(range(1, 13))
    for line in lines:
        send_time = pd.Timestamp(line["send_time"])
        batch_start = START + pd.Timedelta(minutes=5 * (line["batch_no"] - 1))
        assert batch_start <= send_time < batch_start + pd.Timedelta(minutes=5)


def test_batches_lines_sorted_by_send_time(lines):
    send_times = [pd.Timestamp(line["send_time"]) for line in lines]
    assert send_times == sorted(send_times)


def test_batches_lines_have_exactly_eight_fields(lines):
    expected = {"event_id", "user_id", "event_type", "product_id", "amount",
                "event_time", "batch_no", "send_time"}
    for line in lines:
        assert set(line) == expected


def test_batches_watermark_push_event_in_last_batch(lines):
    push = [line for line in lines if pd.Timestamp(line["event_time"]) >= END]
    assert len(push) == 1
    assert push[0]["event_type"] == "view"
    assert push[0]["batch_no"] == 12
    push_time = pd.Timestamp(push[0]["event_time"])
    assert abs(push_time - pd.Timestamp("2026-01-15T11:30:00Z")) <= pd.Timedelta(minutes=5)


# ---------------------------------------------------------------- task 14
def hand_example():
    """Ví dụ tính tay, ngưỡng 10 phút, cửa sổ 5 phút.

    Đợt 1: view 10:00, purchase 10:02 (100), view 10:14  -> watermark sau = 10:04
    Đợt 2: purchase 10:03 (200): cửa sổ kết thúc 10:05 > 10:04, giữ.
           view 10:19                                     -> watermark sau = 10:09
           (view đẩy watermark: nếu chỉ tính purchase thì watermark chỉ là 09:53)
    Đợt 3: purchase 10:04 (300): cửa sổ kết thúc 10:05 <= 10:09, LOẠI.
           purchase 10:07 (400): cửa sổ kết thúc 10:10 > 10:09, GIỮ
           (nếu loại theo event time thì 10:07 <= 10:09 sẽ bị loại)
                                                          -> watermark sau = 10:09
    Đợt 4: view 10:30                                     -> watermark sau = 10:20
    Kết quả: [10:00, 10:05) doanh thu 300, 2 đơn, chốt ở đợt 2;
             [10:05, 10:10) doanh thu 400, 1 đơn, chốt ở đợt 4.
    """
    rows = [
        (1, "view", None, "10:00:00"),
        (1, "purchase", 100, "10:02:00"),
        (1, "view", None, "10:14:00"),
        (2, "purchase", 200, "10:03:00"),
        (2, "view", None, "10:19:00"),
        (3, "purchase", 300, "10:04:00"),
        (3, "purchase", 400, "10:07:00"),
        (4, "view", None, "10:30:00"),
    ]
    return pd.DataFrame(
        {
            "batch_no": [row[0] for row in rows],
            "event_type": [row[1] for row in rows],
            "amount": [row[2] for row in rows],
            "event_time": pd.to_datetime([f"2026-01-15T{row[3]}Z" for row in rows], utc=True),
        }
    )


def t(clock):
    return pd.Timestamp(f"2026-01-15T{clock}Z")


def test_watermark_simulation_batch_watermarks():
    _, batch_watermarks = gen.simulate_watermark(hand_example(), 10, 5, batch_count=4)
    assert pd.isna(batch_watermarks["watermark_before"].iloc[0])
    assert list(batch_watermarks["watermark_after"]) == [t("10:04:00"), t("10:09:00"),
                                                         t("10:09:00"), t("10:20:00")]


def test_watermark_simulation_drops_by_window_end():
    simulated, _ = gen.simulate_watermark(hand_example(), 10, 5, batch_count=4)
    dropped = simulated[simulated["dropped_by_watermark"]]
    assert list(dropped["amount"]) == [300]


def test_watermark_simulation_window_totals_and_close_batch():
    simulated, batch_watermarks = gen.simulate_watermark(hand_example(), 10, 5, batch_count=4)
    windows = gen.aggregate_gold(simulated, batch_watermarks)
    assert list(windows.index) == [t("10:00:00"), t("10:05:00")]
    assert list(windows["revenue"]) == [300, 400]
    assert list(windows["orders"]) == [2, 1]
    assert list(windows["close_batch"]) == [2, 4]
    assert list(windows["dropped_purchases"]) == [1, 0]


def test_watermark_simulation_wider_threshold_drops_nothing():
    simulated, _ = gen.simulate_watermark(hand_example(), 20, 5, batch_count=4)
    assert not simulated["dropped_by_watermark"].any()
