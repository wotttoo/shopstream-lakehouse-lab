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


# ---------------------------------------------------------------- task 15
@pytest.fixture(scope="module")
def golden(output_dir):
    with open(output_dir / "golden_answer.json", encoding="utf-8") as golden_file:
        return json.load(golden_file)


def test_golden_answer_record_counts_are_consistent(golden, lines):
    assert golden["total_records_sent"] == len(lines)
    assert golden["unique_event_ids"] == len({line["event_id"] for line in lines})
    # Tổng = số event_id duy nhất + số bản trùng (sự kiện đẩy watermark được tính trong cả hai)
    assert golden["total_records_sent"] == golden["unique_event_ids"] + golden["duplicate_records"]
    assert golden["duplicate_records"] == (golden["duplicate_identical_records"]
                                           + golden["duplicate_shifted_records"])
    assert golden["business_unique_events"] == golden["unique_event_ids"] - 1


def test_golden_answer_records_push_event_and_excludes_it(golden, lines):
    push = golden["watermark_push_event"]
    assert push["batch_no"] == 12
    assert push["event_type"] == "view"
    assert any(line["event_id"] == push["event_id"] for line in lines)
    assert sum(golden["event_type_counts"].values()) == golden["business_unique_events"]


def test_golden_answer_late_counts_match_spec(golden):
    business = golden["business_unique_events"]
    assert 0.13 <= golden["late_within_count"] / business <= 0.17
    assert 0.02 <= golden["late_beyond_count_wm10"] / business <= 0.03
    assert golden["late_beyond_purchases_wm10"] >= 15
    assert golden["silver_rows_dropped_by_watermark"] == 0


def test_golden_answer_effective_event_time_is_first_sent(lines):
    frame = gen.load_records_frame(lines)
    uniques = gen.first_copies(frame)
    first_seen = {}
    for line in lines:
        first_seen.setdefault(line["event_id"], pd.Timestamp(line["event_time"]))
    for row in uniques.itertuples():
        assert row.event_time == first_seen[row.event_id]


def test_golden_answer_revenue_is_sum_of_unique_purchases(golden, lines):
    seen = set()
    revenue = 0
    for line in lines:
        if line["event_id"] in seen:
            continue
        seen.add(line["event_id"])
        if line["event_type"] == "purchase":
            revenue += line["amount"]
    assert golden["revenue_absolute"] == revenue


# ---------------------------------------------------------------- task 16
@pytest.fixture(scope="module")
def golden_windows(output_dir):
    return pd.read_csv(output_dir / "golden_windows.csv")


def test_golden_windows_cover_twelve_five_minute_windows(golden_windows):
    starts = pd.to_datetime(golden_windows["window_start"], utc=True)
    ends = pd.to_datetime(golden_windows["window_end"], utc=True)
    expected_starts = [START + pd.Timedelta(minutes=5 * i) for i in range(12)]
    assert list(starts) == expected_starts
    assert ((ends - starts) == pd.Timedelta(minutes=5)).all()


def test_golden_windows_absolute_totals_match_golden(golden_windows, golden):
    assert golden_windows["revenue_absolute"].sum() == golden["revenue_absolute"]
    assert golden_windows["orders_absolute"].sum() == golden["purchases_unique"]


def test_golden_windows_wm10_equals_absolute_minus_dropped(golden_windows, golden):
    orders_lost = golden_windows["orders_absolute"] - golden_windows["orders_wm10"]
    assert (orders_lost == golden_windows["dropped_purchases_wm10"]).all()
    assert golden_windows["dropped_purchases_wm10"].sum() == golden["late_beyond_purchases_wm10"]
    assert golden_windows["revenue_wm10"].sum() == golden["gold_wm10"]["revenue_total"]


def test_golden_windows_wm20_closer_to_absolute(golden_windows):
    gap_wm10 = (golden_windows["revenue_absolute"] - golden_windows["revenue_wm10"]).sum()
    gap_wm20 = (golden_windows["revenue_absolute"] - golden_windows["revenue_wm20"]).sum()
    assert gap_wm10 > 0
    assert gap_wm20 < gap_wm10


def test_golden_windows_all_emitted_with_close_batch(golden_windows, golden):
    for minutes in (10, 20):
        assert golden_windows[f"emitted_wm{minutes}"].all()
        close = golden_windows[f"close_batch_wm{minutes}"]
        assert close.between(1, 12).all()
        # Cửa sổ sau chốt muộn hơn hoặc cùng đợt với cửa sổ trước
        assert close.is_monotonic_increasing
    assert golden["gold_wm10"]["emitted_window_count"] == len(golden_windows)


# ---------------------------------------------------------------- task 17
def test_generated_data_passes_all_checks(golden):
    assert golden["data_checks"] == []
    assert golden["late_beyond_designed_all_in_closed_windows"] is True


def test_generated_late_beyond_arrive_after_their_window_closed(lines, golden_windows):
    close_batch = dict(zip(golden_windows["window_start"], golden_windows["close_batch_wm10"]))
    seen = set()
    beyond_count = 0
    for line in lines:
        if line["event_id"] in seen:
            continue
        seen.add(line["event_id"])
        delay = pd.Timestamp(line["send_time"]) - pd.Timestamp(line["event_time"])
        if pd.Timedelta(minutes=12) <= delay <= pd.Timedelta(minutes=20):
            beyond_count += 1
            window_start = pd.Timestamp(line["event_time"]).floor("5min")
            key = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")
            # Cửa sổ đã được chốt ở một đợt TRƯỚC đợt sự kiện đến
            assert close_batch[key] < line["batch_no"]
    assert beyond_count > 0


def test_generated_twice_with_seed_42_is_identical(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    gen.write_outputs(first, 42)
    gen.write_outputs(second, 42)
    for name in ("events.jsonl", "golden_answer.json", "golden_windows.csv"):
        assert (first / name).read_bytes() == (second / name).read_bytes()


def test_generated_summary_prints_main_statistics(output_dir, capsys):
    with open(output_dir / "golden_answer.json", encoding="utf-8") as golden_file:
        golden_data = json.load(golden_file)
    windows = pd.read_csv(output_dir / "golden_windows.csv")
    gen.print_summary(golden_data, windows)
    printed = capsys.readouterr().out
    for key in ("total_records_sent", "unique_event_ids", "duplicate_shifted_records",
                "late_within_count", "late_beyond_purchases_wm10", "revenue_absolute",
                "gold_wm10", "gold_wm20", "Theo đợt", "Theo cửa sổ"):
        assert key in printed
