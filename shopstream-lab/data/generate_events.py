"""Sinh dữ liệu giả lập ShopStream có "cài bẫy" và tính đáp án vàng.

Đầu ra (trong data/):
    events.jsonl         sự kiện theo đúng thứ tự gửi, kèm batch_no và send_time
    golden_answer.json   thống kê tổng của đáp án vàng
    golden_windows.csv   doanh thu và số đơn đúng của từng cửa sổ 5 phút

Cách chạy:
    python data/generate_events.py
    python data/generate_events.py --summary
"""

import argparse
import json
import sys
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from common import config  # noqa: E402

START_TIME = pd.Timestamp(config.DATA_START_TIME)
DURATION_MS = config.DATA_DURATION_MINUTES * 60_000
BATCH_MS = config.BATCH_MINUTES * 60_000
WINDOW_MS = config.WINDOW_MINUTES * 60_000
# Thời điểm gửi cuối cùng hợp lệ (đợt 12 kết thúc trước 11:00:00)
LAST_SEND_MS = config.BATCH_COUNT * BATCH_MS - 1

# Độ trễ của sự kiện đúng hạn (độ trễ mạng bình thường)
ON_TIME_DELAY_MS = (0, 10_000)
# Bản trùng được gửi lại sau bản gốc một khoảng (giống producer retry)
DUPLICATE_RETRY_DELAY_MS = (5_000, 90_000)

# Nhãn nhóm độ trễ
ON_TIME = "on_time"
LATE_WITHIN = "late_within"
LATE_BEYOND = "late_beyond"


# ---------------------------------------------------------------------------
# Hàm tiện ích thời gian
# ---------------------------------------------------------------------------
def ms_to_timestamp(offset_ms):
    """Đổi số mili giây tính từ 10:00:00 sang pd.Timestamp UTC."""
    return START_TIME + pd.Timedelta(milliseconds=int(offset_ms))


def format_event_time(offset_ms):
    """event_time dạng ISO 8601 UTC, độ chính xác giây, ví dụ 2026-01-15T10:03:27Z."""
    return ms_to_timestamp(offset_ms).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_send_time(offset_ms):
    """send_time dạng ISO 8601 UTC, độ chính xác mili giây."""
    ts = ms_to_timestamp(offset_ms)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def seeded_uuid(rng):
    """UUID tất định sinh từ rng có seed (không dùng uuid4 ngẫu nhiên)."""
    return str(uuid.UUID(bytes=rng.bytes(16), version=4))


# ---------------------------------------------------------------------------
# Bước 1: sự kiện gốc
# ---------------------------------------------------------------------------
def generate_base_events(rng, event_count=config.BASE_EVENT_COUNT):
    """Sinh sự kiện duy nhất, event time trải đều trong 60 phút, sắp theo event time."""
    # Event time độ chính xác giây, trong [10:00:00, 11:00:00)
    event_seconds = np.sort(rng.integers(0, DURATION_MS // 1000, size=event_count))
    event_types = list(config.EVENT_TYPE_WEIGHTS)
    type_weights = list(config.EVENT_TYPE_WEIGHTS.values())
    chosen_types = rng.choice(event_types, size=event_count, p=type_weights)
    user_numbers = rng.integers(1, config.USER_COUNT + 1, size=event_count)
    product_numbers = rng.integers(1, config.PRODUCT_COUNT + 1, size=event_count)

    events = []
    for index in range(event_count):
        event_type = str(chosen_types[index])
        amount = None
        if event_type == "purchase":
            # Bội của 1.000 đồng, từ 50.000 đến 500.000
            amount = int(
                rng.integers(
                    config.AMOUNT_MIN // config.AMOUNT_STEP,
                    config.AMOUNT_MAX // config.AMOUNT_STEP + 1,
                )
                * config.AMOUNT_STEP
            )
        events.append(
            {
                "event_id": seeded_uuid(rng),
                "user_id": f"U{user_numbers[index]:03d}",
                "event_type": event_type,
                "product_id": f"P{product_numbers[index]:02d}",
                "amount": amount,
                "event_time_ms": int(event_seconds[index]) * 1000,
            }
        )
    return events


# ---------------------------------------------------------------------------
# Bước 2: độ trễ (đúng hạn, muộn trong ngưỡng, muộn quá ngưỡng)
# ---------------------------------------------------------------------------
def pick_late_beyond_indexes(rng, events):
    """Chọn nhóm muộn quá ngưỡng, có đủ purchase.

    Watermark chỉ cập nhật giữa các đợt và Spark loại dòng theo window_end,
    nên sự kiện ở cửa sổ thứ k chỉ chắc chắn bị loại khi được gửi từ đợt k+4
    trở đi. Vì vậy chỉ chọn sự kiện ở 8 cửa sổ đầu (để kịp gửi trước 11:00)
    và không nằm sát đầu cửa sổ (để có độ trễ hợp lệ trong 12 đến 20 phút).
    """
    target_count = round(len(events) * config.LATE_BEYOND_RATE)
    purchase_target = config.LATE_BEYOND_MIN_PURCHASES + 3
    max_beyond_ms = config.LATE_BEYOND_MINUTES[1] * 60_000
    candidates_purchase = []
    candidates_other = []
    for index, event in enumerate(events):
        event_ms = event["event_time_ms"]
        position_in_window_ms = event_ms % WINDOW_MS
        # Cửa sổ thứ k phải được gửi ở đợt >= k+4, nên k <= 12 - 4 - 1 = 7
        if event_ms >= (config.BATCH_COUNT - 4) * WINDOW_MS:
            continue
        if position_in_window_ms < 30_000:
            continue
        if event["event_type"] == "purchase":
            candidates_purchase.append(index)
        else:
            candidates_other.append(index)
    chosen_purchase = rng.choice(candidates_purchase, size=purchase_target, replace=False)
    chosen_other = rng.choice(
        candidates_other, size=target_count - purchase_target, replace=False
    )
    assert max_beyond_ms == 4 * WINDOW_MS, "công thức chọn giả định trễ tối đa 20 phút"
    return sorted(int(i) for i in np.concatenate([chosen_purchase, chosen_other]))


def assign_delays(rng, events):
    """Gán nhóm độ trễ và send_time_ms = event_time_ms + delay_ms cho từng sự kiện gốc."""
    beyond_indexes = set(pick_late_beyond_indexes(rng, events))
    min_beyond_ms = config.LATE_BEYOND_MINUTES[0] * 60_000
    max_beyond_ms = config.LATE_BEYOND_MINUTES[1] * 60_000
    for index in sorted(beyond_indexes):
        event = events[index]
        position_in_window_ms = event["event_time_ms"] % WINDOW_MS
        # Độ trễ tối thiểu để thời điểm gửi rơi vào đợt k+4 (cộng 5 giây an toàn)
        delay_floor_ms = max(min_beyond_ms, max_beyond_ms - position_in_window_ms + 5_000)
        event["delay_ms"] = int(rng.integers(delay_floor_ms, max_beyond_ms + 1))
        event["delay_group"] = LATE_BEYOND

    # Nhóm muộn trong ngưỡng: 1 đến 4 phút, phải gửi kịp trước 11:00
    within_target = round(len(events) * config.LATE_WITHIN_RATE)
    min_within_ms = config.LATE_WITHIN_MINUTES[0] * 60_000
    max_within_ms = config.LATE_WITHIN_MINUTES[1] * 60_000
    remaining_indexes = [i for i in range(len(events)) if i not in beyond_indexes]
    shuffled = rng.permutation(remaining_indexes)
    within_count = 0
    for index in shuffled:
        if within_count == within_target:
            break
        event = events[int(index)]
        delay_ms = int(rng.integers(min_within_ms, max_within_ms + 1))
        if event["event_time_ms"] + delay_ms > LAST_SEND_MS:
            continue
        event["delay_ms"] = delay_ms
        event["delay_group"] = LATE_WITHIN
        within_count += 1

    # Còn lại: đúng hạn, trễ 0 đến 10 giây, không vượt quá 11:00
    for event in events:
        if "delay_group" in event:
            continue
        delay_ms = int(rng.integers(ON_TIME_DELAY_MS[0], ON_TIME_DELAY_MS[1] + 1))
        event["delay_ms"] = min(delay_ms, LAST_SEND_MS - event["event_time_ms"])
        event["delay_group"] = ON_TIME

    for event in events:
        event["send_time_ms"] = event["event_time_ms"] + event["delay_ms"]
    return events


# ---------------------------------------------------------------------------
# Bước 3: bản trùng
# ---------------------------------------------------------------------------
def make_duplicates(rng, events):
    """Khoảng 10% sự kiện được gửi thêm một bản; khoảng 1/5 bản trùng lệch event_time."""
    duplicate_count = round(len(events) * config.DUPLICATE_RATE)
    shifted_count = round(duplicate_count * config.DUPLICATE_SHIFTED_SHARE)
    duplicate_indexes = sorted(
        int(i) for i in rng.choice(len(events), size=duplicate_count, replace=False)
    )
    shifted_indexes = set(
        int(i) for i in rng.choice(duplicate_indexes, size=shifted_count, replace=False)
    )
    duplicates = []
    for index in duplicate_indexes:
        original = events[index]
        duplicate = dict(original)
        retry_ms = int(rng.integers(DUPLICATE_RETRY_DELAY_MS[0], DUPLICATE_RETRY_DELAY_MS[1] + 1))
        duplicate["send_time_ms"] = min(original["send_time_ms"] + retry_ms, LAST_SEND_MS)
        duplicate["is_duplicate"] = True
        duplicate["is_shifted"] = False
        if index in shifted_indexes:
            shift_seconds = int(
                rng.integers(config.DUPLICATE_SHIFT_SECONDS[0], config.DUPLICATE_SHIFT_SECONDS[1] + 1)
            )
            # event_id giữ nguyên, chỉ event_time lệch vài giây
            duplicate["event_time_ms"] = original["event_time_ms"] + shift_seconds * 1000
            duplicate["is_shifted"] = True
        duplicates.append(duplicate)
    for event in events:
        event["is_duplicate"] = False
        event["is_shifted"] = False
    return duplicates


# ---------------------------------------------------------------------------
# Bước 4: chia đợt và sự kiện đẩy watermark
# ---------------------------------------------------------------------------
def make_watermark_push_event(rng):
    """Một view bình thường có event time 11:30, gửi ở cuối đợt 12, để chốt các cửa sổ cuối."""
    push_ms = int((pd.Timestamp(config.WATERMARK_PUSH_EVENT_TIME) - START_TIME).total_seconds() * 1000)
    return {
        "event_id": seeded_uuid(rng),
        "user_id": f"U{int(rng.integers(1, config.USER_COUNT + 1)):03d}",
        "event_type": "view",
        "product_id": f"P{int(rng.integers(1, config.PRODUCT_COUNT + 1)):02d}",
        "amount": None,
        "event_time_ms": push_ms,
        "send_time_ms": LAST_SEND_MS,
        "delay_ms": LAST_SEND_MS - push_ms,
        "delay_group": "watermark_push",
        "is_duplicate": False,
        "is_shifted": False,
    }


def build_send_order(events, duplicates, push_event):
    """Sắp mọi bản ghi theo send_time, gán batch_no 1..12 (mỗi đợt 5 phút thời gian gửi)."""
    records = list(events) + list(duplicates) + [push_event]
    # Sắp ổn định: cùng send_time thì bản gốc đứng trước bản trùng
    records.sort(key=lambda record: (record["send_time_ms"], record["is_duplicate"]))
    for record in records:
        record["batch_no"] = record["send_time_ms"] // BATCH_MS + 1
    return records


def generate_records(seed=config.SEED):
    """Toàn bộ quy trình sinh dữ liệu, trả về danh sách bản ghi theo thứ tự gửi."""
    rng = np.random.default_rng(seed)
    events = generate_base_events(rng)
    events = assign_delays(rng, events)
    duplicates = make_duplicates(rng, events)
    push_event = make_watermark_push_event(rng)
    return build_send_order(events, duplicates, push_event)


def to_output_line(record):
    """Dòng JSON ghi vào events.jsonl: sáu trường sự kiện cùng batch_no và send_time."""
    return {
        "event_id": record["event_id"],
        "user_id": record["user_id"],
        "event_type": record["event_type"],
        "product_id": record["product_id"],
        "amount": record["amount"],
        "event_time": format_event_time(record["event_time_ms"]),
        "batch_no": int(record["batch_no"]),
        "send_time": format_send_time(record["send_time_ms"]),
    }


# ---------------------------------------------------------------------------
# Mô phỏng watermark của Spark theo từng đợt
# ---------------------------------------------------------------------------
def simulate_watermark(unique_events, watermark_minutes, window_minutes=config.WINDOW_MINUTES,
                       batch_count=config.BATCH_COUNT):
    """Mô phỏng watermark và việc loại dòng quá muộn của aggregation theo cửa sổ.

    unique_events: DataFrame mỗi event_id một dòng (bản gửi trước), có cột
        event_time (Timestamp), event_type, amount, batch_no.

    Quy tắc (giống Spark):
    - Watermark dùng ở đợt b = max event time đã thấy ở các đợt TRƯỚC b, trừ ngưỡng.
      Max tính trên MỌI loại sự kiện vì withWatermark đặt trước khi lọc purchase.
    - Một dòng bị loại nếu window_end của nó <= watermark lúc đợt bắt đầu.
    - Cửa sổ được xuất (append) ở đợt đầu tiên mà watermark sau đợt >= window_end.

    Trả về (events_out, batch_watermarks):
        events_out: bản sao unique_events thêm window_start, window_end,
            watermark_at_arrival, dropped_by_watermark.
        batch_watermarks: DataFrame batch_no, watermark_before, watermark_after.
    """
    threshold = pd.Timedelta(minutes=watermark_minutes)
    window = pd.Timedelta(minutes=window_minutes)
    events_out = unique_events.copy()
    events_out["window_start"] = events_out["event_time"].dt.floor(window)
    events_out["window_end"] = events_out["window_start"] + window
    events_out["watermark_at_arrival"] = pd.Series(
        pd.NaT, index=events_out.index, dtype=events_out["event_time"].dtype
    )
    events_out["dropped_by_watermark"] = False

    max_event_time_seen = None
    watermark = None
    batch_rows = []
    for batch_no in range(1, batch_count + 1):
        watermark_before = watermark
        in_batch = events_out["batch_no"] == batch_no
        if watermark_before is not None:
            events_out.loc[in_batch, "watermark_at_arrival"] = watermark_before
            late_mask = in_batch & (events_out["window_end"] <= watermark_before)
            events_out.loc[late_mask, "dropped_by_watermark"] = True
        if in_batch.any():
            batch_max = events_out.loc[in_batch, "event_time"].max()
            if max_event_time_seen is None or batch_max > max_event_time_seen:
                max_event_time_seen = batch_max
        if max_event_time_seen is not None:
            candidate = max_event_time_seen - threshold
            # Watermark không bao giờ lùi lại
            if watermark is None or candidate > watermark:
                watermark = candidate
        batch_rows.append(
            {"batch_no": batch_no, "watermark_before": watermark_before, "watermark_after": watermark}
        )
    return events_out, pd.DataFrame(batch_rows)


def window_close_batch(window_end, batch_watermarks):
    """Đợt đầu tiên mà watermark sau đợt đó vượt qua (>=) window_end, None nếu chưa."""
    for row in batch_watermarks.itertuples():
        if row.watermark_after is not None and pd.notna(row.watermark_after) and row.watermark_after >= window_end:
            return int(row.batch_no)
    return None


def aggregate_gold(simulated_events, batch_watermarks):
    """Doanh thu, số đơn mỗi cửa sổ từ purchase không bị loại; kèm cửa sổ có được xuất không."""
    purchases = simulated_events[simulated_events["event_type"] == "purchase"]
    kept = purchases[~purchases["dropped_by_watermark"]]
    grouped = kept.groupby("window_start").agg(revenue=("amount", "sum"), orders=("amount", "size"))
    dropped = purchases[purchases["dropped_by_watermark"]].groupby("window_start").size()
    grouped["dropped_purchases"] = dropped.reindex(grouped.index).fillna(0).astype(int)
    grouped["close_batch"] = [
        window_close_batch(start + pd.Timedelta(minutes=config.WINDOW_MINUTES), batch_watermarks)
        for start in grouped.index
    ]
    grouped["emitted"] = grouped["close_batch"].notna()
    return grouped


# ---------------------------------------------------------------------------
# Đáp án vàng (chỉ đọc từ các dòng events.jsonl, tính bằng pandas)
# ---------------------------------------------------------------------------
def load_records_frame(lines):
    frame = pd.DataFrame(lines)
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["send_time"] = pd.to_datetime(frame["send_time"], utc=True)
    frame["send_order"] = range(len(frame))
    return frame


def first_copies(records_frame):
    """Mỗi event_id giữ bản gửi trước; event_time có hiệu lực là của bản này."""
    ordered = records_frame.sort_values("send_order")
    return ordered.drop_duplicates("event_id", keep="first").reset_index(drop=True)


def classify_delay(delay):
    within_low = pd.Timedelta(minutes=config.LATE_WITHIN_MINUTES[0])
    within_high = pd.Timedelta(minutes=config.LATE_WITHIN_MINUTES[1])
    beyond_low = pd.Timedelta(minutes=config.LATE_BEYOND_MINUTES[0])
    beyond_high = pd.Timedelta(minutes=config.LATE_BEYOND_MINUTES[1])
    if delay < pd.Timedelta(0):
        return "watermark_push"
    if within_low <= delay <= within_high:
        return LATE_WITHIN
    if beyond_low <= delay <= beyond_high:
        return LATE_BEYOND
    if delay < within_low:
        return ON_TIME
    return "other"


def compute_golden(lines):
    """Tính đáp án vàng. Trả về (golden_answer dict, golden_windows DataFrame)."""
    records = load_records_frame(lines)
    push_mask = records["event_time"] >= pd.Timestamp(config.WATERMARK_PUSH_EVENT_TIME)
    push_rows = records[push_mask]
    assert len(push_rows) == 1, "phải có đúng một sự kiện đẩy watermark"
    push_row = push_rows.iloc[0]

    uniques = first_copies(records)
    uniques["delay"] = uniques["send_time"] - uniques["event_time"]
    uniques["delay_group"] = uniques["delay"].apply(classify_delay)

    duplicate_rows = records[records.duplicated("event_id", keep="first")]
    original_time = uniques.set_index("event_id")["event_time"]
    shifted_mask = duplicate_rows["event_time"].values != original_time.loc[duplicate_rows["event_id"]].values
    shifted_count = int(shifted_mask.sum())

    # Mô phỏng watermark cho Gold (10 phút, 20 phút) và Silver (30 phút)
    simulated = {}
    batch_watermarks = {}
    for minutes in (config.GOLD_WATERMARK_MINUTES, config.GOLD_COMPARE_WATERMARK_MINUTES,
                    config.SILVER_WATERMARK_MINUTES):
        simulated[minutes], batch_watermarks[minutes] = simulate_watermark(uniques, minutes)

    # Silver: dedup theo event time, dòng gốc bị loại nếu event_time <= watermark Silver
    silver_sim = simulated[config.SILVER_WATERMARK_MINUTES]
    silver_late = silver_sim["watermark_at_arrival"].notna() & (
        silver_sim["event_time"] <= silver_sim["watermark_at_arrival"]
    )

    gold_sim = simulated[config.GOLD_WATERMARK_MINUTES]
    compare_sim = simulated[config.GOLD_COMPARE_WATERMARK_MINUTES]
    business = uniques["event_id"] != push_row["event_id"]
    purchases = uniques[business & (uniques["event_type"] == "purchase")]

    beyond_designed = uniques[uniques["delay_group"] == LATE_BEYOND]
    gold_dropped = gold_sim[gold_sim["dropped_by_watermark"]]
    compare_dropped = compare_sim[compare_sim["dropped_by_watermark"]]

    # Bảng cửa sổ
    window = pd.Timedelta(minutes=config.WINDOW_MINUTES)
    absolute = (
        purchases.assign(window_start=purchases["event_time"].dt.floor(window))
        .groupby("window_start")
        .agg(revenue_absolute=("amount", "sum"), orders_absolute=("amount", "size"))
    )
    gold_windows = aggregate_gold(gold_sim, batch_watermarks[config.GOLD_WATERMARK_MINUTES])
    compare_windows = aggregate_gold(compare_sim, batch_watermarks[config.GOLD_COMPARE_WATERMARK_MINUTES])
    windows = absolute.join(
        gold_windows.add_suffix("_wm10"), how="outer"
    ).join(compare_windows.add_suffix("_wm20"), how="outer")
    windows = windows.reset_index()
    windows["window_end"] = windows["window_start"] + window
    for column in ("revenue_absolute", "orders_absolute", "revenue_wm10", "orders_wm10",
                   "dropped_purchases_wm10", "revenue_wm20", "orders_wm20", "dropped_purchases_wm20"):
        windows[column] = windows[column].fillna(0).astype(int)
    # Purchase bị loại ở cửa sổ không còn đơn nào vẫn phải tính
    for minutes, sim in ((10, gold_sim), (20, compare_sim)):
        dropped_counts = (
            sim[(sim["event_type"] == "purchase") & sim["dropped_by_watermark"]]
            .groupby("window_start").size()
        )
        windows[f"dropped_purchases_wm{minutes}"] = (
            windows["window_start"].map(dropped_counts).fillna(0).astype(int)
        )
    for minutes in (10, 20):
        windows[f"emitted_wm{minutes}"] = windows[f"emitted_wm{minutes}"].fillna(False).astype(bool)
        windows[f"close_batch_wm{minutes}"] = windows[f"close_batch_wm{minutes}"].astype("Int64")
    windows = windows[[
        "window_start", "window_end",
        "revenue_absolute", "orders_absolute",
        "revenue_wm10", "orders_wm10", "dropped_purchases_wm10", "emitted_wm10", "close_batch_wm10",
        "revenue_wm20", "orders_wm20", "dropped_purchases_wm20", "emitted_wm20", "close_batch_wm20",
    ]].sort_values("window_start").reset_index(drop=True)

    def iso(ts):
        return None if ts is None or pd.isna(ts) else ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    per_batch = []
    for batch_no in range(1, config.BATCH_COUNT + 1):
        batch_records = records[records["batch_no"] == batch_no]
        batch_uniques = uniques[uniques["batch_no"] == batch_no]
        gold_batch = batch_watermarks[config.GOLD_WATERMARK_MINUTES].iloc[batch_no - 1]
        compare_batch = batch_watermarks[config.GOLD_COMPARE_WATERMARK_MINUTES].iloc[batch_no - 1]
        silver_batch = batch_watermarks[config.SILVER_WATERMARK_MINUTES].iloc[batch_no - 1]
        per_batch.append({
            "batch_no": batch_no,
            "records_sent": int(len(batch_records)),
            "new_unique_events": int(len(batch_uniques)),
            "new_purchases": int((batch_uniques["event_type"] == "purchase").sum()),
            "purchases_dropped_wm10": int(
                ((gold_sim["batch_no"] == batch_no) & gold_sim["dropped_by_watermark"]
                 & (gold_sim["event_type"] == "purchase")).sum()),
            "purchases_dropped_wm20": int(
                ((compare_sim["batch_no"] == batch_no) & compare_sim["dropped_by_watermark"]
                 & (compare_sim["event_type"] == "purchase")).sum()),
            "gold_watermark_before_wm10": iso(gold_batch["watermark_before"]),
            "gold_watermark_after_wm10": iso(gold_batch["watermark_after"]),
            "gold_watermark_after_wm20": iso(compare_batch["watermark_after"]),
            "silver_watermark_after": iso(silver_batch["watermark_after"]),
        })

    def gold_summary(windows_frame, minutes):
        emitted = windows_frame[windows_frame[f"emitted_wm{minutes}"]]
        return {
            "watermark_minutes": minutes,
            "revenue_total": int(emitted[f"revenue_wm{minutes}"].sum()),
            "orders_total": int(emitted[f"orders_wm{minutes}"].sum()),
            "emitted_window_count": int(len(emitted)),
            "emitted_windows": [iso(ts) for ts in emitted["window_start"]],
        }

    event_type_counts = uniques[business]["event_type"].value_counts().to_dict()
    golden = {
        "seed": config.SEED,
        "data_start_time": config.DATA_START_TIME,
        "total_records_sent": int(len(records)),
        "unique_event_ids": int(records["event_id"].nunique()),
        "business_unique_events": int(business.sum()),
        "duplicate_records": int(len(duplicate_rows)),
        "duplicate_shifted_records": shifted_count,
        "duplicate_identical_records": int(len(duplicate_rows) - shifted_count),
        "duplicate_rate": round(len(duplicate_rows) / int(business.sum()), 4),
        # Nếu dùng dropDuplicates(event_id, event_time) thay vì theo event_id, bản trùng lệch giờ lọt qua
        "silver_rows_if_dedup_by_event_id_and_time": int(records["event_id"].nunique() + shifted_count),
        "event_type_counts": {key: int(value) for key, value in sorted(event_type_counts.items())},
        "users": int(uniques[business]["user_id"].nunique()),
        "products": int(uniques[business]["product_id"].nunique()),
        "late_within_count": int((uniques["delay_group"] == LATE_WITHIN).sum()),
        "late_beyond_designed_count": int(len(beyond_designed)),
        "late_beyond_designed_purchases": int((beyond_designed["event_type"] == "purchase").sum()),
        "late_beyond_count_wm10": int(len(gold_dropped)),
        "late_beyond_purchases_wm10": int((gold_dropped["event_type"] == "purchase").sum()),
        "late_beyond_count_wm20": int(len(compare_dropped)),
        "late_beyond_purchases_wm20": int((compare_dropped["event_type"] == "purchase").sum()),
        "late_beyond_designed_all_in_closed_windows": bool(
            set(beyond_designed["event_id"]) == set(gold_dropped["event_id"])
        ),
        "silver_rows_dropped_by_watermark": int(silver_late.sum()),
        "purchases_unique": int(len(purchases)),
        "revenue_absolute": int(purchases["amount"].sum()),
        "gold_wm10": gold_summary(windows, 10),
        "gold_wm20": gold_summary(windows, 20),
        "watermark_push_event": {
            "event_id": push_row["event_id"],
            "event_type": push_row["event_type"],
            "event_time": iso(push_row["event_time"]),
            "batch_no": int(push_row["batch_no"]),
            "note": "Chỉ dùng để đẩy watermark, không tính vào doanh thu và thống kê nghiệp vụ",
        },
        "per_batch": per_batch,
    }
    return golden, windows


def check_generated_data(golden, windows):
    """Kiểm tra bằng code các điều kiện bắt buộc của dữ liệu. Trả về danh sách lỗi."""
    problems = []
    if not golden["late_beyond_designed_all_in_closed_windows"]:
        problems.append("nhóm muộn quá ngưỡng không trùng khớp với nhóm bị watermark 10 phút loại")
    if golden["late_beyond_purchases_wm10"] < config.LATE_BEYOND_MIN_PURCHASES:
        problems.append("nhóm muộn quá ngưỡng có ít hơn 15 purchase")
    if golden["silver_rows_dropped_by_watermark"] != 0:
        problems.append("có sự kiện gốc bị watermark 30 phút của Silver loại")
    if not windows["emitted_wm10"].all():
        problems.append("có cửa sổ chưa được chốt sau sự kiện đẩy watermark (10 phút)")
    if not windows["emitted_wm20"].all():
        problems.append("có cửa sổ chưa được chốt sau sự kiện đẩy watermark (20 phút)")
    if golden["gold_wm20"]["revenue_total"] < golden["gold_wm10"]["revenue_total"]:
        problems.append("doanh thu 20 phút không gần đáp án tuyệt đối hơn 10 phút")
    return problems


# ---------------------------------------------------------------------------
# Ghi file và tóm tắt
# ---------------------------------------------------------------------------
def write_outputs(output_dir, seed=config.SEED):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    records = generate_records(seed)
    lines = [to_output_line(record) for record in records]
    with open(output_dir / config.EVENTS_FILE.name, "w", encoding="utf-8") as events_file:
        for line in lines:
            events_file.write(json.dumps(line, ensure_ascii=False) + "\n")
    golden, windows = compute_golden(lines)
    golden["data_checks"] = check_generated_data(golden, windows)
    with open(output_dir / config.GOLDEN_ANSWER_FILE.name, "w", encoding="utf-8") as golden_file:
        json.dump(golden, golden_file, ensure_ascii=False, indent=2)
        golden_file.write("\n")
    csv_frame = windows.copy()
    for column in ("window_start", "window_end"):
        csv_frame[column] = csv_frame[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    csv_frame.to_csv(output_dir / config.GOLDEN_WINDOWS_FILE.name, index=False)
    return golden, windows


def print_summary(golden, windows):
    print("=== Thống kê bộ dữ liệu ===")
    keys = [
        "total_records_sent", "unique_event_ids", "business_unique_events",
        "duplicate_records", "duplicate_identical_records", "duplicate_shifted_records",
        "silver_rows_if_dedup_by_event_id_and_time", "users", "products",
        "late_within_count", "late_beyond_designed_count", "late_beyond_designed_purchases",
        "late_beyond_count_wm10", "late_beyond_purchases_wm10",
        "late_beyond_count_wm20", "late_beyond_purchases_wm20",
        "silver_rows_dropped_by_watermark", "purchases_unique", "revenue_absolute",
    ]
    for key in keys:
        print(f"  {key:45s} {golden[key]}")
    print(f"  {'event_type_counts':45s} {golden['event_type_counts']}")
    print(f"  {'watermark_push_event':45s} {golden['watermark_push_event']['event_time']} "
          f"(đợt {golden['watermark_push_event']['batch_no']})")
    for name in ("gold_wm10", "gold_wm20"):
        summary = golden[name]
        print(f"  {name}: doanh thu {summary['revenue_total']:,} đồng, {summary['orders_total']} đơn, "
              f"{summary['emitted_window_count']} cửa sổ được xuất")
    print("\n=== Theo đợt ===")
    print(pd.DataFrame(golden["per_batch"]).to_string(index=False))
    print("\n=== Theo cửa sổ ===")
    print(windows.to_string(index=False))
    print("\nKiểm tra dữ liệu:", "OK" if not golden["data_checks"] else golden["data_checks"])


def main():
    parser = argparse.ArgumentParser(description="Sinh dữ liệu ShopStream và đáp án vàng")
    parser.add_argument("--output-dir", default=str(config.DATA_DIR))
    parser.add_argument("--seed", type=int, default=config.SEED)
    parser.add_argument("--summary", action="store_true", help="in tóm tắt thống kê")
    args = parser.parse_args()
    golden, windows = write_outputs(args.output_dir, args.seed)
    print(f"Đã ghi {golden['total_records_sent']} bản ghi vào {Path(args.output_dir) / 'events.jsonl'}")
    if args.summary:
        print_summary(golden, windows)
    if golden["data_checks"]:
        print("LỖI kiểm tra dữ liệu:", golden["data_checks"])
        sys.exit(1)


if __name__ == "__main__":
    main()
