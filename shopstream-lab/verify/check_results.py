"""Đối chiếu Bronze, Silver, Gold với đáp án vàng. Thoát với mã khác 0 nếu có dòng sai.

Cách chạy:
    python verify/check_results.py                      # mọi tầng, sau đủ 12 đợt
    python verify/check_results.py --layer bronze --up-to-batch 1
    python verify/check_results.py --watermark-suffix _wm20   # thêm Gold 20 phút và so sánh hai ngưỡng
"""

import argparse
import json
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from common import config  # noqa: E402

LAYERS = ("bronze", "silver", "gold")
CHECK_RESULTS_FILE = config.RESULTS_DIR / "check_results.json"


def load_golden():
    with open(config.GOLDEN_ANSWER_FILE, encoding="utf-8") as golden_file:
        return json.load(golden_file)


def cumulative(golden, key, up_to_batch):
    """Tổng một chỉ số theo đợt, từ đợt 1 đến up_to_batch."""
    return sum(row[key] for row in golden["per_batch"] if row["batch_no"] <= up_to_batch)


class CheckTable:
    """Gom các dòng đối chiếu để in thành bảng và biết có dòng nào sai."""

    def __init__(self):
        self.rows = []

    def add(self, layer, item, expected, actual, ok=None):
        if ok is None:
            ok = expected == actual
        self.rows.append((layer, item, expected, actual, ok))
        return ok

    @property
    def all_ok(self):
        return all(row[4] for row in self.rows)

    def print(self):
        header = ("Tầng", "Mục kiểm tra", "Kỳ vọng", "Thực tế", "Kết quả")
        lines = [header] + [
            (layer, item, str(expected), str(actual), "OK" if ok else "SAI")
            for layer, item, expected, actual, ok in self.rows
        ]
        widths = [max(len(line[i]) for line in lines) for i in range(len(header))]
        for index, line in enumerate(lines):
            print(" | ".join(cell.ljust(widths[i]) for i, cell in enumerate(line)))
            if index == 0:
                print("-+-".join("-" * width for width in widths))


def check_bronze(spark, golden, up_to_batch, table):
    expected = cumulative(golden, "records_sent", up_to_batch)
    bronze = spark.read.format("delta").load(str(config.table_path(config.BRONZE_TABLE)))
    table.add("bronze", f"số dòng (= số message đến đợt {up_to_batch})", expected, bronze.count())


def check_silver(spark, golden, up_to_batch, table):
    expected = cumulative(golden, "new_unique_events", up_to_batch)
    silver = spark.read.format("delta").load(str(config.table_path(config.SILVER_TABLE)))
    row_count = silver.count()
    distinct_ids = silver.select("event_id").distinct().count()
    table.add("silver", f"số dòng (= số event_id duy nhất đến đợt {up_to_batch})", expected, row_count)
    table.add("silver", "số event_id bị trùng", 0, row_count - distinct_ids)


def load_golden_windows():
    import pandas as pd

    windows = pd.read_csv(config.GOLDEN_WINDOWS_FILE)
    return windows


def watermark_minutes_for_suffix(suffix):
    return config.GOLD_COMPARE_WATERMARK_MINUTES if suffix == config.GOLD_COMPARE_SUFFIX \
        else config.GOLD_WATERMARK_MINUTES


def check_gold(spark, golden_windows, up_to_batch, table, suffix=""):
    """So từng cửa sổ của bảng Gold với doanh thu, số đơn kỳ vọng; liệt kê thiếu, thừa, lệch."""
    minutes = watermark_minutes_for_suffix(suffix)
    layer = f"gold{suffix}"
    expected = golden_windows[golden_windows[f"close_batch_wm{minutes}"] <= up_to_batch]
    expected_by_start = {
        row.window_start: (int(getattr(row, f"revenue_wm{minutes}")), int(getattr(row, f"orders_wm{minutes}")))
        for row in expected.itertuples()
    }
    gold = spark.read.format("delta").load(str(config.table_path(config.GOLD_TABLE, suffix)))
    actual_rows = gold.selectExpr(
        "date_format(window_start, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS window_start", "revenue", "orders"
    ).collect()
    actual_by_start = {}
    for row in actual_rows:
        # Một cửa sổ xuất hai lần cũng là lỗi (append mode chỉ được ghi mỗi cửa sổ một lần)
        actual_by_start.setdefault(row.window_start, []).append((int(row.revenue), int(row.orders)))

    table.add(layer, f"số cửa sổ (ngưỡng {minutes} phút, đến đợt {up_to_batch})",
              len(expected_by_start), len(actual_rows))
    for window_start in sorted(set(expected_by_start) | set(actual_by_start)):
        expected_value = expected_by_start.get(window_start)
        actual_values = actual_by_start.get(window_start)
        label = f"cửa sổ {window_start[11:16]} (doanh thu, số đơn)"
        if expected_value is None:
            table.add(layer, label + " THỪA", "-", actual_values, ok=False)
        elif actual_values is None:
            table.add(layer, label + " THIẾU", expected_value, "-", ok=False)
        else:
            actual_value = actual_values[0] if len(actual_values) == 1 else actual_values
            table.add(layer, label, expected_value, actual_value)
    return expected_by_start, actual_by_start


def epoch_to_batch(query_name):
    """Ánh xạ batchId (epochId) của truy vấn sang đợt gửi, từ results/progress/batch_XX.json."""
    mapping = {}
    for progress_file in sorted(config.PROGRESS_DIR.glob("batch_*.json")):
        record = json.loads(progress_file.read_text(encoding="utf-8"))
        query_record = record["queries"].get(query_name)
        if query_record:
            for batch_id in query_record["micro_batch_ids"]:
                mapping[batch_id] = record["batch_no"]
    return mapping


def actual_close_batches(spark, suffix=""):
    """Đợt mà mỗi cửa sổ xuất hiện trong Gold: đọc từng phiên bản Delta (time travel) và so với bản trước."""
    gold_path = str(config.table_path(config.GOLD_TABLE, suffix))
    mapping = epoch_to_batch(f"gold{suffix}")
    history = spark.sql(f"DESCRIBE HISTORY delta.`{gold_path}`").orderBy("version").collect()
    seen = set()
    close = {}
    for entry in history:
        epoch_id = (entry.operationParameters or {}).get("epochId")
        rows = spark.read.format("delta").option("versionAsOf", entry.version).load(gold_path).selectExpr(
            "date_format(window_start, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS window_start").collect()
        current = {row.window_start for row in rows}
        for window_start in sorted(current - seen):
            close[window_start] = {
                "delta_version": entry.version,
                "epoch_id": int(epoch_id) if epoch_id is not None else None,
                "batch_no": mapping.get(int(epoch_id)) if epoch_id is not None else None,
            }
        seen = current
    return close


def check_gold_close_batches(spark, golden_windows, up_to_batch, table, suffix=""):
    """Đợt chốt (xuất) của từng cửa sổ trong Gold so với đáp án vàng."""
    minutes = watermark_minutes_for_suffix(suffix)
    layer = f"gold{suffix}"
    close = actual_close_batches(spark, suffix)
    expected = golden_windows[golden_windows[f"close_batch_wm{minutes}"] <= up_to_batch]
    for row in expected.itertuples():
        actual = close.get(row.window_start, {}).get("batch_no")
        table.add(layer, f"cửa sổ {row.window_start[11:16]} chốt ở đợt",
                  int(getattr(row, f"close_batch_wm{minutes}")), actual)
    return close


def dropped_rows_by_batch(query_name, up_to_batch):
    """Tổng numRowsDroppedByWatermark của mọi micro-batch trong mỗi đợt, đọc từ results/progress."""
    dropped = {}
    for batch_no in range(1, up_to_batch + 1):
        progress_file = config.PROGRESS_DIR / f"batch_{batch_no:02d}.json"
        if not progress_file.exists():
            return None
        record = json.loads(progress_file.read_text(encoding="utf-8"))
        if query_name not in record["queries"]:
            return None
        total = 0
        for progress in record["queries"][query_name]["progress"]:
            for operator in progress.get("stateOperators", []):
                total += operator.get("numRowsDroppedByWatermark", 0)
        dropped[batch_no] = total
    return dropped


def check_dropped(golden, golden_windows, gold_actual, up_to_batch, table, suffix=""):
    """Số dòng bị loại vì quá muộn: từ tiến trình truy vấn và từ bảng Gold, so với đáp án vàng."""
    minutes = watermark_minutes_for_suffix(suffix)
    layer = f"gold{suffix}"
    # 1) Số purchase bị mất ở Gold = số đơn tuyệt đối - số đơn trong bảng Gold (chỉ khi đã chốt đủ cửa sổ)
    if up_to_batch == config.BATCH_COUNT:
        orders_absolute = int(golden_windows["orders_absolute"].sum())
        orders_in_gold = sum(values[0][1] for values in gold_actual.values())
        table.add(layer, "số purchase bị loại vì quá muộn (tính từ bảng Gold)",
                  golden[f"late_beyond_purchases_wm{minutes}"], orders_absolute - orders_in_gold)
    # 2) numRowsDroppedByWatermark: đếm ở đầu vào toán tử có trạng thái (sau partial aggregation),
    #    nên mỗi cửa sổ quá muộn trong một đợt chỉ tính là một dòng
    dropped = dropped_rows_by_batch(layer, up_to_batch)
    if dropped is None:
        table.add(layer, "numRowsDroppedByWatermark (thiếu file results/progress)", "-", "-", ok=False)
        return
    for row in golden["per_batch"]:
        batch_no = row["batch_no"]
        if batch_no > up_to_batch:
            continue
        expected = row[f"late_windows_wm{minutes}"]
        if expected or dropped[batch_no]:
            table.add(layer, f"numRowsDroppedByWatermark đợt {batch_no} (= số cửa sổ quá muộn)",
                      expected, dropped[batch_no])
    table.add(layer, "numRowsDroppedByWatermark tổng",
              cumulative(golden, f"late_windows_wm{minutes}", up_to_batch), sum(dropped.values()))


def revenue_gap(golden, gold_actual):
    """Chênh lệch doanh thu của bảng Gold so với đáp án tuyệt đối."""
    revenue_in_gold = sum(values[0][0] for values in gold_actual.values())
    return golden["revenue_absolute"] - revenue_in_gold


def check_threshold_comparison(golden, gold_by_suffix, table, compare_suffix):
    gap_main = revenue_gap(golden, gold_by_suffix[""])
    gap_compare = revenue_gap(golden, gold_by_suffix[compare_suffix])
    minutes = watermark_minutes_for_suffix(compare_suffix)
    print(f"\nDoanh thu tuyệt đối: {golden['revenue_absolute']:,} đồng")
    print(f"Chênh lệch ngưỡng {config.GOLD_WATERMARK_MINUTES} phút: {gap_main:,} đồng")
    print(f"Chênh lệch ngưỡng {minutes} phút: {gap_compare:,} đồng")
    table.add("so sánh", f"chênh lệch doanh thu ngưỡng {config.GOLD_WATERMARK_MINUTES} phút (đồng)",
              golden["revenue_absolute"] - golden["gold_wm10"]["revenue_total"], gap_main)
    table.add("so sánh", f"chênh lệch doanh thu ngưỡng {minutes} phút (đồng)",
              golden["revenue_absolute"] - golden["gold_wm20"]["revenue_total"], gap_compare)
    table.add("so sánh", f"{minutes} phút gần đáp án tuyệt đối hơn", True, gap_compare < gap_main)
    return {"gap_wm10": gap_main, f"gap_wm{minutes}": gap_compare}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Đối chiếu kết quả với đáp án vàng")
    parser.add_argument("--layer", choices=LAYERS + ("all",), default="all")
    parser.add_argument("--up-to-batch", type=int, default=config.BATCH_COUNT)
    parser.add_argument("--watermark-suffix", default=None,
                        help="kiểm tra thêm bảng Gold có hậu tố này (ví dụ _wm20) và so sánh hai ngưỡng")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    from common.spark_session import get_spark

    golden = load_golden()
    spark = get_spark("check_results")
    table = CheckTable()
    saved = {"layer": args.layer, "up_to_batch": args.up_to_batch, "gold": {}}
    layers = LAYERS if args.layer == "all" else (args.layer,)
    if "bronze" in layers:
        check_bronze(spark, golden, args.up_to_batch, table)
    if "silver" in layers:
        check_silver(spark, golden, args.up_to_batch, table)
    if "gold" in layers:
        golden_windows = load_golden_windows()
        _, gold_actual = check_gold(spark, golden_windows, args.up_to_batch, table)
        close = check_gold_close_batches(spark, golden_windows, args.up_to_batch, table)
        saved["gold"][""] = {"windows": gold_actual, "close": close}
        check_dropped(golden, golden_windows, gold_actual, args.up_to_batch, table)
        if args.watermark_suffix:
            suffix = args.watermark_suffix
            _, compare_actual = check_gold(spark, golden_windows, args.up_to_batch, table, suffix)
            compare_close = check_gold_close_batches(spark, golden_windows, args.up_to_batch, table, suffix)
            check_dropped(golden, golden_windows, compare_actual, args.up_to_batch, table, suffix)
            saved["gold"][suffix] = {"windows": compare_actual, "close": compare_close}
            if args.up_to_batch == config.BATCH_COUNT:
                saved["comparison"] = check_threshold_comparison(
                    golden, {"": gold_actual, suffix: compare_actual}, table, suffix)
    spark.stop()

    print()
    table.print()
    print("\nKẾT QUẢ:", "KHỚP" if table.all_ok else "CÓ DÒNG SAI")
    saved["all_ok"] = table.all_ok
    saved["rows"] = [
        {"layer": layer, "item": item, "expected": expected, "actual": actual, "ok": ok}
        for layer, item, expected, actual, ok in table.rows
    ]
    # Chỉ lưu lần kiểm tra đầy đủ (mọi tầng, đủ 12 đợt, có so sánh ngưỡng) để báo cáo đọc lại
    if args.layer == "all" and args.up_to_batch == config.BATCH_COUNT and args.watermark_suffix:
        CHECK_RESULTS_FILE.write_text(json.dumps(saved, ensure_ascii=False, indent=2, default=str) + "\n",
                                      encoding="utf-8")
        print(f"Đã lưu {CHECK_RESULTS_FILE.relative_to(PROJECT_DIR)}")
    sys.exit(0 if table.all_ok else 1)


if __name__ == "__main__":
    main()
