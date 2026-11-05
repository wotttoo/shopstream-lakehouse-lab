"""Xem tiến trình truy vấn streaming và lịch sử bảng Delta.

Cách chạy:
    python verify/inspect_progress.py --progress   # tiến trình, lưu results/progress_summary.json
    python verify/inspect_progress.py --history    # DESCRIBE HISTORY và time travel, lưu results/delta_history.json
    python verify/inspect_progress.py              # cả hai
"""

import argparse
import json
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from common import config  # noqa: E402

PROGRESS_SUMMARY_FILE = config.RESULTS_DIR / "progress_summary.json"
DELTA_HISTORY_FILE = config.RESULTS_DIR / "delta_history.json"


def load_batch_records():
    records = []
    for progress_file in sorted(config.PROGRESS_DIR.glob("batch_*.json")):
        records.append(json.loads(progress_file.read_text(encoding="utf-8")))
    return records


def summarize_micro_batch(progress):
    """Các trường đáng chú ý của một micro-batch."""
    state = progress.get("stateOperators") or []
    return {
        "batch_id": progress["batchId"],
        "num_input_rows": progress.get("numInputRows"),
        "input_rows_per_second": round(progress.get("inputRowsPerSecond") or 0.0, 1),
        "processed_rows_per_second": round(progress.get("processedRowsPerSecond") or 0.0, 1),
        "watermark": (progress.get("eventTime") or {}).get("watermark"),
        "state_rows_total": sum(operator.get("numRowsTotal", 0) for operator in state) if state else None,
        "rows_dropped_by_watermark": (
            sum(operator.get("numRowsDroppedByWatermark", 0) for operator in state) if state else None
        ),
        "observed_metrics": progress.get("observedMetrics") or None,
    }


def show_progress():
    records = load_batch_records()
    if not records:
        print("Chưa có file results/progress/batch_*.json, hãy chạy streaming/run_all_batches.py trước")
        return False
    summary = {"per_batch": [], "latest": {}}
    for record in records:
        batch_summary = {"batch_no": record["batch_no"], "row_counts": record["row_counts"], "queries": {}}
        for query_name, query_record in record["queries"].items():
            batch_summary["queries"][query_name] = [
                summarize_micro_batch(progress) for progress in query_record["progress"]
            ]
        summary["per_batch"].append(batch_summary)

    last_record = records[-1]
    print(f"=== Tiến trình gần nhất (đợt {last_record['batch_no']}) ===")
    for query_name, query_record in last_record["queries"].items():
        micro_batches = [summarize_micro_batch(progress) for progress in query_record["progress"]]
        summary["latest"][query_name] = micro_batches
        for item in micro_batches:
            print(f"{query_name:10s} batch {item['batch_id']:>3}: đầu vào {item['num_input_rows']:>4} dòng, "
                  f"{item['processed_rows_per_second']:>7} dòng/giây, watermark {item['watermark']}, "
                  f"state {item['state_rows_total']}, bị loại vì quá muộn {item['rows_dropped_by_watermark']}")

    print("\n=== Theo đợt: số dòng đầu vào, watermark sau đợt, số dòng bị loại (Gold) ===")
    for batch_summary in summary["per_batch"]:
        cells = [f"đợt {batch_summary['batch_no']:>2}"]
        for query_name, micro_batches in batch_summary["queries"].items():
            input_rows = sum(item["num_input_rows"] or 0 for item in micro_batches)
            watermark = micro_batches[-1]["watermark"] if micro_batches else None
            dropped = sum(item["rows_dropped_by_watermark"] or 0 for item in micro_batches)
            cell = f"{query_name}: vào {input_rows}"
            if watermark and not watermark.startswith("1970"):
                cell += f", wm {watermark[11:19]}"
            if query_name.startswith("gold"):
                cell += f", loại {dropped}"
            cells.append(cell)
        print(" | ".join(cells))

    PROGRESS_SUMMARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_SUMMARY_FILE.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\nĐã lưu {PROGRESS_SUMMARY_FILE.relative_to(PROJECT_DIR)}")
    return True


def show_history():
    from common.spark_session import get_spark

    spark = get_spark("inspect_history")
    result = {}
    for table_name in (config.BRONZE_TABLE, config.SILVER_TABLE, config.GOLD_TABLE):
        table_path = config.table_path(table_name)
        # Mỗi micro-batch có dữ liệu tạo một phiên bản mới của bảng Delta
        history = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`").orderBy("version")
        rows = history.select("version", "timestamp", "operation", "operationParameters",
                              "operationMetrics").collect()
        result[table_name] = [
            {
                "version": row.version,
                "timestamp": row.timestamp.isoformat(),
                "operation": row.operation,
                "epoch_id": (row.operationParameters or {}).get("epochId"),
                "rows_written": (row.operationMetrics or {}).get(
                    "numTargetRowsInserted", (row.operationMetrics or {}).get("numOutputRows")),
            }
            for row in rows
        ]
        print(f"=== DESCRIBE HISTORY {table_name}: {len(rows)} phiên bản ===")
        for item in result[table_name]:
            print(f"  v{item['version']:<3} {item['operation']:<18} epochId={item['epoch_id']}, "
                  f"số dòng ghi {item['rows_written']}")

    # Time travel: đọc Silver ở một phiên bản cũ
    silver_path = str(config.table_path(config.SILVER_TABLE))
    latest_version = result[config.SILVER_TABLE][-1]["version"]
    old_version = 1
    old_count = spark.read.format("delta").option("versionAsOf", old_version).load(silver_path).count()
    latest_count = spark.read.format("delta").load(silver_path).count()
    result["time_travel"] = {
        "table": config.SILVER_TABLE,
        "old_version": old_version,
        "old_row_count": old_count,
        "latest_version": latest_version,
        "latest_row_count": latest_count,
    }
    print(f"\n=== Time travel Silver ===\n  phiên bản {old_version}: {old_count} dòng"
          f"\n  phiên bản mới nhất {latest_version}: {latest_count} dòng")
    spark.stop()
    DELTA_HISTORY_FILE.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Đã lưu {DELTA_HISTORY_FILE.relative_to(PROJECT_DIR)}")
    return old_count < latest_count


def main(argv=None):
    parser = argparse.ArgumentParser(description="Xem tiến trình truy vấn và lịch sử Delta")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--history", action="store_true")
    args = parser.parse_args(argv)
    run_all = not (args.progress or args.history)
    ok = True
    if args.progress or run_all:
        ok = show_progress() and ok
    if args.history or run_all:
        ok = show_history() and ok
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
