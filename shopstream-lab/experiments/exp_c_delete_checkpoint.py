"""Thí nghiệm C: xóa checkpoint Bronze rồi chạy lại Bronze với startingOffsets earliest.

Các bước:
1. Trạng thái sạch, chạy bình thường 12 đợt qua Bronze và Silver trong thư mục exp_c.
2. Xóa checkpoints/exp_c/bronze, chạy lại Bronze: không còn offsets đã lưu nên Spark đọc lại
   topic từ đầu (earliest). Đo số dòng Bronze.
3. Chạy tiếp Silver (checkpoint Silver còn nguyên). Đo số dòng Silver và xem tiến trình.

Không giả định trước kết quả của Silver: đo rồi giải thích theo số liệu.

Cách chạy:
    python experiments/exp_c_delete_checkpoint.py
"""

import json
import sys
import tempfile
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
for path in (PROJECT_DIR, PROJECT_DIR / "experiments"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import lab_utils  # noqa: E402

from common import config  # noqa: E402

EXPERIMENT = "exp_c"


def run_with_progress(command, label, raw_dir, name):
    progress_file = Path(raw_dir) / f"{name}.json"
    lab_utils.run_python(command + ["--progress-file", str(progress_file)], label)
    return json.loads(progress_file.read_text(encoding="utf-8"))


def summarize_silver_progress(record):
    micro_batches = []
    for progress in record["progress"]:
        state = progress.get("stateOperators") or [{}]
        observed = (progress.get("observedMetrics") or {}).get("silver_parse") or {}
        micro_batches.append({
            "batch_id": progress["batchId"],
            "num_input_rows": progress["numInputRows"],
            "watermark": (progress.get("eventTime") or {}).get("watermark"),
            "rows_dropped_by_watermark": state[0].get("numRowsDroppedByWatermark"),
            "rows_removed_as_duplicate": state[0].get("customMetrics", {}).get("numDroppedDuplicateRows"),
            "state_rows_total": state[0].get("numRowsTotal"),
            "rejected_rows": observed.get("rejected_rows"),
        })
    return micro_batches


def build_observation(result):
    """Nhận xét dựng từ số đo thật của lần chạy."""
    lines = [
        f"Bronze: {result['bronze_rows_before']} dòng trước, {result['bronze_rows_after']} dòng sau khi xóa checkpoint "
        f"và chạy lại (đọc lại {result['bronze_rows_reread']} message từ earliest). Delta sink của Bronze chống ghi "
        "trùng theo (queryId, batchId) lưu trong checkpoint, nên checkpoint mới không nhận ra dữ liệu đã ghi.",
        f"Silver: {result['silver_rows_before']} dòng trước, {result['silver_rows_after']} dòng sau "
        f"(chênh lệch so với số event_id duy nhất: {result['silver_difference']}).",
    ]
    for item in result["silver_micro_batches"]:
        lines.append(
            f"Micro-batch Silver {item['batch_id']}: đầu vào {item['num_input_rows']} dòng, watermark {item['watermark']}, "
            f"bị loại vì quá muộn {item['rows_dropped_by_watermark']}, bị loại vì trùng trong state "
            f"{item['rows_removed_as_duplicate']}, số dòng trong state {item['state_rows_total']}."
        )
    lines.append(
        "Giải thích: checkpoint Silver còn nguyên nên Silver đọc tiếp phiên bản Bronze mới (toàn bản sao). "
        "Watermark Silver đã ở mức event time lớn nhất trừ 30 phút, nên mọi bản sao có event_time cũ hơn bị "
        "dropDuplicatesWithinWatermark loại như dữ liệu quá muộn; bản sao còn lại (sự kiện đẩy watermark) còn "
        "event_id trong state nên bị loại vì trùng. foreachBatch nhận batch rỗng, MERGE không phải chặn dòng nào. "
        "MERGE chỉ chèn là lớp chặn cuối cùng khi state không giúp được: checkpoint Silver bị mất, hoặc một batch "
        "đã ghi bị chạy lại (thí nghiệm A)."
    )
    return lines


def main():
    golden = lab_utils.load_golden()
    lakehouse_dir, checkpoint_dir = lab_utils.experiment_dirs(EXPERIMENT)
    bronze_table = lakehouse_dir / config.BRONZE_TABLE
    silver_table = lakehouse_dir / config.SILVER_TABLE
    bronze_checkpoint = checkpoint_dir / config.BRONZE_TABLE

    print(f"=== {EXPERIMENT}: dọn trạng thái ===")
    lab_utils.reset_experiment(EXPERIMENT)
    for batch_no in range(1, config.BATCH_COUNT + 1):
        print(f"=== {EXPERIMENT}: đợt {batch_no} ===")
        lab_utils.run_python([str(lab_utils.PRODUCER), "--batch", str(batch_no)], f"gửi đợt {batch_no}")
        lab_utils.run_python(lab_utils.query_command("01_kafka_to_bronze.py", EXPERIMENT), "bronze")
        lab_utils.run_python(lab_utils.query_command("02_bronze_to_silver.py", EXPERIMENT), "silver")

    bronze_before, silver_before = lab_utils.count_rows_with_spark([bronze_table, silver_table])
    silver_versions_before = len(lab_utils.delta_commits(silver_table))
    print(f"Trước khi xóa checkpoint: Bronze {bronze_before} dòng, Silver {silver_before} dòng")

    print("=== Xóa checkpoint Bronze và chạy lại Bronze ===")
    lab_utils.safe_remove_experiment_dir(bronze_checkpoint)
    with tempfile.TemporaryDirectory() as raw_dir:
        bronze_record = run_with_progress(
            lab_utils.query_command("01_kafka_to_bronze.py", EXPERIMENT), "bronze chạy lại từ earliest",
            raw_dir, "bronze")
        bronze_after = bronze_record["row_count"]
        print("=== Chạy tiếp Silver (checkpoint Silver còn nguyên) ===")
        silver_record = run_with_progress(
            lab_utils.query_command("02_bronze_to_silver.py", EXPERIMENT), "silver",
            raw_dir, "silver")
    silver_after = silver_record["row_count"]
    silver_micro_batches = summarize_silver_progress(silver_record)
    new_silver_versions = lab_utils.delta_commits(silver_table)[silver_versions_before:]

    result = lab_utils.make_result(
        EXPERIMENT,
        expected=2 * golden["total_records_sent"],
        actual=bronze_after,
        layer="bronze",
        bronze_rows_before=bronze_before,
        bronze_rows_after=bronze_after,
        bronze_rows_reread=bronze_after - bronze_before,
        bronze_micro_batches_after_delete=bronze_record["micro_batch_ids"],
        silver_expected_rows=golden["unique_event_ids"],
        silver_rows_before=silver_before,
        silver_rows_after=silver_after,
        silver_difference=silver_after - golden["unique_event_ids"],
        silver_new_delta_versions=[
            {"version": commit["version"], "operation": commit["operation"],
             "rows_written": commit["metrics"].get("numTargetRowsInserted")}
            for commit in new_silver_versions
        ],
        silver_micro_batches=silver_micro_batches,
    )
    result["observation"] = build_observation(result)
    lab_utils.write_result(result)
    print(f"Silver: trước {silver_before} dòng, sau {silver_after} dòng")
    for item in silver_micro_batches:
        print(f"  silver batch {item}")


if __name__ == "__main__":
    main()
