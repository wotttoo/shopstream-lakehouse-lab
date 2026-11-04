"""Thí nghiệm A: Silver lỗi giữa chừng, chạy lại cùng checkpoint, ghi idempotent (MERGE).

Các bước:
1. Trạng thái sạch: xóa lakehouse/exp_a, checkpoints/exp_a, tạo lại topic.
2. Chạy 12 đợt qua Bronze và Silver trong thư mục exp_a.
3. Ở đợt FAIL_ROUND, chạy Silver với FAIL_AT_BATCH = batchId của đợt đó: foreachBatch ném lỗi
   NGAY SAU khi MERGE xong, TRƯỚC khi hàm kết thúc (sink đã nhận dữ liệu, Spark chưa ghi commits).
4. Chụp offsets, commits, _delta_log (bằng chứng "trước"), chạy lại Silver không đặt biến lỗi,
   cùng checkpoint, chụp lại (bằng chứng "sau").
5. So số dòng Silver cuối cùng với số event_id duy nhất.

Gold không ảnh hưởng tới số dòng Silver nên không chạy trong thí nghiệm này.

Cách chạy:
    python experiments/exp_a_rerun_idempotent.py
"""

import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
for path in (PROJECT_DIR, PROJECT_DIR / "experiments"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import lab_utils  # noqa: E402

from common import config  # noqa: E402

EXPERIMENT = "exp_a"
FAIL_ROUND = 6


def run_experiment(experiment=EXPERIMENT, fail_round=FAIL_ROUND, silver_extra_args=()):
    """Chạy kịch bản lỗi rồi chạy lại. Dùng chung cho thí nghiệm A và B (B truyền thêm --append-only)."""
    golden = lab_utils.load_golden()
    lakehouse_dir, checkpoint_dir = lab_utils.experiment_dirs(experiment)
    silver_checkpoint = checkpoint_dir / config.SILVER_TABLE
    silver_table = lakehouse_dir / config.SILVER_TABLE
    evidence_dirs = ([silver_checkpoint], [silver_table])

    print(f"=== {experiment}: dọn trạng thái ===")
    lab_utils.reset_experiment(experiment)

    details = {}
    for batch_no in range(1, config.BATCH_COUNT + 1):
        print(f"=== {experiment}: đợt {batch_no} ===")
        lab_utils.run_python([str(lab_utils.PRODUCER), "--batch", str(batch_no)], f"gửi đợt {batch_no}")
        lab_utils.run_python(lab_utils.query_command("01_kafka_to_bronze.py", experiment), "bronze")
        silver_command = lab_utils.query_command("02_bronze_to_silver.py", experiment, silver_extra_args)
        if batch_no != fail_round:
            lab_utils.run_python(silver_command, "silver")
            continue

        # --- Gây lỗi ở batch có dữ liệu của đợt này ---
        failed_batch_id = lab_utils.next_batch_id(silver_checkpoint)
        versions_before_failure = len(lab_utils.delta_commits(silver_table))
        exit_code, _ = lab_utils.run_python(
            silver_command, f"silver với FAIL_AT_BATCH={failed_batch_id}",
            extra_env={"FAIL_AT_BATCH": str(failed_batch_id)}, allow_failure=True,
        )
        if exit_code == 0:
            raise SystemExit("Silver không lỗi như dự kiến")
        commits_after_failure = lab_utils.delta_commits(silver_table)
        failed_commit = commits_after_failure[-1]
        rows_written_by_failed_batch = int(
            failed_commit["metrics"].get("numTargetRowsInserted", failed_commit["metrics"].get("numOutputRows", 0))
        )
        silver_rows_after_failure = lab_utils.count_rows_with_spark([silver_table])[0]
        lab_utils.snapshot_state(*evidence_dirs, config.EVIDENCE_DIR / f"{experiment}_before.txt",
                                 f"{experiment}: sau khi Silver lỗi ở batch {failed_batch_id}, TRƯỚC khi chạy lại")
        offsets_before = lab_utils.list_names(silver_checkpoint / "offsets")
        commits_before = lab_utils.list_names(silver_checkpoint / "commits")

        # --- Chạy lại cùng checkpoint, không đặt biến lỗi ---
        lab_utils.run_python(silver_command, "silver chạy lại (cùng checkpoint)")
        lab_utils.snapshot_state(*evidence_dirs, config.EVIDENCE_DIR / f"{experiment}_after.txt",
                                 f"{experiment}: SAU khi chạy lại Silver cùng checkpoint")
        commits_after_rerun = lab_utils.delta_commits(silver_table)
        rerun_commits = commits_after_rerun[len(commits_after_failure):]
        details = {
            "fail_round": fail_round,
            "failed_batch_id": failed_batch_id,
            "delta_version_written_by_failed_batch": failed_commit["version"],
            "new_delta_versions_by_failed_run": len(commits_after_failure) - versions_before_failure,
            "rows_written_by_failed_batch": rows_written_by_failed_batch,
            "silver_rows_after_failure": silver_rows_after_failure,
            "offsets_before_rerun": offsets_before,
            "commits_before_rerun": commits_before,
            "offsets_after_rerun": lab_utils.list_names(silver_checkpoint / "offsets"),
            "commits_after_rerun": lab_utils.list_names(silver_checkpoint / "commits"),
            "delta_commits_by_rerun": [
                {"version": commit["version"], "operation": commit["operation"],
                 "rows_written": commit["metrics"].get("numTargetRowsInserted",
                                                       commit["metrics"].get("numOutputRows"))}
                for commit in rerun_commits
            ],
            "evidence_files": [f"results/evidence/{experiment}_before.txt",
                               f"results/evidence/{experiment}_after.txt"],
        }

    silver_rows, bronze_rows = lab_utils.count_rows_with_spark([silver_table, lakehouse_dir / config.BRONZE_TABLE])
    expected = golden["unique_event_ids"]
    return lab_utils.make_result(
        experiment, expected=expected, actual=silver_rows,
        layer="silver", bronze_rows=bronze_rows, expected_bronze_rows=golden["total_records_sent"], **details,
    )


def main():
    result = run_experiment()
    result["description"] = ("Silver lỗi sau khi MERGE xong, chạy lại cùng checkpoint; "
                             "MERGE chỉ chèn nên batch chạy lại không sinh dòng thừa")
    lab_utils.write_result(result)


if __name__ == "__main__":
    main()
