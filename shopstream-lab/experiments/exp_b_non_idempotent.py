"""Thí nghiệm B: giống thí nghiệm A nhưng Silver ghi KHÔNG idempotent.

Biến thể Silver (--append-only) ghi append thẳng trong foreachBatch: không MERGE, không txnAppId.
Khi chạy lại batch bị lỗi, dữ liệu của batch đó được ghi thêm một lần nữa.

Kỳ vọng: số dòng dư đúng bằng số dòng của batch bị chạy lại.

Cách chạy:
    python experiments/exp_b_non_idempotent.py
"""

import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
for path in (PROJECT_DIR, PROJECT_DIR / "experiments"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import lab_utils  # noqa: E402
from exp_a_rerun_idempotent import FAIL_ROUND, run_experiment  # noqa: E402

EXPERIMENT = "exp_b"


def main():
    result = run_experiment(EXPERIMENT, FAIL_ROUND, silver_extra_args=["--append-only"])
    replayed_rows = result["rows_written_by_failed_batch"]
    result["replayed_batch_id"] = result["failed_batch_id"]
    result["replayed_batch_rows"] = replayed_rows
    result["excess_equals_replayed_batch_rows"] = result["difference"] == replayed_rows
    result["description"] = (
        f"Silver ghi append (không MERGE, không txnAppId); batch {result['failed_batch_id']} "
        f"({replayed_rows} dòng) bị chạy lại nên được ghi hai lần"
    )
    lab_utils.write_result(result)
    if not result["excess_equals_replayed_batch_rows"]:
        print(f"CẢNH BÁO: số dòng dư {result['difference']} khác số dòng của batch chạy lại {replayed_rows}")


if __name__ == "__main__":
    main()
