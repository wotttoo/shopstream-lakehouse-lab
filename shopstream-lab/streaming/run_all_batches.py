"""Chạy tuần tự từng đợt: gửi đợt đó vào Kafka, rồi chạy Bronze, Silver, Gold (availableNow).

Mỗi truy vấn chạy trong một tiến trình riêng (subprocess), giống như chạy tay từng script.

Cách chạy:
    python streaming/run_all_batches.py --reset                 # chạy lại từ trạng thái sạch, đủ 12 đợt
    python streaming/run_all_batches.py --reset --up-to-batch 2
    python streaming/run_all_batches.py --reset --compare-wm20  # thêm Gold 20 phút ở MỖI đợt
"""

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from common import config  # noqa: E402

STREAMING_DIR = PROJECT_DIR / "streaming"
PRODUCER = PROJECT_DIR / "producer" / "produce.py"


def run_step(label, command):
    """Chạy một lệnh con, in các dòng log chính, dừng cả vòng lặp nếu lệnh lỗi."""
    started = time.time()
    completed = subprocess.run(
        [sys.executable] + command, cwd=PROJECT_DIR, capture_output=True, text=True
    )
    for line in completed.stdout.splitlines():
        if line.startswith("[") or line.startswith("Đã"):
            print(f"    {line}")
    elapsed = time.time() - started
    if completed.returncode != 0:
        print(completed.stdout[-3000:])
        print(completed.stderr[-3000:])
        raise SystemExit(f"Bước {label} lỗi (mã {completed.returncode})")
    print(f"  - {label}: xong sau {elapsed:.1f} giây")
    return completed


def reset_standard_state():
    """Dọn trạng thái của lần chạy chuẩn: bảng Delta, checkpoint và topic (đều tái tạo được)."""
    for directory in (config.LAKEHOUSE_DIR, config.CHECKPOINT_DIR):
        for child in sorted(directory.glob("*")) if directory.exists() else []:
            # Không đụng tới thư mục của thí nghiệm lỗi
            if child.name.startswith("exp_"):
                continue
            shutil.rmtree(child)
    run_step("tạo lại topic", [str(PRODUCER), "--recreate-topic"])


def query_steps(compare_wm20):
    steps = [
        ("bronze", [str(STREAMING_DIR / "01_kafka_to_bronze.py")]),
        ("silver", [str(STREAMING_DIR / "02_bronze_to_silver.py")]),
        ("gold", [str(STREAMING_DIR / "03_silver_to_gold.py")]),
    ]
    if compare_wm20:
        steps.append((
            "gold" + config.GOLD_COMPARE_SUFFIX,
            [str(STREAMING_DIR / "03_silver_to_gold.py"),
             "--watermark", config.GOLD_COMPARE_WATERMARK,
             "--suffix", config.GOLD_COMPARE_SUFFIX],
        ))
    return steps


def run_batch(batch_no, compare_wm20):
    """Gửi một đợt rồi chạy các truy vấn; gom tiến trình vào results/progress/batch_XX.json."""
    print(f"=== Đợt {batch_no} ===")
    run_step(f"gửi đợt {batch_no}", [str(PRODUCER), "--batch", str(batch_no)])
    batch_record = {"batch_no": batch_no, "queries": {}}
    with tempfile.TemporaryDirectory() as raw_dir:
        for query_name, command in query_steps(compare_wm20):
            progress_file = Path(raw_dir) / f"{query_name}.json"
            run_step(query_name, command + ["--progress-file", str(progress_file)])
            batch_record["queries"][query_name] = json.loads(progress_file.read_text(encoding="utf-8"))
    batch_record["row_counts"] = {
        name: record["row_count"] for name, record in batch_record["queries"].items()
    }
    batch_record["micro_batch_counts"] = {
        name: record["micro_batch_count"] for name, record in batch_record["queries"].items()
    }
    config.PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = config.PROGRESS_DIR / f"batch_{batch_no:02d}.json"
    output_file.write_text(json.dumps(batch_record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"  số dòng: {batch_record['row_counts']}, số micro-batch: {batch_record['micro_batch_counts']}")
    return batch_record


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Chạy 12 đợt qua Bronze, Silver, Gold")
    parser.add_argument("--up-to-batch", type=int, default=config.BATCH_COUNT)
    parser.add_argument("--from-batch", type=int, default=1)
    parser.add_argument("--reset", action="store_true",
                        help="xóa lakehouse/, checkpoints/ (trừ exp_*) và tạo lại topic trước khi chạy")
    parser.add_argument("--compare-wm20", action="store_true",
                        help="chạy thêm Gold ngưỡng 20 phút ở MỖI đợt")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.reset:
        reset_standard_state()
    for batch_no in range(args.from_batch, args.up_to_batch + 1):
        run_batch(batch_no, args.compare_wm20)


if __name__ == "__main__":
    main()
