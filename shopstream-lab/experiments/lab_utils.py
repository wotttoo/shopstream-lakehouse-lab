"""Tiện ích dùng chung cho ba thí nghiệm lỗi.

- Dọn trạng thái của thí nghiệm: CHỈ được xóa thư mục tên exp_* trong lakehouse/ và checkpoints/.
- Tạo lại topic Kafka.
- Chạy một truy vấn streaming trong tiến trình riêng.
- Chụp nội dung offsets/, commits/ và _delta_log/ ra file văn bản làm bằng chứng.
- Ghi ba con số: kỳ vọng, thực tế, chênh lệch.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from common import config  # noqa: E402

STREAMING_DIR = PROJECT_DIR / "streaming"
PRODUCER = PROJECT_DIR / "producer" / "produce.py"
EXPERIMENT_PREFIX = "exp_"


# ---------------------------------------------------------------------------
# Thư mục của thí nghiệm
# ---------------------------------------------------------------------------
def experiment_dirs(name, lakehouse_dir=config.LAKEHOUSE_DIR, checkpoint_dir=config.CHECKPOINT_DIR):
    """Thư mục bảng và checkpoint riêng của thí nghiệm, ví dụ lakehouse/exp_a, checkpoints/exp_a."""
    if not name.startswith(EXPERIMENT_PREFIX):
        raise ValueError(f"tên thí nghiệm phải bắt đầu bằng {EXPERIMENT_PREFIX}: {name}")
    return Path(lakehouse_dir) / name, Path(checkpoint_dir) / name


def safe_remove_experiment_dir(path, allowed_parents=(config.LAKEHOUSE_DIR, config.CHECKPOINT_DIR)):
    """Xóa một thư mục của thí nghiệm. Từ chối mọi đường dẫn không phải <lakehouse|checkpoints>/exp_*[/...]."""
    path = Path(path).resolve()
    parents = [Path(parent).resolve() for parent in allowed_parents]
    for parent in parents:
        try:
            relative = path.relative_to(parent)
        except ValueError:
            continue
        if relative.parts and relative.parts[0].startswith(EXPERIMENT_PREFIX):
            if path.exists():
                shutil.rmtree(path)
            return True
    raise ValueError(f"từ chối xóa {path}: chỉ được xóa thư mục exp_* trong lakehouse/ hoặc checkpoints/")


def reset_experiment(name, recreate_kafka_topic=True):
    """Bắt đầu thí nghiệm từ trạng thái sạch: xóa bảng, checkpoint của thí nghiệm và tạo lại topic."""
    for directory in experiment_dirs(name):
        safe_remove_experiment_dir(directory)
    if recreate_kafka_topic:
        run_python([str(PRODUCER), "--recreate-topic"], label="tạo lại topic")


# ---------------------------------------------------------------------------
# Chạy lệnh con
# ---------------------------------------------------------------------------
def run_python(arguments, label, extra_env=None, allow_failure=False):
    """Chạy một script Python trong tiến trình riêng. Trả về (mã thoát, stdout)."""
    env = dict(os.environ)
    env.pop("FAIL_AT_BATCH", None)
    if extra_env:
        env.update(extra_env)
    completed = subprocess.run(
        [sys.executable] + list(arguments), cwd=PROJECT_DIR, env=env, capture_output=True, text=True
    )
    for line in completed.stdout.splitlines():
        if line.startswith("[") or line.startswith("Đã"):
            print(f"    {line}")
    status = "OK" if completed.returncode == 0 else f"thoát với mã {completed.returncode}"
    print(f"  - {label}: {status}")
    if completed.returncode != 0 and not allow_failure:
        print(completed.stderr[-3000:])
        raise SystemExit(f"{label} lỗi ngoài dự kiến")
    return completed.returncode, completed.stdout + completed.stderr


def query_command(script_name, name, extra_args=()):
    """Lệnh chạy một truy vấn với thư mục bảng và checkpoint của thí nghiệm."""
    lakehouse_dir, checkpoint_dir = experiment_dirs(name)
    return [
        str(STREAMING_DIR / script_name),
        "--lakehouse-dir", str(lakehouse_dir),
        "--checkpoint-dir", str(checkpoint_dir),
        *extra_args,
    ]


# ---------------------------------------------------------------------------
# Chụp checkpoint và _delta_log
# ---------------------------------------------------------------------------
def _describe_directory(directory, max_chars_per_file=1500):
    lines = []
    directory = Path(directory)
    if not directory.exists():
        return [f"(không tồn tại: {directory.name})"]
    files = sorted(
        (path for path in directory.iterdir() if path.is_file() and not path.name.startswith(".")),
        key=lambda path: (len(path.name), path.name),
    )
    if not files:
        return ["(rỗng)"]
    for path in files:
        lines.append(f"--- {path.name} ---")
        content = path.read_text(encoding="utf-8", errors="replace")
        if len(content) > max_chars_per_file:
            content = content[:max_chars_per_file] + "\n... (cắt bớt)"
        lines.append(content.rstrip("\n"))
    return lines


def snapshot_state(checkpoint_dirs, delta_log_dirs, output_file, title):
    """Ghi nội dung offsets/, commits/ của các checkpoint và _delta_log/ của các bảng ra file văn bản."""
    lines = [f"# {title}", ""]
    for checkpoint in checkpoint_dirs:
        checkpoint = Path(checkpoint)
        for sub_dir in ("offsets", "commits"):
            lines.append(f"## {checkpoint.name}/{sub_dir}")
            lines.extend(_describe_directory(checkpoint / sub_dir))
            lines.append("")
    for table_dir in delta_log_dirs:
        table_dir = Path(table_dir)
        lines.append(f"## {table_dir.name}/_delta_log")
        lines.extend(_describe_directory(table_dir / "_delta_log"))
        lines.append("")
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_file


def list_names(directory):
    """Tên các file trong thư mục (bỏ file ẩn .crc), sắp theo số."""
    directory = Path(directory)
    if not directory.exists():
        return []
    names = [path.name for path in directory.iterdir() if path.is_file() and not path.name.startswith(".")]
    return sorted(names, key=lambda name: (len(name), name))


def delta_commits(table_dir):
    """Đọc _delta_log/*.json (không cần Spark): mỗi phiên bản một dict gồm operation, metrics, số file thêm."""
    commits = []
    log_dir = Path(table_dir) / "_delta_log"
    for name in list_names(log_dir):
        if not name.endswith(".json"):
            continue
        version = int(name.split(".")[0])
        commit = {"version": version, "operation": None, "metrics": {}, "added_files": 0, "txn": None}
        for line in (log_dir / name).read_text(encoding="utf-8").splitlines():
            action = json.loads(line)
            if "commitInfo" in action:
                commit["operation"] = action["commitInfo"].get("operation")
                commit["metrics"] = action["commitInfo"].get("operationMetrics", {})
            elif "add" in action:
                commit["added_files"] += 1
            elif "txn" in action:
                commit["txn"] = action["txn"]
        commits.append(commit)
    return commits


def next_batch_id(checkpoint_dir):
    """batchId mà truy vấn sẽ chạy tiếp: số lớn nhất trong offsets/ cộng 1 (0 nếu chưa có)."""
    numbers = [int(name) for name in list_names(Path(checkpoint_dir) / "offsets") if name.isdigit()]
    return max(numbers) + 1 if numbers else 0


def count_rows_with_spark(table_paths):
    """Đếm số dòng của các bảng Delta bằng một SparkSession ngắn (chạy trong tiến trình con)."""
    script = (
        "import json, sys\n"
        "from common.spark_session import get_spark\n"
        "spark = get_spark('count_rows')\n"
        "result = {}\n"
        "for path in sys.argv[1:]:\n"
        "    result[path] = spark.read.format('delta').load(path).count()\n"
        "print('COUNTS=' + json.dumps(result))\n"
        "spark.stop()\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(PROJECT_DIR)
    completed = subprocess.run(
        [sys.executable, "-c", script, *[str(path) for path in table_paths]],
        cwd=PROJECT_DIR, env=env, capture_output=True, text=True,
    )
    for line in completed.stdout.splitlines():
        if line.startswith("COUNTS="):
            counts = json.loads(line[len("COUNTS="):])
            return [counts[str(path)] for path in table_paths]
    raise SystemExit("đếm số dòng lỗi:\n" + completed.stderr[-3000:])


# ---------------------------------------------------------------------------
# Ghi kết quả
# ---------------------------------------------------------------------------
def make_result(name, expected, actual, **details):
    """Ba con số chính của thí nghiệm, kèm chi tiết."""
    return {
        "experiment": name,
        "expected_rows": int(expected),
        "actual_rows": int(actual),
        "difference": int(actual) - int(expected),
        **details,
    }


def write_result(result, results_dir=config.RESULTS_DIR):
    """Ghi results/<experiment>.json và in ba con số."""
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    output_file = results_dir / f"{result['experiment']}.json"
    output_file.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n[{result['experiment']}] kỳ vọng {result['expected_rows']} dòng, "
          f"thực tế {result['actual_rows']} dòng, chênh lệch {result['difference']}")
    print(f"Đã lưu {output_file}")
    return output_file


def load_golden():
    with open(config.GOLDEN_ANSWER_FILE, encoding="utf-8") as golden_file:
        return json.load(golden_file)
