"""Kiểm tra results/run_report.json (và tùy chọn run_report.md) có đủ nội dung theo mục 13 của CLAUDE.md.

Cách chạy:
    python verify/check_report.py
    python verify/check_report.py --markdown
"""

import argparse
import json
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from common import config  # noqa: E402

REPORT_JSON = config.RESULTS_DIR / "run_report.json"
REPORT_MD = config.RESULTS_DIR / "run_report.md"

VERSION_KEYS = ("java", "python", "pyspark", "delta_spark", "kafka_connector", "kafka_image")
DATASET_KEYS = ("total_records_sent", "unique_event_ids", "duplicate_records", "duplicate_shifted_records",
                "late_within_count", "late_beyond_count_wm10", "late_beyond_purchases_wm10", "revenue_absolute")
EXPERIMENT_KEYS = ("expected_rows", "actual_rows", "difference")
MARKDOWN_HEADINGS = ("Bộ phiên bản", "Bảng 9", "Bảng 4", "Bảng phần III.4", "Bảng 10",
                     "Mẫu checkpoint", "Bất thường")


def check_report(report):
    problems = []
    for key in VERSION_KEYS:
        if not report.get("versions", {}).get(key):
            problems.append(f"thiếu phiên bản: {key}")
    dataset_keys = {row["key"] for row in report.get("dataset", [])}
    for key in DATASET_KEYS:
        if key not in dataset_keys:
            problems.append(f"thiếu thống kê bộ dữ liệu: {key}")
    comparison = report.get("watermark_comparison", {})
    for name in ("wm10", "wm20"):
        item = comparison.get(name)
        if not item:
            problems.append(f"thiếu so sánh ngưỡng {name}")
            continue
        if len(item.get("windows", [])) != 12:
            problems.append(f"{name}: không đủ 12 cửa sổ")
        for window in item.get("windows", []):
            if window.get("revenue_actual") is None or window.get("close_batch_actual") is None:
                problems.append(f"{name}: cửa sổ {window.get('window_start')} thiếu doanh thu hoặc đợt chốt")
        for key in ("late_events_expected", "late_purchases_actual", "num_rows_dropped_by_watermark_total"):
            if key not in item:
                problems.append(f"{name}: thiếu {key}")
    if "wm10" in comparison and "wm20" in comparison:
        if not comparison["wm20"]["revenue_gap_to_absolute"] < comparison["wm10"]["revenue_gap_to_absolute"]:
            problems.append("chênh lệch doanh thu của 20 phút không nhỏ hơn của 10 phút")
    for name in ("exp_a", "exp_b", "exp_c"):
        result = report.get("experiments", {}).get(name)
        if not result:
            problems.append(f"thiếu kết quả thí nghiệm {name}")
            continue
        for key in EXPERIMENT_KEYS:
            if key not in result:
                problems.append(f"{name}: thiếu {key}")
    standard = report.get("evidence", {}).get("standard_run", {})
    for query in ("bronze", "silver", "gold"):
        sample = standard.get(query, {})
        for kind in ("offsets", "commits", "delta_log"):
            if not sample.get(kind):
                problems.append(f"thiếu mẫu {kind} của {query}")
    evidence_files = report.get("evidence", {}).get("experiment_files", [])
    for name in ("exp_a_before", "exp_a_after", "exp_b_before", "exp_b_after"):
        if not any(name in path for path in evidence_files):
            problems.append(f"thiếu file bằng chứng {name}")
    if "anomalies" not in report:
        problems.append("thiếu mục bất thường")
    if not report.get("checks", {}).get("all_ok"):
        problems.append("check_results.py chưa khớp")
    return problems


def check_markdown(text):
    problems = []
    for heading in MARKDOWN_HEADINGS:
        if heading not in text:
            problems.append(f"run_report.md thiếu mục: {heading}")
    if sum(1 for line in text.splitlines() if line.startswith("|")) == 0:
        problems.append("run_report.md không có bảng nào")
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description="Kiểm tra run_report đủ nội dung")
    parser.add_argument("--markdown", action="store_true", help="kiểm tra thêm run_report.md")
    args = parser.parse_args(argv)
    if not REPORT_JSON.exists():
        print("Chưa có results/run_report.json, hãy chạy verify/build_report.py")
        sys.exit(1)
    problems = check_report(json.loads(REPORT_JSON.read_text(encoding="utf-8")))
    if args.markdown:
        if REPORT_MD.exists():
            problems += check_markdown(REPORT_MD.read_text(encoding="utf-8"))
        else:
            problems.append("chưa có results/run_report.md")
    for experiment in ("exp_a", "exp_b", "exp_c"):
        path = config.RESULTS_DIR / f"{experiment}.json"
        if path.exists():
            result = json.loads(path.read_text(encoding="utf-8"))
            print(f"{experiment}: kỳ vọng {result['expected_rows']}, thực tế {result['actual_rows']}, "
                  f"chênh lệch {result['difference']}")
    if problems:
        print("THIẾU NỘI DUNG:")
        for problem in problems:
            print(f"  - {problem}")
        sys.exit(1)
    print("run_report đủ nội dung: OK")


if __name__ == "__main__":
    main()
