"""Tổng hợp mọi kết quả trong results/ và data/ thành results/run_report.json và run_report.md.

Mọi con số đọc từ file kết quả của lần chạy thật, không gõ tay.

Cách chạy:
    python verify/build_report.py
"""

import json
import platform
import re
import subprocess
import sys
from importlib import metadata
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

import pandas as pd  # noqa: E402

from common import config  # noqa: E402

REPORT_JSON = config.RESULTS_DIR / "run_report.json"
REPORT_MD = config.RESULTS_DIR / "run_report.md"
EXPERIMENTS = ("exp_a", "exp_b", "exp_c")
PLAN_FILE = PROJECT_DIR.parent / "PLAN.md"


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Bộ phiên bản
# ---------------------------------------------------------------------------
def collect_versions():
    java_home = None
    activate = PROJECT_DIR / ".venv" / "bin" / "activate"
    if activate.exists():
        match = re.search(r"JAVA_HOME=(\S+)", activate.read_text(encoding="utf-8"))
        java_home = match.group(1) if match else None
    java_binary = str(Path(java_home) / "bin" / "java") if java_home else "java"
    java_output = subprocess.run([java_binary, "-version"], capture_output=True, text=True).stderr
    java_match = re.search(r'version "([^"]+)"', java_output)
    compose = (PROJECT_DIR / "docker-compose.yml").read_text(encoding="utf-8")
    image_match = re.search(r"image:\s*(\S+)", compose)
    import delta  # noqa: F401
    import pyspark

    return {
        "java": java_match.group(1) if java_match else java_output.strip(),
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "delta_spark": metadata.version("delta-spark"),
        "delta_jar": f"io.delta:delta-spark_4.1_2.13:{metadata.version('delta-spark')}",
        "kafka_connector": config.KAFKA_CONNECTOR_PACKAGE,
        "kafka_image": image_match.group(1) if image_match else None,
        "kafka_python": metadata.version("kafka-python"),
        "pandas": metadata.version("pandas"),
        "numpy": metadata.version("numpy"),
        "pytest": metadata.version("pytest"),
    }


# ---------------------------------------------------------------------------
# Bảng 9: bộ dữ liệu và đáp án vàng
# ---------------------------------------------------------------------------
def dataset_table(golden):
    keys = [
        ("total_records_sent", "Tổng số bản ghi gửi đi (kể cả trùng và sự kiện đẩy watermark)"),
        ("unique_event_ids", "Số event_id duy nhất"),
        ("business_unique_events", "Số sự kiện nghiệp vụ duy nhất (bỏ sự kiện đẩy watermark)"),
        ("duplicate_records", "Số bản trùng"),
        ("duplicate_identical_records", "  trong đó giống hệt bản gốc"),
        ("duplicate_shifted_records", "  trong đó event_time lệch vài giây"),
        ("silver_rows_if_dedup_by_event_id_and_time", "Số dòng Silver nếu lọc trùng theo (event_id, event_time)"),
        ("late_within_count", "Số sự kiện đến muộn trong ngưỡng (1 đến 4 phút)"),
        ("late_beyond_count_wm10", "Số sự kiện đến muộn quá ngưỡng (ngưỡng Gold 10 phút)"),
        ("late_beyond_purchases_wm10", "  trong đó purchase"),
        ("users", "Số user"),
        ("products", "Số sản phẩm"),
        ("purchases_unique", "Số purchase duy nhất"),
        ("revenue_absolute", "Doanh thu tuyệt đối (đồng)"),
    ]
    rows = [{"key": key, "label": label, "value": golden[key]} for key, label in keys]
    for event_type, count in golden["event_type_counts"].items():
        rows.append({"key": f"event_type_{event_type}", "label": f"Số sự kiện {event_type}", "value": count})
    return rows


# ---------------------------------------------------------------------------
# Bảng 4 và III.4: so sánh ngưỡng
# ---------------------------------------------------------------------------
def gold_dropped_from_progress(batch_records, query_name):
    per_batch = {}
    for record in batch_records:
        query = record["queries"].get(query_name)
        if not query:
            continue
        per_batch[record["batch_no"]] = sum(
            operator.get("numRowsDroppedByWatermark", 0)
            for progress in query["progress"] for operator in progress.get("stateOperators", [])
        )
    return per_batch


def watermark_comparison(golden, golden_windows, check, batch_records):
    result = {}
    for suffix, minutes in (("", config.GOLD_WATERMARK_MINUTES),
                            (config.GOLD_COMPARE_SUFFIX, config.GOLD_COMPARE_WATERMARK_MINUTES)):
        gold = check["gold"].get(suffix)
        if gold is None:
            continue
        windows = []
        for row in golden_windows.itertuples():
            actual = gold["windows"].get(row.window_start)
            actual_revenue, actual_orders = (actual[0] if actual else (None, None))
            close = gold["close"].get(row.window_start, {})
            windows.append({
                "window_start": row.window_start,
                "window_end": row.window_end,
                "revenue_absolute": int(row.revenue_absolute),
                "orders_absolute": int(row.orders_absolute),
                "revenue_expected": int(getattr(row, f"revenue_wm{minutes}")),
                "orders_expected": int(getattr(row, f"orders_wm{minutes}")),
                "revenue_actual": actual_revenue,
                "orders_actual": actual_orders,
                "dropped_purchases": int(row.orders_absolute) - int(actual_orders or 0),
                "close_batch_expected": int(getattr(row, f"close_batch_wm{minutes}")),
                "close_batch_actual": close.get("batch_no"),
                "close_epoch_id": close.get("epoch_id"),
            })
        revenue_actual = sum(window["revenue_actual"] or 0 for window in windows)
        orders_actual = sum(window["orders_actual"] or 0 for window in windows)
        dropped_rows = gold_dropped_from_progress(batch_records, f"gold{suffix}")
        result[f"wm{minutes}"] = {
            "watermark_minutes": minutes,
            "late_events_expected": golden[f"late_beyond_count_wm{minutes}"],
            "late_purchases_expected": golden[f"late_beyond_purchases_wm{minutes}"],
            "late_purchases_actual": golden["purchases_unique"] - orders_actual,
            "num_rows_dropped_by_watermark_total": sum(dropped_rows.values()),
            "num_rows_dropped_by_watermark_per_batch": dropped_rows,
            "revenue_actual_total": revenue_actual,
            "orders_actual_total": orders_actual,
            "revenue_absolute_total": golden["revenue_absolute"],
            "revenue_gap_to_absolute": golden["revenue_absolute"] - revenue_actual,
            "revenue_gap_pct": round(100 * (golden["revenue_absolute"] - revenue_actual)
                                     / golden["revenue_absolute"], 2),
            "windows": windows,
        }
    return result


def per_batch_table(golden, batch_records):
    rows = []
    for record, expected in zip(batch_records, golden["per_batch"]):
        row = {"batch_no": record["batch_no"], "records_sent": expected["records_sent"],
               "row_counts": record["row_counts"], "micro_batch_counts": record["micro_batch_counts"]}
        for query_name, query in record["queries"].items():
            last = query["progress"][-1] if query["progress"] else {}
            row[f"{query_name}_watermark_after"] = (last.get("eventTime") or {}).get("watermark")
        row["gold_watermark_after_expected"] = expected["gold_watermark_after_wm10"]
        row["gold_wm20_watermark_after_expected"] = expected["gold_watermark_after_wm20"]
        row["silver_watermark_after_expected"] = expected["silver_watermark_after"]
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Bảng 10: thí nghiệm lỗi
# ---------------------------------------------------------------------------
def experiments_table():
    rows = {}
    for name in EXPERIMENTS:
        path = config.RESULTS_DIR / f"{name}.json"
        rows[name] = read_json(path) if path.exists() else None
    return rows


# ---------------------------------------------------------------------------
# Mẫu checkpoint và _delta_log
# ---------------------------------------------------------------------------
def tail_files(directory, count=2, max_chars=1200, extension=None):
    directory = Path(directory)
    if not directory.exists():
        return {}
    names = sorted((path.name for path in directory.iterdir()
                    if path.is_file() and not path.name.startswith(".") and not path.name.endswith(".crc")
                    and (extension is None or path.name.endswith(extension))),
                   key=lambda name: (len(name), name))
    return {name: (directory / name).read_text(encoding="utf-8")[:max_chars] for name in names[-count:]}


def evidence_samples():
    samples = {"standard_run": {}}
    for query in ("bronze", "silver", "gold"):
        checkpoint = config.checkpoint_path(query)
        samples["standard_run"][query] = {
            "offsets": tail_files(checkpoint / "offsets"),
            "commits": tail_files(checkpoint / "commits"),
            "delta_log": tail_files(config.table_path(query) / "_delta_log", count=1, max_chars=2500,
                                    extension=".json"),
        }
    samples["experiment_files"] = sorted(
        str(path.relative_to(PROJECT_DIR)) for path in config.EVIDENCE_DIR.glob("*.txt"))
    return samples


# ---------------------------------------------------------------------------
# Bất thường (đọc từ mục Vướng mắc của PLAN.md)
# ---------------------------------------------------------------------------
def anomalies_from_plan():
    if not PLAN_FILE.exists():
        return []
    text = PLAN_FILE.read_text(encoding="utf-8")
    section = text.split("## 5. Vướng mắc", 1)[-1]
    items = []
    for block in re.split(r"\n### ", section)[1:]:
        title, _, body = block.partition("\n")
        items.append({"title": title.strip(), "details": body.strip()})
    return items


# ---------------------------------------------------------------------------
# Markdown
# ---------------------------------------------------------------------------
def money(value):
    return "-" if value is None else f"{value:,}".replace(",", ".")


def build_markdown(report):
    lines = ["# Báo cáo kết quả chạy ShopStream Lab", "",
             "Mọi số liệu dưới đây sinh tự động từ `results/` và `data/` bằng `verify/build_report.py`.", ""]
    lines += ["## 1. Bộ phiên bản đã chạy thật", "", "| Thành phần | Phiên bản |", "|---|---|"]
    lines += [f"| {key} | {value} |" for key, value in report["versions"].items()]

    lines += ["", "## 2. Bảng 9: thống kê bộ dữ liệu và đáp án vàng", "", "| Chỉ số | Giá trị |", "|---|---|"]
    lines += [f"| {row['label'].strip()} | {money(row['value']) if 'revenue' in row['key'] else row['value']} |"
              for row in report["dataset"]]

    lines += ["", "## 3. Bảng 4: so sánh ngưỡng watermark Gold", "",
              "| Ngưỡng | Sự kiện quá ngưỡng (đáp án vàng) | Purchase bị loại (đo từ Gold) | "
              "numRowsDroppedByWatermark | Doanh thu Gold | Chênh lệch so với tuyệt đối | Tỷ lệ |",
              "|---|---|---|---|---|---|---|"]
    for name, item in report["watermark_comparison"].items():
        lines.append(
            f"| {item['watermark_minutes']} phút | {item['late_events_expected']} | {item['late_purchases_actual']} | "
            f"{item['num_rows_dropped_by_watermark_total']} | {money(item['revenue_actual_total'])} | "
            f"{money(item['revenue_gap_to_absolute'])} | {item['revenue_gap_pct']}% |")
    absolute = report["dataset_lookup"]["revenue_absolute"]
    lines.append(f"\nDoanh thu tuyệt đối: {money(absolute)} đồng.")

    lines += ["", "## 4. Bảng phần III.4: doanh thu từng cửa sổ và đợt chốt cửa sổ", "",
              "| Cửa sổ | Tuyệt đối (đơn) | 10 phút: doanh thu (đơn) | 10 phút: bị loại | 10 phút: đợt chốt | "
              "20 phút: doanh thu (đơn) | 20 phút: bị loại | 20 phút: đợt chốt |",
              "|---|---|---|---|---|---|---|---|"]
    comparison = report["watermark_comparison"]
    if "wm10" in comparison and "wm20" in comparison:
        for w10, w20 in zip(comparison["wm10"]["windows"], comparison["wm20"]["windows"]):
            lines.append(
                f"| {w10['window_start'][11:16]}-{w10['window_end'][11:16]} | "
                f"{money(w10['revenue_absolute'])} ({w10['orders_absolute']}) | "
                f"{money(w10['revenue_actual'])} ({w10['orders_actual']}) | {w10['dropped_purchases']} | "
                f"{w10['close_batch_actual']} | "
                f"{money(w20['revenue_actual'])} ({w20['orders_actual']}) | {w20['dropped_purchases']} | "
                f"{w20['close_batch_actual']} |")

    lines += ["", "## 5. Watermark và số dòng theo đợt", "",
              "| Đợt | Bản ghi gửi | Bronze | Silver | Gold | Gold 20 phút | Watermark Silver | "
              "Watermark Gold 10 phút | Watermark Gold 20 phút |",
              "|---|---|---|---|---|---|---|---|---|"]
    for row in report["per_batch"]:
        counts = row["row_counts"]
        lines.append(
            f"| {row['batch_no']} | {row['records_sent']} | {counts.get('bronze')} | {counts.get('silver')} | "
            f"{counts.get('gold')} | {counts.get('gold_wm20', '-')} | "
            f"{(row.get('silver_watermark_after') or '')[11:19]} | {(row.get('gold_watermark_after') or '')[11:19]} | "
            f"{(row.get('gold_wm20_watermark_after') or '')[11:19]} |")

    lines += ["", "## 6. Bảng 10: kết quả ba thí nghiệm lỗi", "",
              "| Thí nghiệm | Tầng đo | Kỳ vọng | Thực tế | Chênh lệch | Ghi chú |", "|---|---|---|---|---|---|"]
    notes = {
        "exp_a": lambda r: (f"lỗi ở batch {r['failed_batch_id']} (đợt {r['fail_round']}), batch đó đã ghi "
                            f"{r['rows_written_by_failed_batch']} dòng; chạy lại cùng checkpoint, MERGE chèn "
                            f"{sum(int(c['rows_written'] or 0) for c in r['delta_commits_by_rerun'])} dòng"),
        "exp_b": lambda r: (f"batch {r['replayed_batch_id']} chạy lại, {r['replayed_batch_rows']} dòng bị ghi hai lần"),
        "exp_c": lambda r: (f"Bronze đọc lại {r['bronze_rows_reread']} dòng; Silver trước {r['silver_rows_before']}, "
                            f"sau {r['silver_rows_after']} (kỳ vọng {r['silver_expected_rows']})"),
    }
    for name, result in report["experiments"].items():
        if result is None:
            lines.append(f"| {name} | - | - | - | - | chưa chạy |")
            continue
        lines.append(f"| {name} | {result['layer']} | {result['expected_rows']} | {result['actual_rows']} | "
                     f"{result['difference']} | {notes[name](result)} |")

    lines += ["", "## 7. Mẫu checkpoint và _delta_log (lần chạy chuẩn)", ""]
    for query, sample in report["evidence"]["standard_run"].items():
        for kind in ("offsets", "commits", "delta_log"):
            for file_name, content in sample[kind].items():
                lines += [f"**checkpoints/{query}/{kind}/{file_name}**" if kind != "delta_log"
                          else f"**lakehouse/{query}/_delta_log/{file_name}**", "", "```", content.rstrip(), "```", ""]
    lines += ["Bằng chứng của thí nghiệm A, B (trước và sau khi chạy lại):", ""]
    lines += [f"- `{path}`" for path in report["evidence"]["experiment_files"]]

    lines += ["", "## 8. Bất thường và cách xử lý", ""]
    for item in report["anomalies"]:
        lines += [f"### {item['title']}", "", item["details"], ""]
    lines += ["## 9. Kiểm tra đầu cuối", "",
              f"`verify/check_results.py`: {'KHỚP' if report['checks']['all_ok'] else 'CÓ DÒNG SAI'} "
              f"({report['checks']['row_count']} dòng đối chiếu, {report['checks']['failed_rows']} dòng sai).", ""]
    return "\n".join(lines) + "\n"


def build_report():
    golden = read_json(config.GOLDEN_ANSWER_FILE)
    golden_windows = pd.read_csv(config.GOLDEN_WINDOWS_FILE)
    check = read_json(config.RESULTS_DIR / "check_results.json")
    batch_records = [read_json(path) for path in sorted(config.PROGRESS_DIR.glob("batch_*.json"))]
    report = {
        "versions": collect_versions(),
        "dataset": dataset_table(golden),
        "dataset_lookup": {key: value for key, value in golden.items() if not isinstance(value, (list, dict))},
        "watermark_comparison": watermark_comparison(golden, golden_windows, check, batch_records),
        "per_batch": per_batch_table(golden, batch_records),
        "experiments": experiments_table(),
        "evidence": evidence_samples(),
        "anomalies": anomalies_from_plan(),
        "checks": {
            "all_ok": check["all_ok"],
            "row_count": len(check["rows"]),
            "failed_rows": sum(1 for row in check["rows"] if not row["ok"]),
        },
    }
    history_file = config.RESULTS_DIR / "delta_history.json"
    if history_file.exists():
        report["time_travel"] = read_json(history_file).get("time_travel")
    REPORT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    REPORT_MD.write_text(build_markdown(report), encoding="utf-8")
    print(f"Đã ghi {REPORT_JSON.relative_to(PROJECT_DIR)} và {REPORT_MD.relative_to(PROJECT_DIR)}")
    return report


if __name__ == "__main__":
    build_report()
