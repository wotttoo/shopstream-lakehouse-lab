"""Unit test cho verify/check_report.py với báo cáo nhỏ dựng tay."""

import importlib.util
from pathlib import Path

from common import config

spec = importlib.util.spec_from_file_location(
    "check_report", Path(config.PROJECT_DIR) / "verify" / "check_report.py"
)
check_report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_report)


def complete_report():
    window = {"window_start": "w", "revenue_actual": 1, "close_batch_actual": 1}
    comparison_item = {"windows": [window] * 12, "late_events_expected": 1, "late_purchases_actual": 1,
                       "num_rows_dropped_by_watermark_total": 1}
    return {
        "versions": {key: "x" for key in check_report.VERSION_KEYS},
        "dataset": [{"key": key} for key in check_report.DATASET_KEYS],
        "watermark_comparison": {
            "wm10": dict(comparison_item, revenue_gap_to_absolute=100),
            "wm20": dict(comparison_item, revenue_gap_to_absolute=0),
        },
        "experiments": {name: {"expected_rows": 1, "actual_rows": 1, "difference": 0}
                        for name in ("exp_a", "exp_b", "exp_c")},
        "evidence": {
            "standard_run": {query: {"offsets": {"0": "x"}, "commits": {"0": "x"}, "delta_log": {"0": "x"}}
                             for query in ("bronze", "silver", "gold")},
            "experiment_files": [f"results/evidence/{name}.txt"
                                 for name in ("exp_a_before", "exp_a_after", "exp_b_before", "exp_b_after")],
        },
        "anomalies": [],
        "checks": {"all_ok": True},
    }


def test_complete_report_has_no_problems():
    assert check_report.check_report(complete_report()) == []


def test_missing_experiment_is_reported():
    report = complete_report()
    del report["experiments"]["exp_b"]
    assert any("exp_b" in problem for problem in check_report.check_report(report))


def test_wm20_gap_must_be_smaller_than_wm10():
    report = complete_report()
    report["watermark_comparison"]["wm20"]["revenue_gap_to_absolute"] = 100
    assert any("20 phút" in problem for problem in check_report.check_report(report))


def test_markdown_needs_tables_and_headings():
    assert check_report.check_markdown("không có gì") != []
    text = "\n".join(check_report.MARKDOWN_HEADINGS) + "\n| a | b |\n"
    assert check_report.check_markdown(text) == []
