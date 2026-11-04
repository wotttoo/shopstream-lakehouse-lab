"""Đối chiếu Bronze, Silver, Gold với đáp án vàng. Thoát với mã khác 0 nếu có dòng sai.

Cách chạy:
    python verify/check_results.py                      # mọi tầng, sau đủ 12 đợt
    python verify/check_results.py --layer bronze --up-to-batch 1
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Đối chiếu kết quả với đáp án vàng")
    parser.add_argument("--layer", choices=LAYERS + ("all",), default="all")
    parser.add_argument("--up-to-batch", type=int, default=config.BATCH_COUNT)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    from common.spark_session import get_spark

    golden = load_golden()
    spark = get_spark("check_results")
    table = CheckTable()
    layers = LAYERS if args.layer == "all" else (args.layer,)
    if "bronze" in layers:
        check_bronze(spark, golden, args.up_to_batch, table)
    spark.stop()

    print()
    table.print()
    print("\nKẾT QUẢ:", "KHỚP" if table.all_ok else "CÓ DÒNG SAI")
    sys.exit(0 if table.all_ok else 1)


if __name__ == "__main__":
    main()
