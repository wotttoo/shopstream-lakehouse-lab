"""Kiểm tra môi trường: Spark ghi đọc được Delta, Spark đọc được Kafka.

Cách chạy:
    python verify/smoke_env.py --delta
    python verify/smoke_env.py --kafka
    python verify/smoke_env.py --delta --kafka
"""

import argparse
import shutil
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).resolve().parent.parent
# Đường dẫn có dấu cách và ký tự "→" để thử Spark xử lý được tên thư mục lạ
SMOKE_DELTA_DIR = PROJECT_DIR / "lakehouse" / "_smoke" / "bảng thử → delta"
SMOKE_ROW_COUNT = 10

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"


def build_spark():
    builder = (
        SparkSession.builder.appName("smoke_env")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=[KAFKA_PACKAGE]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def check_delta(spark):
    """Ghi một bảng Delta nhỏ rồi đọc lại, so số dòng."""
    if SMOKE_DELTA_DIR.exists():
        shutil.rmtree(SMOKE_DELTA_DIR)
    spark.range(SMOKE_ROW_COUNT).write.format("delta").save(str(SMOKE_DELTA_DIR))
    read_count = spark.read.format("delta").load(str(SMOKE_DELTA_DIR)).count()
    print(f"[delta] đã ghi {SMOKE_ROW_COUNT} dòng, đọc lại {read_count} dòng")
    print(f"[delta] đường dẫn: {SMOKE_DELTA_DIR}")
    return read_count == SMOKE_ROW_COUNT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delta", action="store_true", help="thử ghi đọc Delta")
    parser.add_argument("--kafka", action="store_true", help="thử đọc Kafka")
    args = parser.parse_args()
    if not (args.delta or args.kafka):
        parser.error("chọn ít nhất một trong --delta, --kafka")

    spark = build_spark()
    print(f"[spark] phiên bản {spark.version}")
    all_ok = True
    if args.delta:
        all_ok = check_delta(spark) and all_ok
    spark.stop()
    print("KẾT QUẢ:", "OK" if all_ok else "LỖI")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
