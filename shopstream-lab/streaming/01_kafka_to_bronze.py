"""Bronze: đọc Kafka, giữ nguyên value dạng chuỗi, ghi append vào Delta.

Không parse, không lọc: bản trùng vẫn còn nguyên ở Bronze.

Cách chạy:
    python streaming/01_kafka_to_bronze.py               # availableNow: xử lý hết rồi dừng
    python streaming/01_kafka_to_bronze.py --continuous  # chạy liên tục (demo)
"""

import argparse
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.progress import save_query_progress  # noqa: E402
from common.spark_session import get_spark  # noqa: E402

QUERY_NAME = "bronze"


def build_bronze_stream(spark, topic=config.TOPIC_NAME):
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        # Chỉ có tác dụng ở lần chạy đầu của checkpoint; sau đó Spark đọc tiếp từ offsets đã lưu
        .option("startingOffsets", "earliest")
        .load()
    )
    return kafka_stream.select(
        F.col("key").cast("string").alias("kafka_key"),
        # Giữ nguyên value dạng chuỗi JSON, chưa parse
        F.col("value").cast("string").alias("value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        # Thời điểm nạp vào Bronze (chỉ là thông tin theo dõi, không dùng trong logic)
        F.current_timestamp().alias("ingest_time"),
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Kafka -> Bronze")
    parser.add_argument("--continuous", action="store_true", help="chạy liên tục thay vì availableNow")
    parser.add_argument("--progress-file", help="ghi tiến trình của lần chạy này ra file JSON")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    spark = get_spark("01_kafka_to_bronze")
    bronze_path = config.table_path(config.BRONZE_TABLE)
    checkpoint = config.checkpoint_path(QUERY_NAME)

    writer = (
        build_bronze_stream(spark)
        .writeStream.format("delta")
        .queryName(QUERY_NAME)
        .outputMode("append")
        # Checkpoint riêng của truy vấn Bronze: lưu offsets Kafka đã đọc và đã commit
        .option("checkpointLocation", str(checkpoint))
    )
    if args.continuous:
        writer = writer.trigger(processingTime="10 seconds")
    else:
        # availableNow: xử lý hết dữ liệu hiện có rồi dừng
        writer = writer.trigger(availableNow=True)

    query = writer.start(str(bronze_path))
    query.awaitTermination()
    row_count = spark.read.format("delta").load(str(bronze_path)).count()
    print(f"[bronze] số dòng trong bảng: {row_count}")
    save_query_progress(query, args.progress_file, QUERY_NAME, row_count)
    spark.stop()


if __name__ == "__main__":
    main()
