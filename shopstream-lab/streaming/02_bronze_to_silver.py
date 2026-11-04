"""Silver: đọc stream từ Bronze, parse, lọc lỗi, lọc trùng, MERGE chỉ chèn vào Delta.

Cách chạy:
    python streaming/02_bronze_to_silver.py
    python streaming/02_bronze_to_silver.py --continuous
"""

import argparse
import os
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import config  # noqa: E402
from common.progress import save_query_progress  # noqa: E402
from common.spark_session import get_spark  # noqa: E402
from common.transforms import SILVER_COLUMNS, dedup_within_batch, keep_valid, parse_events  # noqa: E402

QUERY_NAME = "silver"
# Biến môi trường chèn lỗi có chủ đích (mặc định tắt), dùng cho thí nghiệm lỗi
FAIL_AT_BATCH_ENV = "FAIL_AT_BATCH"


class InjectedFailure(RuntimeError):
    """Lỗi chèn có chủ đích: sink đã nhận dữ liệu nhưng Spark chưa ghi commits."""


def fail_if_requested(batch_id):
    """Ném lỗi nếu FAIL_AT_BATCH bằng batch_id hiện tại."""
    fail_at = os.environ.get(FAIL_AT_BATCH_ENV)
    if fail_at is not None and fail_at.strip() != "" and int(fail_at) == batch_id:
        raise InjectedFailure(
            f"FAIL_AT_BATCH={fail_at}: lỗi chèn sau khi đã ghi batch {batch_id}, trước khi foreachBatch kết thúc"
        )
OBSERVATION_NAME = "silver_parse"


def ensure_silver_table(spark, silver_path):
    """Tạo bảng Silver rỗng nếu chưa có, để MERGE luôn có bảng đích."""
    schema = (
        "event_id STRING, user_id STRING, event_type STRING, product_id STRING, "
        "amount BIGINT, event_time TIMESTAMP, kafka_partition INT, kafka_offset BIGINT, "
        "kafka_timestamp TIMESTAMP"
    )
    if not DeltaTable.isDeltaTable(spark, str(silver_path)):
        spark.createDataFrame([], schema).write.format("delta").save(str(silver_path))


def build_silver_stream(spark, bronze_path):
    parsed = parse_events(spark.readStream.format("delta").load(str(bronze_path)))
    # Đếm số dòng đầu vào và số dòng bị loại vì lỗi; kết quả nằm trong observedMetrics của tiến trình
    observed = parsed.observe(
        OBSERVATION_NAME,
        F.count(F.lit(1)).alias("input_rows"),
        F.coalesce(F.sum(F.when(F.col("is_valid"), 0).otherwise(1)), F.lit(0)).alias("rejected_rows"),
    )
    return (
        keep_valid(observed)
        # Watermark 30 phút: Silver phải giữ đủ mọi sự kiện duy nhất nên ngưỡng rộng hơn Gold
        .withWatermark("event_time", config.SILVER_WATERMARK)
        # Lọc trùng chỉ theo event_id, kể cả khi bản trùng có event_time lệch vài giây
        .dropDuplicatesWithinWatermark(["event_id"])
        .select(*SILVER_COLUMNS)
    )


def make_merge_batch(silver_path):
    """Tạo hàm foreachBatch: lọc trùng trong batch rồi MERGE chỉ chèn."""

    def merge_batch(batch_df, batch_id):
        spark = batch_df.sparkSession
        if batch_df.isEmpty():
            # Micro-batch không dữ liệu: Spark chạy để dọn state theo watermark mới
            print(f"[silver] batch {batch_id}: không có dữ liệu mới")
            return
        # Lọc trùng event_id bên trong batch trước khi MERGE
        source = dedup_within_batch(batch_df)
        target = DeltaTable.forPath(spark, str(silver_path))
        version_before = target.history(1).collect()[0]["version"]
        # MERGE chỉ chèn khi event_id chưa có: chạy lại cùng batch không sinh dòng thừa.
        # Không có nhánh UPDATE vì Gold đọc Silver dạng stream (bảng phải chỉ thêm dữ liệu).
        (
            target.alias("target")
            .merge(source.alias("source"), "target.event_id = source.event_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
        last_commit = target.history(1).collect()[0]
        if last_commit["version"] == version_before:
            # Không có dòng mới thì Delta không tạo phiên bản mới
            inserted = 0
        else:
            inserted = (last_commit["operationMetrics"] or {}).get("numTargetRowsInserted", "?")
        print(f"[silver] batch {batch_id}: chèn {inserted} dòng (phiên bản bảng {last_commit['version']})")
        # Điểm chèn lỗi: dữ liệu đã nằm trong Silver nhưng batch chưa được ghi vào commits/
        fail_if_requested(batch_id)

    return merge_batch


def make_append_batch(silver_path):
    """Biến thể KHÔNG idempotent, chỉ dùng cho thí nghiệm B: ghi append thẳng, không MERGE, không txnAppId.

    Nếu batch bị chạy lại sau lỗi, dữ liệu của batch đó được ghi thêm lần nữa.
    """

    def append_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            print(f"[silver] batch {batch_id}: không có dữ liệu mới")
            return
        source = dedup_within_batch(batch_df).persist()
        row_count = source.count()
        # Ghi append thường: Delta không biết batch này đã từng được ghi hay chưa
        source.write.format("delta").mode("append").save(str(silver_path))
        source.unpersist()
        print(f"[silver] batch {batch_id}: append {row_count} dòng (không idempotent)")
        fail_if_requested(batch_id)

    return append_batch


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Bronze -> Silver")
    parser.add_argument("--continuous", action="store_true", help="chạy liên tục thay vì availableNow")
    parser.add_argument("--progress-file", help="ghi tiến trình của lần chạy này ra file JSON")
    parser.add_argument("--lakehouse-dir", default=str(config.LAKEHOUSE_DIR),
                        help="thư mục gốc chứa các bảng Delta (thí nghiệm dùng lakehouse/exp_*)")
    parser.add_argument("--checkpoint-dir", default=str(config.CHECKPOINT_DIR),
                        help="thư mục gốc chứa checkpoint (thí nghiệm dùng checkpoints/exp_*)")
    parser.add_argument("--suffix", default="", help="hậu tố cho thư mục bảng Silver và checkpoint")
    parser.add_argument("--append-only", action="store_true",
                        help="biến thể không idempotent cho thí nghiệm B: append thay vì MERGE")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    spark = get_spark("02_bronze_to_silver")
    bronze_path = config.table_path(config.BRONZE_TABLE, lakehouse_dir=args.lakehouse_dir)
    silver_path = config.table_path(config.SILVER_TABLE, args.suffix, lakehouse_dir=args.lakehouse_dir)
    checkpoint = config.checkpoint_path(QUERY_NAME, args.suffix, checkpoint_dir=args.checkpoint_dir)
    ensure_silver_table(spark, silver_path)

    writer = (
        build_silver_stream(spark, bronze_path)
        .writeStream.queryName(QUERY_NAME)
        .foreachBatch(make_append_batch(silver_path) if args.append_only else make_merge_batch(silver_path))
        # Checkpoint riêng: lưu phiên bản Bronze đã đọc và state của dropDuplicatesWithinWatermark
        .option("checkpointLocation", str(checkpoint))
    )
    if args.continuous:
        writer = writer.trigger(processingTime="10 seconds")
    else:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    query.awaitTermination()
    for progress in query.recentProgress:
        observed = progress.get("observedMetrics", {}).get(OBSERVATION_NAME)
        if observed:
            print(f"[silver] batch {progress['batchId']}: đầu vào {observed['input_rows']} dòng, "
                  f"bị loại vì lỗi {observed['rejected_rows']} dòng")
    row_count = spark.read.format("delta").load(str(silver_path)).count()
    print(f"[silver] số dòng trong bảng: {row_count}")
    save_query_progress(query, args.progress_file, QUERY_NAME + args.suffix, row_count)
    spark.stop()


if __name__ == "__main__":
    main()
