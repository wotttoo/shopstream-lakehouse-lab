"""Gold: doanh thu và số đơn theo tumbling window 5 phút, output mode append.

Cách chạy:
    python streaming/03_silver_to_gold.py
    python streaming/03_silver_to_gold.py --watermark "20 minutes" --suffix _wm20
    python streaming/03_silver_to_gold.py --continuous
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

QUERY_NAME = "gold"


def build_gold_stream(spark, silver_path, watermark):
    silver_stream = spark.readStream.format("delta").load(str(silver_path))
    is_purchase = F.col("event_type") == "purchase"
    return (
        silver_stream
        # Watermark tính trên MỌI loại sự kiện (kể cả view đẩy watermark), nhờ đó các cửa sổ cuối được chốt.
        # Lưu ý: KHÔNG dùng .filter(purchase) ở đây. Optimizer của Spark đẩy filter không liên quan
        # event_time xuống DƯỚI withWatermark, khi đó watermark chỉ còn tính trên purchase.
        .withWatermark("event_time", watermark)
        # Tumbling window 5 phút theo event time
        .groupBy(F.window("event_time", config.WINDOW_DURATION))
        # Chỉ cộng purchase: điều kiện nằm trong hàm tổng hợp
        .agg(
            F.sum(F.when(is_purchase, F.col("amount"))).alias("revenue"),
            F.count(F.when(is_purchase, F.lit(1))).alias("orders"),
        )
        # Bỏ cửa sổ không có đơn nào (chỉ có view, add_to_cart)
        .filter(F.col("orders") > 0)
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "revenue",
            "orders",
        )
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Silver -> Gold")
    parser.add_argument("--continuous", action="store_true", help="chạy liên tục thay vì availableNow")
    parser.add_argument("--progress-file", help="ghi tiến trình của lần chạy này ra file JSON")
    parser.add_argument("--watermark", default=config.GOLD_WATERMARK, help='ví dụ "10 minutes"')
    parser.add_argument("--suffix", default="", help="hậu tố cho thư mục bảng Gold và checkpoint")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    spark = get_spark("03_silver_to_gold")
    silver_path = config.table_path(config.SILVER_TABLE)
    gold_path = config.table_path(config.GOLD_TABLE, args.suffix)
    checkpoint = config.checkpoint_path(QUERY_NAME, args.suffix)

    writer = (
        build_gold_stream(spark, silver_path, args.watermark)
        .writeStream.format("delta")
        .queryName(QUERY_NAME + args.suffix)
        # Append: một cửa sổ chỉ được ghi một lần, khi watermark đã vượt qua window_end
        .outputMode("append")
        # Checkpoint riêng: lưu phiên bản Silver đã đọc, watermark và state của các cửa sổ đang mở
        .option("checkpointLocation", str(checkpoint))
    )
    if args.continuous:
        writer = writer.trigger(processingTime="10 seconds")
    else:
        writer = writer.trigger(availableNow=True)

    query = writer.start(str(gold_path))
    query.awaitTermination()
    for progress in query.recentProgress:
        state = (progress.get("stateOperators") or [{}])[0]
        print(f"[gold{args.suffix}] batch {progress['batchId']}: đầu vào {progress['numInputRows']} dòng, "
              f"watermark {progress.get('eventTime', {}).get('watermark')}, "
              f"bị loại vì quá muộn {state.get('numRowsDroppedByWatermark')}")
    row_count = spark.read.format("delta").load(str(gold_path)).count()
    print(f"[gold{args.suffix}] số cửa sổ trong bảng: {row_count}")
    save_query_progress(query, args.progress_file, QUERY_NAME + args.suffix, row_count)
    spark.stop()


if __name__ == "__main__":
    main()
