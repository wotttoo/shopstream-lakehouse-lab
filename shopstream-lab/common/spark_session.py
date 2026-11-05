"""Hàm tạo SparkSession dùng chung cho mọi truy vấn và script kiểm tra."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from common import config


def get_spark(app_name="shopstream"):
    """SparkSession local: múi giờ UTC, 4 partition shuffle, bật Delta, có jar Kafka."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Múi giờ phiên làm việc UTC để event_time và cửa sổ không bị lệch giờ máy
        .config("spark.sql.session.timeZone", config.SPARK_TIMEZONE)
        # Dữ liệu nhỏ nên dùng ít partition shuffle (state store cũng ít file hơn)
        .config("spark.sql.shuffle.partitions", str(config.SPARK_SHUFFLE_PARTITIONS))
        # Hai cấu hình bắt buộc để dùng Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Tắt thanh tiến trình trên console cho log dễ đọc
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
    )
    # configure_spark_with_delta_pip thêm jar Delta khớp phiên bản delta-spark đã cài
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=[config.KAFKA_CONNECTOR_PACKAGE]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
