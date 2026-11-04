"""Cấu hình dùng chung: đường dẫn, Kafka, watermark, cửa sổ, tham số sinh dữ liệu.

Mọi đường dẫn tính theo vị trí file này, nên chạy script ở thư mục nào cũng được.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Đường dẫn
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_DIR / "data"
EVENTS_FILE = DATA_DIR / "events.jsonl"
GOLDEN_ANSWER_FILE = DATA_DIR / "golden_answer.json"
GOLDEN_WINDOWS_FILE = DATA_DIR / "golden_windows.csv"

LAKEHOUSE_DIR = PROJECT_DIR / "lakehouse"
CHECKPOINT_DIR = PROJECT_DIR / "checkpoints"
RESULTS_DIR = PROJECT_DIR / "results"
PROGRESS_DIR = RESULTS_DIR / "progress"
EVIDENCE_DIR = RESULTS_DIR / "evidence"

# Tên bảng (cũng là tên thư mục con trong lakehouse/ và checkpoints/)
BRONZE_TABLE = "bronze"
SILVER_TABLE = "silver"
GOLD_TABLE = "gold"

BRONZE_PATH = LAKEHOUSE_DIR / BRONZE_TABLE
SILVER_PATH = LAKEHOUSE_DIR / SILVER_TABLE
GOLD_PATH = LAKEHOUSE_DIR / GOLD_TABLE

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "shop_events"
TOPIC_PARTITIONS = 3
TOPIC_REPLICATION_FACTOR = 1

# ---------------------------------------------------------------------------
# Watermark và cửa sổ
# ---------------------------------------------------------------------------
# Silver giữ đủ mọi sự kiện duy nhất nên ngưỡng rộng
SILVER_WATERMARK_MINUTES = 30
# Gold chấp nhận bỏ dữ liệu quá muộn: đây là quyết định nghiệp vụ
GOLD_WATERMARK_MINUTES = 10
# Ngưỡng dùng để so sánh ở bước 6
GOLD_COMPARE_WATERMARK_MINUTES = 20
GOLD_COMPARE_SUFFIX = "_wm20"
# Tumbling window
WINDOW_MINUTES = 5

SILVER_WATERMARK = f"{SILVER_WATERMARK_MINUTES} minutes"
GOLD_WATERMARK = f"{GOLD_WATERMARK_MINUTES} minutes"
GOLD_COMPARE_WATERMARK = f"{GOLD_COMPARE_WATERMARK_MINUTES} minutes"
WINDOW_DURATION = f"{WINDOW_MINUTES} minutes"

# ---------------------------------------------------------------------------
# Sinh dữ liệu giả lập
# ---------------------------------------------------------------------------
SEED = 42
DATA_START_TIME = "2026-01-15T10:00:00Z"
DATA_DURATION_MINUTES = 60
BASE_EVENT_COUNT = 2000
USER_COUNT = 50
PRODUCT_COUNT = 20

# Tỷ lệ loại sự kiện view / add_to_cart / purchase
EVENT_TYPE_WEIGHTS = {"view": 0.70, "add_to_cart": 0.20, "purchase": 0.10}
# Amount của purchase: bội của 1.000 đồng, từ 50.000 đến 500.000
AMOUNT_MIN = 50_000
AMOUNT_MAX = 500_000
AMOUNT_STEP = 1_000

# Bẫy trùng lặp
DUPLICATE_RATE = 0.10
# Trong số bản trùng, tỷ lệ bản có event_time lệch vài giây
DUPLICATE_SHIFTED_SHARE = 0.20
DUPLICATE_SHIFT_SECONDS = (1, 5)

# Bẫy đến muộn trong ngưỡng: 1 đến 4 phút
LATE_WITHIN_RATE = 0.15
LATE_WITHIN_MINUTES = (1, 4)
# Bẫy đến muộn quá ngưỡng: 12 đến 20 phút
LATE_BEYOND_RATE = 0.025
LATE_BEYOND_MINUTES = (12, 20)
LATE_BEYOND_MIN_PURCHASES = 15

# Chia đợt gửi
BATCH_COUNT = 12
BATCH_MINUTES = 5

# Sự kiện "đẩy watermark" ở đợt cuối
WATERMARK_PUSH_EVENT_TIME = "2026-01-15T11:30:00Z"

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
SPARK_SHUFFLE_PARTITIONS = 4
SPARK_TIMEZONE = "UTC"
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"


def table_path(table_name, suffix="", lakehouse_dir=LAKEHOUSE_DIR):
    """Đường dẫn bảng Delta, ví dụ table_path("gold", "_wm20") -> lakehouse/gold_wm20."""
    return Path(lakehouse_dir) / f"{table_name}{suffix}"


def checkpoint_path(query_name, suffix="", checkpoint_dir=CHECKPOINT_DIR):
    """Checkpoint riêng cho mỗi truy vấn, ví dụ checkpoints/silver."""
    return Path(checkpoint_dir) / f"{query_name}{suffix}"
