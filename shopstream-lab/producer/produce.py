"""Producer gửi sự kiện từ data/events.jsonl vào Kafka theo đợt.

Producer chỉ gửi đúng những gì có trong file: không tự sinh event_id hay thời gian.
Hai trường batch_no và send_time chỉ dùng để chọn đợt, KHÔNG đưa vào value.

Cách chạy:
    python producer/produce.py --create-topic
    python producer/produce.py --batch 3
    python producer/produce.py --all
    python producer/produce.py --count
    python producer/produce.py --delete-topic
"""

import argparse
import json
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from common import config  # noqa: E402
from common.schema import EVENT_FIELDS  # noqa: E402

# Hai trường chỉ dùng cho producer và kiểm tra, không gửi lên Kafka
PRODUCER_ONLY_FIELDS = ("batch_no", "send_time")


def load_events(events_file=config.EVENTS_FILE):
    """Đọc events.jsonl, giữ nguyên thứ tự gửi."""
    with open(events_file, encoding="utf-8") as file:
        return [json.loads(line) for line in file if line.strip()]


def select_batch(lines, batch_no=None):
    """Lọc các dòng của một đợt; batch_no=None nghĩa là lấy tất cả."""
    if batch_no is None:
        return list(lines)
    return [line for line in lines if line["batch_no"] == batch_no]


def to_kafka_message(line):
    """Trả về (key, value) dạng bytes. Key là user_id, value là JSON sáu trường sự kiện."""
    event = {field: line[field] for field in EVENT_FIELDS}
    key = line["user_id"].encode("utf-8")
    value = json.dumps(event, ensure_ascii=False).encode("utf-8")
    return key, value
