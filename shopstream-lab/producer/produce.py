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


# ---------------------------------------------------------------------------
# Quản lý topic
# ---------------------------------------------------------------------------
def create_topic(topic=config.TOPIC_NAME, partitions=config.TOPIC_PARTITIONS,
                 bootstrap=config.KAFKA_BOOTSTRAP_SERVERS, timeout_seconds=60):
    """Tạo topic. Nếu topic vừa bị xóa và Kafka chưa dọn xong thì thử lại."""
    import time

    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    deadline = time.time() + timeout_seconds
    try:
        while True:
            try:
                admin.create_topics([NewTopic(topic, num_partitions=partitions,
                                              replication_factor=config.TOPIC_REPLICATION_FACTOR)])
                print(f"Đã tạo topic {topic} với {partitions} partition")
                return True
            except TopicAlreadyExistsError:
                # Topic vừa bị xóa nhưng Kafka chưa dọn xong thì không có trong danh sách
                if topic in admin.list_topics() or time.time() > deadline:
                    print(f"Topic {topic} đã tồn tại")
                    return False
                time.sleep(1)
    finally:
        admin.close()


def delete_topic(topic=config.TOPIC_NAME, bootstrap=config.KAFKA_BOOTSTRAP_SERVERS,
                 timeout_seconds=60):
    """Xóa topic để chạy lại từ đầu; chờ đến khi topic biến mất khỏi danh sách."""
    import time

    from kafka.admin import KafkaAdminClient
    from kafka.errors import UnknownTopicOrPartitionError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        try:
            admin.delete_topics([topic])
        except UnknownTopicOrPartitionError:
            print(f"Topic {topic} không tồn tại, bỏ qua")
            return False
        deadline = time.time() + timeout_seconds
        while topic in admin.list_topics() and time.time() < deadline:
            time.sleep(0.5)
        print(f"Đã xóa topic {topic}")
        return True
    finally:
        admin.close()


def recreate_topic(topic=config.TOPIC_NAME):
    """Xóa (nếu có) rồi tạo lại topic rỗng."""
    delete_topic(topic)
    create_topic(topic)


# ---------------------------------------------------------------------------
# Gửi
# ---------------------------------------------------------------------------
def send_lines(lines, topic=config.TOPIC_NAME, bootstrap=config.KAFKA_BOOTSTRAP_SERVERS):
    """Gửi các dòng theo đúng thứ tự trong file, chờ broker xác nhận hết."""
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        # Một request mỗi lần để giữ đúng thứ tự gửi trong từng partition
        max_in_flight_requests_per_connection=1,
        retries=5,
    )
    for line in lines:
        key, value = to_kafka_message(line)
        producer.send(topic, key=key, value=value)
    producer.flush()
    producer.close()
    return len(lines)


def count_messages(topic=config.TOPIC_NAME, bootstrap=config.KAFKA_BOOTSTRAP_SERVERS):
    """Tổng số message trong topic = tổng (offset cuối - offset đầu) của mọi partition."""
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=bootstrap)
    partitions = consumer.partitions_for_topic(topic) or set()
    topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    beginning = consumer.beginning_offsets(topic_partitions)
    end = consumer.end_offsets(topic_partitions)
    consumer.close()
    per_partition = {tp.partition: end[tp] - beginning[tp] for tp in topic_partitions}
    return sum(per_partition.values()), per_partition


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Gửi sự kiện ShopStream vào Kafka")
    parser.add_argument("--create-topic", action="store_true", help="tạo topic 3 partition")
    parser.add_argument("--delete-topic", action="store_true", help="xóa topic")
    parser.add_argument("--recreate-topic", action="store_true", help="xóa rồi tạo lại topic")
    parser.add_argument("--batch", type=int, help="gửi một đợt (1 đến 12)")
    parser.add_argument("--all", action="store_true", help="gửi toàn bộ file")
    parser.add_argument("--count", action="store_true", help="in tổng số message trong topic")
    parser.add_argument("--topic", default=config.TOPIC_NAME)
    parser.add_argument("--events-file", default=str(config.EVENTS_FILE))
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.delete_topic or args.recreate_topic:
        delete_topic(args.topic)
    if args.create_topic or args.recreate_topic:
        create_topic(args.topic)
    if args.batch is not None or args.all:
        lines = load_events(args.events_file)
        selected = select_batch(lines, None if args.all else args.batch)
        if not selected:
            print(f"Không có bản ghi nào cho đợt {args.batch}")
            sys.exit(1)
        sent_count = send_lines(selected, args.topic)
        label = "toàn bộ" if args.all else f"đợt {args.batch}"
        print(f"Đã gửi {sent_count} bản ghi ({label}) vào topic {args.topic}")
    if args.count:
        total, per_partition = count_messages(args.topic)
        print(f"Theo partition: {per_partition}", file=sys.stderr)
        # Chỉ in con số ra stdout để dễ so sánh bằng shell
        print(total)


if __name__ == "__main__":
    main()
