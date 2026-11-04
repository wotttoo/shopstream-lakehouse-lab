# ShopStream Lab: Stream Processing vào Lakehouse

Bài thực hành mô phỏng sàn thương mại điện tử ShopStream: sự kiện `view`, `add_to_cart`, `purchase` đi qua Kafka, được Spark Structured Streaming xử lý và ghi vào Delta Lake theo kiến trúc Medallion (Bronze, Silver, Gold). Bài minh họa bằng số liệu thật ba vấn đề: dữ liệu trùng, dữ liệu đến muộn, hệ thống lỗi giữa chừng.

Kết quả của lần chạy thật nằm ở `results/run_report.md`.

## Bộ phiên bản đã chạy thật

| Thành phần | Phiên bản |
|---|---|
| Hệ điều hành | Ubuntu 24.04 x86_64 |
| Java | OpenJDK 17.0.20.1 |
| Python | 3.11.18 |
| Apache Spark, PySpark | 4.1.0 |
| Delta Lake | `delta-spark==4.1.0` (jar `io.delta:delta-spark_4.1_2.13:4.1.0`) |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0` |
| Kafka | Docker image `apache/kafka:3.9.1`, một broker, KRaft |
| kafka-python | 2.2.15 |
| pandas, numpy, pytest | 2.3.3, 2.3.5, 8.4.2 |

Bộ phiên bản chính (Spark 4.1.0, delta-spark 4.1.0) chạy được, không phải lùi về Spark 4.0.1.

## 1. Cài đặt (Ubuntu 24.04)

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk-headless docker.io docker-compose-v2 git curl ca-certificates
sudo usermod -aG docker "$USER"     # đăng xuất rồi đăng nhập lại để có hiệu lực
curl -LsSf https://astral.sh/uv/install.sh | sh
source "$HOME/.local/bin/env"
```

Tạo môi trường Python, chạy tại thư mục `shopstream-lab/`:

```bash
uv venv --python 3.11 .venv
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> .venv/bin/activate
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> .venv/bin/activate
echo "export PYTHONPATH=\"$PWD\"" >> .venv/bin/activate
source .venv/bin/activate
uv pip install -r requirements.txt
```

Kiểm tra:

```bash
java -version                       # phải là 17
python --version                    # phải là 3.11
python -c "import pyspark; print(pyspark.__version__)"
docker compose up -d
docker compose ps                   # broker phải ở trạng thái running
python verify/smoke_env.py --delta --kafka
```

Lần chạy Spark đầu tiên cần mạng để tải jar Delta và Kafka connector từ Maven (lưu ở `~/.ivy2.5.2`).

## 2. Chạy toàn bộ từ đầu

Mọi lệnh chạy tại `shopstream-lab/` sau khi `source .venv/bin/activate`.

```bash
docker compose up -d                                  # khởi động Kafka
python data/generate_events.py --summary              # sinh events.jsonl và đáp án vàng (seed 42)
python producer/produce.py --create-topic             # tạo topic shop_events, 3 partition
python streaming/run_all_batches.py --reset --compare-wm20   # 12 đợt qua Bronze, Silver, Gold (+ Gold 20 phút)
python verify/check_results.py --watermark-suffix _wm20      # đối chiếu với đáp án vàng
python verify/inspect_progress.py                     # tiến trình truy vấn, lịch sử Delta, time travel
```

`--reset` xóa `lakehouse/`, `checkpoints/` (trừ thư mục `exp_*` của thí nghiệm) và tạo lại topic, nên lệnh chạy lại được nhiều lần. Một lần chạy đủ 12 đợt mất khoảng 15 đến 20 phút vì mỗi truy vấn khởi động một tiến trình Spark riêng.

Chạy từng bước bằng tay (giống những gì `run_all_batches.py` làm cho một đợt):

```bash
python producer/produce.py --batch 1
python streaming/01_kafka_to_bronze.py
python streaming/02_bronze_to_silver.py
python streaming/03_silver_to_gold.py
python streaming/03_silver_to_gold.py --watermark "20 minutes" --suffix _wm20
```

Mỗi script có `--continuous` để chạy liên tục (dùng cho demo, không dùng cho kiểm tra).

Lệnh khác của producer: `--all` gửi toàn bộ file, `--count` in tổng số message trong topic, `--delete-topic`, `--recreate-topic`.

## 3. Thí nghiệm lỗi

Ba thí nghiệm dùng thư mục riêng `lakehouse/exp_*`, `checkpoints/exp_*` và tạo lại topic khi bắt đầu, nên không chạm vào bảng của lần chạy chuẩn. Vì dùng chung topic, hãy chạy lần lượt từng thí nghiệm. Sau khi chạy thí nghiệm, topic chỉ còn dữ liệu của thí nghiệm; muốn chạy lại luồng chuẩn thì dùng `run_all_batches.py --reset`.

```bash
python experiments/exp_a_rerun_idempotent.py   # Silver lỗi giữa chừng, chạy lại cùng checkpoint, MERGE
python experiments/exp_b_non_idempotent.py     # như A nhưng Silver ghi append (không idempotent)
python experiments/exp_c_delete_checkpoint.py  # xóa checkpoint Bronze rồi chạy lại
```

Lỗi được chèn bằng biến môi trường `FAIL_AT_BATCH=k`: foreachBatch của Silver ném ngoại lệ ngay sau khi đã ghi xong batch `k`, trước khi hàm kết thúc. Kết quả ghi ở `results/exp_*.json`, bằng chứng checkpoint và `_delta_log` ở `results/evidence/`.

## 4. Báo cáo

```bash
python verify/check_results.py --watermark-suffix _wm20   # lưu results/check_results.json (bản đầy đủ)
python verify/build_report.py        # tổng hợp results/ thành run_report.json và run_report.md
python verify/check_report.py --markdown
```

`check_results.py` chỉ lưu `results/check_results.json` khi kiểm tra đầy đủ (mọi tầng, đủ 12 đợt, có `--watermark-suffix`), nên chạy kiểm tra một tầng không ghi đè dữ liệu của báo cáo.

| File trong `results/` | Nội dung |
|---|---|
| `run_report.md`, `run_report.json` | báo cáo tổng hợp: phiên bản, bảng 4, 9, 10, phần III.4, mẫu checkpoint, bất thường |
| `progress/batch_XX.json` | tiến trình (`recentProgress`) của mọi micro-batch và số dòng ba tầng sau từng đợt |
| `check_results.json` | bảng đối chiếu đầy đủ với đáp án vàng |
| `progress_summary.json`, `delta_history.json` | tóm tắt tiến trình, `DESCRIBE HISTORY` và time travel |
| `exp_a.json`, `exp_b.json`, `exp_c.json` | kết quả ba thí nghiệm lỗi |
| `evidence/exp_*_before.txt`, `evidence/exp_*_after.txt` | nội dung `offsets/`, `commits/`, `_delta_log/` trước và sau khi chạy lại |

## 5. Test

```bash
pytest tests/                        # unit test, không cần Kafka hay Spark đang chạy
python verify/check_results.py       # kiểm tra đầu cuối, thoát với mã khác 0 nếu có dòng sai
```

`tests/test_transforms.py` là ngoại lệ duy nhất: dùng Spark local (không cần Kafka hay mạng).

## 6. Cấu trúc thư mục

| Thư mục | Nội dung |
|---|---|
| `common/` | cấu hình, SparkSession dùng chung, schema, hàm biến đổi của Silver, lưu tiến trình |
| `data/` | bộ sinh dữ liệu và đầu ra: `events.jsonl`, `golden_answer.json`, `golden_windows.csv` |
| `producer/` | gửi sự kiện vào Kafka theo đợt |
| `streaming/` | ba truy vấn Bronze, Silver, Gold và vòng lặp chạy 12 đợt |
| `experiments/` | ba thí nghiệm lỗi và tiện ích dùng chung |
| `verify/` | kiểm tra môi trường, đối chiếu kết quả, xem tiến trình, dựng báo cáo |
| `results/` | số liệu thật của các lần chạy |
| `lakehouse/`, `checkpoints/` | bảng Delta và checkpoint (không commit lên git) |

## 7. Khắc phục lỗi thường gặp

**`docker compose up -d` báo không kết nối được Docker daemon.** Daemon chưa chạy: `sudo systemctl start docker`. Trong container không có systemd thì chạy `sudo dockerd &`.

**Spark báo lỗi Java hoặc `java -version` không phải 17.** Kiểm tra đã `source .venv/bin/activate` (file này đặt `JAVA_HOME` cho Java 17). Nếu máy chỉ có Java 21, cài thêm `openjdk-17-jdk-headless`; Java mặc định của máy không cần đổi.

**Lần chạy đầu treo hoặc lỗi khi tải jar từ Maven.** Cần mạng ra Maven Central. Sau lần tải đầu, jar nằm trong `~/.ivy2.5.2/jars` và không cần mạng nữa.

**`ModuleNotFoundError: No module named 'common'`.** Chưa `source .venv/bin/activate` (file này đặt `PYTHONPATH`). Các script trong `streaming/`, `producer/`, `data/`, `experiments/` tự thêm thư mục project vào `sys.path`, nên lỗi này thường chỉ gặp khi chạy `python -c` hoặc pytest ở thư mục khác.

**`TopicAlreadyExistsError` ngay sau khi xóa topic.** Kafka xóa topic bất đồng bộ. `produce.py --create-topic` tự chờ và thử lại đến 60 giây.

**Gold không có cửa sổ nào sau vài đợt đầu.** Đây là hành vi đúng của output mode append: một cửa sổ chỉ được ghi khi watermark (event time lớn nhất đã thấy trừ 10 phút) vượt qua `window_end`. Cửa sổ 10:00 được chốt ở đợt 4.

**Các cửa sổ cuối không bao giờ được chốt, hoặc watermark Gold thấp hơn mong đợi.** Đừng viết `.withWatermark(...).filter(event_type == 'purchase')`: optimizer của Spark đẩy filter đó xuống DƯỚI watermark, nên watermark chỉ tính trên purchase và sự kiện "đẩy watermark" (một view) mất tác dụng. `03_silver_to_gold.py` đặt điều kiện purchase vào trong hàm tổng hợp để tránh việc này (xem `results/run_report.md`, mục Bất thường).

**`numRowsDroppedByWatermark` của Gold nhỏ hơn số sự kiện đến muộn.** Metric này đếm dòng ở đầu vào toán tử có trạng thái, tức sau partial aggregation và shuffle: mỗi cửa sổ quá muộn trong một micro-batch chỉ tính là một dòng. Số purchase thật sự bị loại đo bằng cách so bảng Gold với đáp án tuyệt đối.

**Log Silver có thêm micro-batch "không có dữ liệu mới".** Với watermark, `availableNow` chạy thêm một micro-batch rỗng sau batch có dữ liệu để dọn state và chốt cửa sổ theo watermark mới. Vì vậy mỗi đợt dùng hai batchId (ví dụ đợt 1 là batch 0 và 1).

**Chạy lại luồng chuẩn sau thí nghiệm, số dòng Bronze sai.** Thí nghiệm tạo lại topic với dữ liệu của chính nó. Luôn dùng `run_all_batches.py --reset` khi chạy lại luồng chuẩn.

**Spark 4 báo lỗi khi parse `event_time` sai định dạng.** Spark 4 bật ANSI mặc định nên `to_timestamp` ném lỗi với chuỗi sai. `common/transforms.py` dùng `try_to_timestamp` để chuỗi sai thành null rồi bị lọc như bản ghi lỗi.
