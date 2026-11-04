# CLAUDE.md: Bài thực hành "Stream Processing vào Lakehouse/Warehouse"

Mục 1 đến 7 là hướng dẫn làm việc chung. Mục 8 đến 15 là đặc tả chi tiết của bài. Hãy đọc toàn bộ trước khi làm.

## 1. Tổng quan

ShopStream là bài thực hành (phần VI của reading) trong chương trình AIVN Self-Learning Data Engineering, mô phỏng một sàn thương mại điện tử: sự kiện view, add_to_cart, purchase đi qua Kafka, được Spark Structured Streaming xử lý và ghi vào Delta Lake theo kiến trúc Medallion (Bronze, Silver, Gold). Bài giải quyết ba vấn đề: dữ liệu trùng, dữ liệu đến muộn, hệ thống lỗi giữa chừng. Nhiệm vụ là tạo code và data chạy được, tự đứng độc lập (tạo dữ liệu từ đầu, không dùng lại dữ liệu hay code của bài khác), rồi ghi lại số liệu thật cho reading (phần I đến V đã xong), quiz và solution.

## 2. Tech stack và phiên bản

| Thành phần | Phiên bản |
|---|---|
| Java | 17 |
| Python | 3.11 |
| Apache Spark, PySpark | 4.1.0 |
| Delta Lake | Python: `delta-spark==4.1.0`; Maven: `io.delta:delta-spark_4.1_2.13:4.1.0` |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0` |
| Kafka | Docker image `apache/kafka:3.9.1`, một broker, chế độ KRaft, không dùng Zookeeper |
| Thư viện Python khác | kafka-python hoặc confluent-kafka, pandas, numpy, pytest |

- Nếu phiên bản nào không tương thích khi chạy thật, thử Spark 4.0.1 với delta-spark 4.0.x, và ghi rõ bộ phiên bản cuối cùng chạy được vào `requirements.txt` và `README.md`.
- Spark chạy local, chạy ngoài Docker. Cấu hình Kafka advertised listeners phải để Spark kết nối được qua localhost.
- SparkSession dùng chung: múi giờ phiên làm việc UTC, số partition shuffle nhỏ (4), bật hai cấu hình Delta (extension và catalog).

## 3. Cài môi trường từ đầu

Áp dụng cho Ubuntu 24.04 x86_64. Python 3.11 không có trong apt của Ubuntu 24.04 nên dùng `uv` để cài. Trên máy khác kiến trúc, đổi đường dẫn `JAVA_HOME` cho đúng.

Cài công cụ hệ thống:

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk-headless docker.io docker-compose-v2 git curl ca-certificates
sudo usermod -aG docker "$USER"     # đăng xuất rồi đăng nhập lại để có hiệu lực
curl -LsSf https://astral.sh/uv/install.sh | sh
source "$HOME/.local/bin/env"
```

Tạo môi trường Python (chạy tại `shopstream-lab/`):

```bash
uv venv --python 3.11 .venv
# Dùng Java 17 riêng cho project, không đổi Java mặc định của máy
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> .venv/bin/activate
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> .venv/bin/activate
# Để các script import được common/
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
```

Lần chạy Spark đầu tiên cần mạng để tải jar Delta và Kafka connector từ Maven.

## 4. Cách chạy và cách test

Mọi lệnh chạy tại `shopstream-lab/`, sau khi `source .venv/bin/activate`.

Chạy toàn bộ từ đầu:

```bash
docker compose up -d                       # khởi động Kafka
python data/generate_events.py             # sinh events.jsonl và đáp án vàng
python producer/produce.py --create-topic  # tạo topic shop_events
python streaming/run_all_batches.py        # chạy 12 đợt qua Bronze, Silver, Gold
python verify/check_results.py             # đối chiếu với đáp án vàng
python verify/inspect_progress.py          # xem tiến trình truy vấn, lịch sử Delta
```

So sánh ngưỡng và chạy thí nghiệm lỗi:

```bash
python streaming/run_all_batches.py --compare-wm20   # chạy thêm Gold 20 phút ở MỖI đợt, không chạy một lần sau cùng
python experiments/exp_a_rerun_idempotent.py
python experiments/exp_b_non_idempotent.py
python experiments/exp_c_delete_checkpoint.py
```

Test có hai tầng, đều phải pass trước khi commit theo phạm vi ở mục 7:

```bash
pytest tests/                       # unit test, không cần Kafka hay Spark đang chạy
python verify/check_results.py      # kiểm tra đầu cuối, thoát với mã khác 0 nếu có dòng sai
```

## 5. Cấu trúc thư mục

`CLAUDE.md` và `PLAN.md` nằm ở thư mục gốc, cũng là gốc của git repo. Toàn bộ code nằm trong `shopstream-lab/`.

```
./
  CLAUDE.md                    # hướng dẫn cho Claude Code (file này)
  PLAN.md                      # danh sách task, tiến độ và mục "Vướng mắc"
  shopstream-lab/
    README.md                  # hướng dẫn cài đặt và chạy, kèm mục khắc phục lỗi thường gặp
    requirements.txt
    docker-compose.yml         # Kafka một broker
    common/
      config.py                # đường dẫn, tên topic, ngưỡng watermark, kích thước cửa sổ
      spark_session.py         # hàm tạo SparkSession dùng chung
      schema.py                # schema JSON của sự kiện
      transforms.py            # hàm parse và lọc trùng dùng cho Silver
    data/
      generate_events.py       # sinh dữ liệu giả lập, seed cố định
      events.jsonl             # đầu ra: sự kiện theo đúng thứ tự gửi, có số đợt
      golden_answer.json       # đáp án vàng (thống kê tổng)
      golden_windows.csv       # doanh thu và số đơn đúng của từng cửa sổ 5 phút
    producer/
      produce.py               # gửi sự kiện vào Kafka theo đợt
    streaming/
      01_kafka_to_bronze.py
      02_bronze_to_silver.py
      03_silver_to_gold.py
      run_all_batches.py       # chạy tuần tự: gửi một đợt, chạy ba truy vấn availableNow, lặp lại
    experiments/
      exp_a_rerun_idempotent.py
      exp_b_non_idempotent.py
      exp_c_delete_checkpoint.py
      lab_utils.py             # dọn trạng thái thí nghiệm, chụp checkpoint, ghi kết quả
    verify/
      check_results.py         # đối chiếu Bronze, Silver, Gold với đáp án vàng
      inspect_progress.py      # in thông tin tiến trình truy vấn
      smoke_env.py             # kiểm tra Spark, Delta, Kafka kết nối được với nhau
      build_report.py          # tổng hợp results/ thành run_report
      check_report.py          # kiểm tra run_report đủ nội dung
    tests/                     # unit test chạy bằng pytest
    lakehouse/                 # bảng Delta (không commit lên git)
      bronze/ silver/ gold/
    checkpoints/               # checkpoint riêng mỗi truy vấn (không commit lên git)
    results/
      run_report.json          # số liệu thật từ các lần chạy
      run_report.md            # bản đọc được, dùng để viết reading
    .gitignore
```

## 6. Quy ước code

Style:
- Theo PEP 8. Code viết cho người mới học: ưu tiên cách đơn giản và dễ đọc hơn cách tối ưu.
- Mỗi file một việc.
- Chú thích tiếng Việt ngắn gọn ở các dòng cốt lõi (watermark, dedup, window, MERGE, checkpoint).
- Mọi đường dẫn, tên topic, tham số đặt trong `common/config.py`, không rải rác.

Đặt tên:
- Tên biến rõ nghĩa, ví dụ `late_event_count`, không dùng `cnt` hay `df2`.
- Biến và hàm dùng `snake_case`, hằng số trong config dùng `UPPER_SNAKE_CASE`.
- Giữ nguyên tên bảng, topic và cột đã đặc tả: `shop_events`, `bronze`, `silver`, `gold`, `revenue`, `orders`, `window_start`, `window_end`.

Viết test:
- Đặt trong `tests/test_<tên_module>.py`, tên hàm `test_<hành_vi_cần_kiểm_tra>`.
- Unit test không phụ thuộc Kafka, Spark hay mạng, và ghi file tạm qua `tmp_path` của pytest.
- Tối thiểu phải kiểm tra: sinh dữ liệu hai lần với seed 42 cho kết quả giống hệt nhau, các tỷ lệ bẫy nằm trong mục tiêu, hàm mô phỏng watermark đúng với một ví dụ nhỏ tính tay, value gửi Kafka không chứa `batch_no` và `send_time`.
- Kỳ vọng trong test lấy từ đặc tả hoặc ví dụ tính tay, không chép từ kết quả vừa chạy ra.

## 7. Quy tắc làm việc

Commit:
- Mỗi task xong phải chạy test pass mới commit. `pytest tests/` luôn phải pass; task chạm vào luồng dữ liệu (`data/`, `producer/`, `streaming/`) còn phải pass `verify/check_results.py`, từ bước 5 trở đi.
- Commit message rõ ràng, một dòng tóm tắt việc đã làm theo dạng `<loại>: <mô tả>`, ví dụ `feat: thêm truy vấn Bronze đọc từ Kafka`. Loại gồm `feat`, `fix`, `test`, `docs`, `chore`.
- Mỗi commit một task. Không commit `lakehouse/`, `checkpoints/`, `.venv/`.

Dữ liệu và bí mật:
- Không xóa dữ liệu. Ngoại lệ duy nhất là các thao tác dọn trạng thái đã đặc tả cho thí nghiệm và chạy lại từ đầu, chỉ áp dụng cho `lakehouse/`, `checkpoints/` và topic Kafka, vì chúng tái tạo được. Không bao giờ xóa `data/` hay `results/`.
- Không hardcode API key hay secret, không commit hay in ra bất kỳ khóa bí mật nào (bài này không cần khóa nào).

Số liệu:
- Mọi con số trong báo cáo phải lấy từ lần chạy thật. Không bịa số, không ước lượng.
- Chạy và kiểm tra từng bước trước khi sang bước sau. Không sửa kỳ vọng cho khớp với kết quả.

Vướng mắc:
- Gặp vấn đề không tự giải quyết được (kể cả một bước không đạt kỳ vọng, hoặc thiếu Docker, Java) thì ghi vào mục "Vướng mắc" trong `PLAN.md` rồi chuyển sang task khác. Ghi rõ: hiện tượng, đã thử gì, kết quả, cách cài hoặc hướng xử lý đề xuất.
- Chỉ chuyển sang task không phụ thuộc vào task đang vướng. Không tự đổi thiết kế để né vấn đề.
- Điểm dừng ở bước 3 (mục 14) vẫn giữ nguyên: phải chờ người dùng xem thống kê trước khi sang bước 4.

## 8. Thiết kế dữ liệu giả lập (`data/generate_events.py`)

Dữ liệu phải được "cài bẫy" có chủ đích và tái lập được. Dùng seed cố định 42.

Sự kiện gốc:
- Khoảng 2.000 sự kiện duy nhất, event time trải đều trong 60 phút, bắt đầu 10:00:00 UTC của một ngày cố định (ví dụ 2026-01-15), kết thúc trước 11:00:00.
- Khoảng 50 user (U001...), 20 sản phẩm (P01...).
- Ba loại event_type: view, add_to_cart, purchase, tỷ lệ khoảng 70/20/10. Chỉ purchase có amount (đơn vị đồng, bội của 1.000, từ 50.000 đến 500.000). Các loại khác để amount là null.
- Trường của mỗi sự kiện: event_id (UUID sinh theo seed, không dùng uuid4 ngẫu nhiên không seed), user_id, event_type, product_id, amount, event_time (chuỗi ISO 8601 UTC).

Ba loại "bẫy" (tỷ lệ là mục tiêu, ghi số thực tế vào đáp án vàng):
- Trùng lặp: khoảng 10% sự kiện được gửi thêm một bản. Phần lớn bản trùng giống hệt bản gốc; khoảng 1/5 số bản trùng có event_time lệch vài giây so với bản gốc (event_id vẫn giữ nguyên). Mục đích: cho thấy sự khác nhau giữa dropDuplicates có watermark và dropDuplicatesWithinWatermark.
- Đến muộn trong ngưỡng: khoảng 15% sự kiện được gửi muộn 1 đến 4 phút so với event time.
- Đến muộn quá ngưỡng: khoảng 2 đến 3% sự kiện gửi muộn 12 đến 20 phút. Trong nhóm này phải có đủ purchase để doanh thu bị ảnh hưởng thấy rõ (ít nhất 15 purchase). Nhóm này phải rơi vào những cửa sổ đã bị chốt ở Gold khi chúng đến.

Thứ tự gửi và chia đợt:
- Thời điểm gửi = event time cộng độ trễ. Sắp theo thời điểm gửi.
- Chia thành 12 đợt, mỗi đợt tương ứng 5 phút thời gian gửi. Mỗi dòng trong `events.jsonl` có thêm trường `batch_no` (1 đến 12) và `send_time`. Hai trường này chỉ dùng cho producer và kiểm tra, KHÔNG đưa vào value gửi lên Kafka.
- Thêm một sự kiện "đẩy watermark" ở đợt cuối: một view bình thường có event time khoảng 11:30, để các cửa sổ cuối được chốt. Ghi rõ sự kiện này trong đáp án vàng và loại nó khỏi phần tính doanh thu.
- Tính độ trễ theo thời điểm gửi chứ không theo thứ tự trong file, và kiểm tra bằng code rằng số sự kiện đến muộn quá ngưỡng thực sự rơi vào cửa sổ đã chốt (xem mục 10, bước Gold).

Đáp án vàng (tính bằng pandas, ghi vào `golden_answer.json` và `golden_windows.csv`):
- Tổng số bản ghi gửi đi (kể cả trùng và sự kiện đẩy watermark).
- Số event_id duy nhất.
- Số bản trùng, trong đó số bản trùng có event_time lệch.
- Số sự kiện đến muộn trong ngưỡng, số sự kiện đến muộn quá ngưỡng (theo ngưỡng Gold 10 phút), và số purchase trong nhóm quá ngưỡng.
- Doanh thu và số đơn của từng cửa sổ 5 phút, tính trên toàn bộ purchase duy nhất (đáp án tuyệt đối).
- Doanh thu và số đơn của từng cửa sổ 5 phút sau khi bỏ nhóm muộn quá ngưỡng (đáp án kỳ vọng của Gold với ngưỡng 10 phút).
- Danh sách cửa sổ mà Gold sẽ xuất ra (các cửa sổ đã bị watermark vượt qua sau sự kiện đẩy watermark).

Cách xác định "muộn quá ngưỡng" cần mô phỏng đúng cơ chế watermark của Spark theo từng đợt (watermark = event time lớn nhất đã thấy ở các đợt TRƯỚC, trừ ngưỡng), vì Spark áp dụng watermark của batch trước cho batch sau. Viết hàm mô phỏng này trong `generate_events.py` và đối chiếu với kết quả thật từ Spark ở bước kiểm tra. Nếu hai bên lệch nhau, điều tra nguyên nhân; chưa giải quyết được thì ghi vào Vướng mắc trong `PLAN.md`, không tự chỉnh để khớp.

## 9. Producer (`producer/produce.py`)

- Topic `shop_events`, 3 partition, một broker, key là user_id, value là JSON của sự kiện (không có batch_no và send_time).
- Nhận tham số chọn đợt cần gửi (`--batch 3`, hoặc `--all`).
- Chỉ gửi, không tự sinh event_id hay thời gian.
- Có lệnh tạo topic và xóa topic để chạy lại từ đầu.
- Kiểm tra: tổng số message trong topic bằng tổng số bản ghi trong events.jsonl sau khi gửi hết.

## 10. Ba truy vấn streaming

Quy ước chung: mỗi truy vấn có checkpoint riêng trong `checkpoints/`. Dùng trigger `availableNow` để mỗi lần chạy xử lý hết dữ liệu hiện có rồi dừng, nhờ đó mỗi đợt tương ứng một micro-batch và kết quả tái lập được. Mỗi script có tham số `--continuous` để chạy liên tục (dùng cho demo, không dùng cho kiểm tra).

Bước Bronze (`01_kafka_to_bronze.py`):
- Đọc Kafka với startingOffsets earliest (chỉ có tác dụng ở lần chạy đầu của checkpoint).
- Giữ nguyên value dạng chuỗi, kèm partition, offset, timestamp của Kafka và thời điểm nạp. Không parse, không lọc.
- Ghi append vào Delta `lakehouse/bronze`.
- Kỳ vọng: số dòng Bronze bằng tổng số message (bản trùng vẫn còn).

Bước Silver (`02_bronze_to_silver.py`):
- Đọc stream từ bảng Bronze, parse JSON theo schema, chuyển event_time sang timestamp, lọc bản ghi lỗi (thiếu event_id hoặc event_time không hợp lệ; ghi số lượng bị loại vào log).
- Watermark 30 phút trên event_time, rồi dropDuplicatesWithinWatermark theo event_id.
- Ghi qua foreachBatch bằng MERGE vào `lakehouse/silver`: chỉ chèn khi event_id chưa tồn tại. Trước khi MERGE, lọc trùng event_id bên trong batch. Không dùng MERGE có nhánh UPDATE, vì Gold đọc Silver dưới dạng stream và nguồn streaming Delta yêu cầu bảng chỉ được thêm dữ liệu.
- Lý do watermark Silver rộng (30 phút) hơn Gold (10 phút): Silver có nhiệm vụ giữ đủ mọi sự kiện duy nhất; việc chấp nhận bỏ dữ liệu muộn là quyết định nghiệp vụ của Gold.
- Kỳ vọng: số dòng Silver bằng số event_id duy nhất trong đáp án vàng (trừ sự kiện nào bị lọc vì lỗi, nếu có; mặc định dữ liệu sinh ra không có bản ghi lỗi).
- Trong foreachBatch có điểm chèn lỗi có chủ đích (xem mục 11), mặc định tắt.

Bước Gold (`03_silver_to_gold.py`):
- Đọc stream từ Silver, chỉ lấy purchase, watermark 10 phút trên event_time, tumbling window 5 phút, tính tổng amount (`revenue`) và số đơn (`orders`).
- Output mode append, ghi vào `lakehouse/gold`. Cột window tách thành `window_start`, `window_end`.
- Tham số `--watermark` cho phép đổi ngưỡng (mặc định 10 phút) và `--suffix` để ghi vào thư mục bảng và checkpoint khác (dùng để so sánh ngưỡng 10 phút và 20 phút).
- Kỳ vọng: bảng Gold khớp doanh thu kỳ vọng sau khi bỏ nhóm muộn quá ngưỡng. Với ngưỡng 20 phút, doanh thu phải gần với đáp án tuyệt đối hơn, ghi lại chênh lệch.
- Lưu ý: ở append mode, một cửa sổ chỉ xuất khi watermark vượt qua nó. Sự kiện đẩy watermark ở đợt cuối nhằm chốt các cửa sổ cuối.

`streaming/run_all_batches.py`: với mỗi đợt từ 1 đến 12: gửi đợt đó, chạy Bronze, Silver, Gold (availableNow), rồi ghi lại số dòng và thông tin tiến trình của đợt vào `results/`.

## 11. Thí nghiệm lỗi (`experiments/`)

Không tắt tiến trình bằng tay. Dùng biến môi trường `FAIL_AT_BATCH=k` để hàm foreachBatch ném ngoại lệ NGAY SAU khi đã ghi xong nhưng TRƯỚC khi hàm kết thúc. Đây đúng là tình huống "sink đã nhận dữ liệu nhưng Spark chưa ghi commits".

Mỗi thí nghiệm bắt đầu từ trạng thái sạch (xóa lakehouse, checkpoints, tạo lại topic) và chạy đến hết 12 đợt, trừ đoạn gây lỗi. Mỗi thí nghiệm in và ghi ba con số: số dòng kỳ vọng, số dòng thực tế, chênh lệch.

- Thí nghiệm A (chạy lại cùng checkpoint, ghi idempotent): gây lỗi ở một batch của Silver, sau đó chạy lại không đặt biến lỗi, cùng checkpoint. Kỳ vọng: số dòng Silver vẫn đúng bằng số event_id duy nhất.
- Thí nghiệm B (ghi không idempotent): dùng biến thể của Silver ghi append thẳng trong foreachBatch (không MERGE, không txnAppId). Gây lỗi như A rồi chạy lại. Kỳ vọng: số dòng dư đúng bằng số dòng của batch bị chạy lại. Ghi rõ batch nào, bao nhiêu dòng.
- Thí nghiệm C (xóa checkpoint Bronze): chạy bình thường đến xong, xóa `checkpoints/bronze`, chạy lại Bronze với startingOffsets earliest. Kỳ vọng: Bronze gấp đôi số dòng. Sau đó chạy tiếp Silver (checkpoint Silver còn nguyên) và ghi lại kết quả. Nhận xét thật những gì quan sát được; không giả định trước Silver sẽ đúng, hãy chạy và đo, và nếu Silver có thay đổi bất ngờ thì giải thích nguyên nhân.

Với thí nghiệm A và B, kiểm tra thêm bằng cách đọc thư mục checkpoints (offsets, commits) và `_delta_log` trước và sau khi chạy lại, chụp lại nội dung (dạng văn bản) vào `results/` để làm bằng chứng.

## 12. Kiểm tra và giám sát

`verify/check_results.py`: in một bảng đối chiếu cuối cùng và thoát với mã khác 0 nếu có dòng sai.
- Bronze: số dòng so với tổng số message.
- Silver: số dòng so với số event_id duy nhất, và kiểm tra không có event_id trùng.
- Gold: từng cửa sổ so với doanh thu và số đơn kỳ vọng; liệt kê cửa sổ thiếu, thừa hoặc lệch.
- Số sự kiện bị loại vì quá muộn (lấy từ tiến trình truy vấn) so với đáp án vàng.

`verify/inspect_progress.py`: sau mỗi lần chạy, in và lưu các trường đáng chú ý của tiến trình truy vấn gần nhất: số dòng đầu vào, tốc độ xử lý, watermark hiện tại, số dòng trong state, số dòng bị loại vì quá muộn. Thêm một đoạn minh họa lịch sử bảng Delta (mỗi đợt tạo một phiên bản) và time travel (đọc bảng Silver ở phiên bản cũ).

## 13. Báo cáo kết quả (`results/`)

`run_report.json` và `run_report.md` phải chứa, đủ để người viết reading điền thẳng vào các bảng:
- Bộ phiên bản đã chạy thật (Java, Python, Spark, Delta, connector).
- Bảng 9 của reading: thống kê bộ dữ liệu và đáp án vàng.
- Bảng 4 và bảng ở phần III.4 thật: với ngưỡng 10 phút và 20 phút, số sự kiện bị loại, doanh thu từng cửa sổ, batch chốt cửa sổ.
- Bảng 10 của reading: kết quả ba thí nghiệm lỗi (kỳ vọng, thực tế, chênh lệch).
- Mẫu nội dung thư mục checkpoint (offsets, commits) và `_delta_log` cho các hình chụp.
- Mọi bất thường gặp phải và cách xử lý.

## 14. Thứ tự thực hiện

1. Dựng môi trường, `docker-compose.yml`, kiểm tra Kafka và Spark kết nối được với nhau. Báo cáo bộ phiên bản chạy được.
2. Viết `config.py`, `schema.py`, `spark_session.py`.
3. Viết `generate_events.py`, sinh dữ liệu, kiểm tra các tỷ lệ và đáp án vàng. Dừng lại, tóm tắt thống kê cho người dùng xem trước khi sang bước 4.
4. Viết producer và chạy thử.
5. Viết ba truy vấn streaming, chạy `run_all_batches.py`, chạy `check_results.py` cho đến khi khớp.
6. Chạy Gold với ngưỡng 20 phút để so sánh.
7. Viết và chạy ba thí nghiệm lỗi.
8. Hoàn thiện `README.md` và `results/`.

Sau mỗi bước, báo ngắn gọn: đã làm gì, kết quả kiểm tra, số liệu chính.

## 15. Những điều không được làm

- Không sửa nội dung reading hay đổi thiết kế (cửa sổ 5 phút, watermark Gold 10 phút, watermark Silver 30 phút, topic, tên bảng) mà không hỏi người dùng.
- Không dùng dữ liệu có sẵn từ bài khác hay tải dữ liệu ngoài.
- Không để thí nghiệm lỗi chạy vào dữ liệu của lần chạy chuẩn: luôn dùng thư mục và checkpoint riêng hoặc trạng thái sạch.
- Không dùng thời gian hiện tại, số ngẫu nhiên không seed hay UUID mới trong logic xử lý streaming (phải tất định để chạy lại cho kết quả giống nhau).
