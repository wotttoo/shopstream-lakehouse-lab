# PLAN.md: ShopStream Lab

Đặc tả chi tiết nằm ở `CLAUDE.md`. Lệnh kiểm chứng chạy tại `shopstream-lab/` sau khi `source .venv/bin/activate`, trừ khi ghi khác.

## 1. Mục tiêu cuối cùng

Có một bộ code và data tự đứng độc lập, chạy lại được từ trạng thái sạch trên Ubuntu 24.04, đưa sự kiện ShopStream từ Kafka qua Spark Structured Streaming vào Delta Lake (Bronze, Silver, Gold). Bộ này minh họa bằng số liệu thật cách xử lý dữ liệu trùng, dữ liệu đến muộn và lỗi giữa chừng, đủ để điền thẳng vào reading, quiz và solution.

## 2. Định nghĩa "hoàn thành"

Project hoàn thành khi mọi task dưới đây đã đánh dấu `[x]`, mục "Vướng mắc" không còn vấn đề mở, và tất cả điều kiện sau đều đúng:

| # | Điều kiện | Lệnh kiểm chứng |
|---|---|---|
| 1 | Unit test pass | `pytest tests/` |
| 2 | Bronze, Silver, Gold khớp đáp án vàng sau một lần chạy từ trạng thái sạch | `python verify/check_results.py` thoát với mã 0 |
| 3 | Sinh dữ liệu tái lập được | Chạy `python data/generate_events.py` hai lần, `sha256sum data/events.jsonl` không đổi |
| 4 | Có so sánh ngưỡng 10 phút và 20 phút từ cùng một lần chạy theo đợt | `python verify/check_results.py --watermark-suffix _wm20` in chênh lệch so với đáp án tuyệt đối |
| 5 | Có kết quả thật của ba thí nghiệm lỗi | `python verify/check_report.py` xác nhận đủ kỳ vọng, thực tế, chênh lệch của A, B, C |
| 6 | Báo cáo đủ các bảng theo mục 13 của `CLAUDE.md` | `python verify/check_report.py` thoát với mã 0 |
| 7 | Không có dữ liệu sinh ra hay bí mật trong git | `git ls-files \| grep -E 'lakehouse/\|checkpoints/\|\.venv/'` không in gì |
| 8 | Người mới làm theo `README.md` chạy được | Task 49 pass |

## 3. Song song

Mỗi nhóm chỉ làm song song sau khi các task phụ thuộc của nhóm đã xong. Task không thuộc nhóm nào phải làm tuần tự. Các task sửa cùng một file không được xếp song song, nên cả Giai đoạn 3 là tuần tự.

| Nhóm | Task | Điều kiện bắt đầu | Ghi chú |
|---|---|---|---|
| A | 3, 4 | Task 2 xong | Docker và Spark độc lập nhau |
| B | 7, 8 | Task 6 xong | Hai file riêng |
| C | 19, 26 | Task 17 xong và người dùng đã duyệt | Chỉ phần viết hàm thuần, file riêng |
| D | 38, 39 | Task 36 xong | Chỉ đọc kết quả, file riêng |
| E | 42, 44, 45 | Task 41 xong | Chỉ viết code song song. Khi CHẠY thì tuần tự, vì ba thí nghiệm dùng chung topic |

## 4. Danh sách task

### Giai đoạn 1: Môi trường

- [x] **1. Khởi tạo repo và khung thư mục**
  - Việc cần làm: `git init` ở thư mục gốc, tạo `shopstream-lab/` với các thư mục con, viết `.gitignore`.
  - File: `shopstream-lab/.gitignore`.
  - Hoàn thành khi (chạy ở thư mục gốc): `git check-ignore shopstream-lab/lakehouse/x shopstream-lab/checkpoints/x shopstream-lab/.venv/x` in đủ ba dòng.

- [x] **2. Cài môi trường Python và Java**
  - Việc cần làm: viết `requirements.txt` có ghim phiên bản, tạo `.venv` theo mục 3 của `CLAUDE.md`.
  - File: `requirements.txt`.
  - Hoàn thành khi: `java -version` là 17, `python --version` là 3.11, và `python -c "import pyspark, delta; print(pyspark.__version__)"` in `4.1.0`.

- [x] **3. Dựng Kafka bằng Docker Compose** (nhóm A)
  - Việc cần làm: một broker KRaft, advertised listener qua `localhost:9092`.
  - File: `docker-compose.yml`.
  - Hoàn thành khi: `docker compose up -d` rồi `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` thoát với mã 0.

- [x] **4. Kiểm tra Spark ghi đọc được Delta** (nhóm A)
  - Việc cần làm: script ghi rồi đọc một bảng Delta nhỏ trong thư mục project. Đây cũng là phép thử đường dẫn có dấu cách và ký tự `→`.
  - File: `verify/smoke_env.py`.
  - Hoàn thành khi: `python verify/smoke_env.py --delta` thoát với mã 0 và in đúng số dòng đã ghi.

- [x] **5. Kiểm tra Spark đọc được Kafka**
  - Việc cần làm: thêm chế độ gửi một message vào topic tạm, đọc lại bằng Spark, xóa topic tạm.
  - File: `verify/smoke_env.py`.
  - Phụ thuộc: 3, 4.
  - Hoàn thành khi: `python verify/smoke_env.py --kafka` thoát với mã 0 và in lại đúng message.

- [x] **6. Ghi bộ phiên bản chạy được**
  - Việc cần làm: tạo `README.md` bản đầu với bộ phiên bản đã chạy thật. Nếu phải lùi về Spark 4.0.1 thì cập nhật cả `requirements.txt` và mục 2 của `CLAUDE.md`.
  - File: `README.md`, `requirements.txt`.
  - Hoàn thành khi: `grep -F "$(python -c 'import pyspark; print(pyspark.__version__)')" README.md` tìm thấy.

### Giai đoạn 2: Phần dùng chung

- [x] **7. Viết file cấu hình** (nhóm B)
  - Việc cần làm: đường dẫn, tên topic, ngưỡng watermark Gold và Silver, kích thước cửa sổ, seed. Đường dẫn tính theo vị trí file, không phụ thuộc thư mục đang đứng.
  - File: `common/config.py`, `tests/test_config.py`.
  - Hoàn thành khi: `pytest tests/test_config.py` pass, kiểm tra được 5 phút, 10 phút, 30 phút và `shop_events`.

- [x] **8. Viết schema sự kiện** (nhóm B)
  - Việc cần làm: schema Spark của sáu trường sự kiện, `event_time` để dạng chuỗi.
  - File: `common/schema.py`, `tests/test_schema.py`.
  - Hoàn thành khi: `pytest tests/test_schema.py` pass.

- [x] **9. Viết hàm tạo SparkSession dùng chung**
  - Việc cần làm: múi giờ UTC, 4 partition shuffle, hai cấu hình Delta, jar Kafka. Chuyển `smoke_env.py` sang dùng hàm này.
  - File: `common/spark_session.py`, `verify/smoke_env.py`.
  - Phụ thuộc: 7.
  - Hoàn thành khi: `python verify/smoke_env.py --delta --kafka` thoát với mã 0 và in `UTC`, `4`.

### Giai đoạn 3: Dữ liệu giả lập

Toàn bộ giai đoạn này chỉ sửa `data/generate_events.py` và `tests/test_generate_events.py`, không cần Spark hay Kafka.

- [x] **10. Sinh sự kiện gốc**
  - Việc cần làm: khoảng 2.000 sự kiện, 50 user, 20 sản phẩm, tỷ lệ 70/20/10, amount đúng luật, event_id tất định theo seed 42.
  - Hoàn thành khi: `pytest tests/test_generate_events.py -k base_events` pass.

- [x] **11. Cài bẫy trùng lặp**
  - Việc cần làm: thêm khoảng 10% bản trùng, trong đó khoảng 1/5 lệch event_time vài giây, giữ nguyên event_id.
  - Hoàn thành khi: `pytest tests/test_generate_events.py -k duplicates` pass.

- [x] **12. Cài bẫy đến muộn và tính send_time**
  - Việc cần làm: khoảng 15% muộn 1 đến 4 phút, khoảng 2 đến 3% muộn 12 đến 20 phút với ít nhất 15 purchase, send_time bằng event time cộng độ trễ.
  - Hoàn thành khi: `pytest tests/test_generate_events.py -k late` pass.

- [x] **13. Chia đợt và thêm sự kiện đẩy watermark**
  - Việc cần làm: sắp theo send_time, gán `batch_no` 1 đến 12, thêm một view khoảng 11:30 ở đợt 12.
  - Hoàn thành khi: `pytest tests/test_generate_events.py -k batches` pass.

- [x] **14. Viết hàm mô phỏng watermark theo đợt**
  - Việc cần làm: watermark của một đợt bằng event time lớn nhất ở các đợt TRƯỚC trừ ngưỡng.
  - Lưu ý cần xác nhận ở task 34: (a) tính max trên MỌI loại sự kiện, vì view đẩy watermark phải có tác dụng; (b) với aggregation theo cửa sổ, nhiều khả năng Spark loại dòng theo `window_end <= watermark`, không phải theo event time của dòng, nên mô phỏng theo cửa sổ.
  - Hoàn thành khi: `pytest tests/test_generate_events.py -k watermark_simulation` pass với ví dụ nhỏ tính tay.

- [x] **15. Tính thống kê tổng của đáp án vàng**
  - Việc cần làm: mọi con số tổng ở mục 8 của `CLAUDE.md`. Với bản trùng lệch thời gian, event_time có hiệu lực là của bản gửi trước.
  - Hoàn thành khi: `python data/generate_events.py` tạo `data/events.jsonl`, `data/golden_answer.json`, và `pytest tests/test_generate_events.py -k golden_answer` pass.

- [x] **16. Tính đáp án vàng theo cửa sổ**
  - Việc cần làm: doanh thu và số đơn tuyệt đối, kỳ vọng ngưỡng 10 phút, kỳ vọng ngưỡng 20 phút, danh sách cửa sổ được xuất, đợt chốt mỗi cửa sổ.
  - Hoàn thành khi: `python data/generate_events.py` tạo `data/golden_windows.csv`, và `pytest tests/test_generate_events.py -k golden_windows` pass.

- [x] **17. Kiểm tra toàn bộ dữ liệu sinh ra**
  - Việc cần làm: kiểm tra bằng code rằng nhóm quá ngưỡng thực sự rơi vào cửa sổ đã chốt, chạy lại cho cùng kết quả, thêm tùy chọn in tóm tắt.
  - Hoàn thành khi: `pytest tests/test_generate_events.py` pass và `python data/generate_events.py --summary` in đủ thống kê.

- [ ] **18. ĐIỂM DỪNG: người dùng duyệt thống kê**
  - Việc cần làm: tóm tắt thống kê cho người dùng. Không làm task nào từ 19 trở đi khi chưa được duyệt.
  - Hoàn thành khi: người dùng xác nhận đồng ý (không kiểm chứng bằng lệnh được).

### Giai đoạn 4: Producer

- [ ] **19. Viết hàm tạo value gửi Kafka** (nhóm C)
  - Việc cần làm: hàm thuần tách `batch_no`, `send_time` khỏi value, key là user_id, lọc theo đợt.
  - File: `producer/produce.py`, `tests/test_produce.py`.
  - Hoàn thành khi: `pytest tests/test_produce.py` pass.

- [ ] **20. Thêm lệnh tạo và xóa topic**
  - Việc cần làm: `--create-topic` với 3 partition, `--delete-topic`.
  - File: `producer/produce.py`.
  - Phụ thuộc: 19.
  - Hoàn thành khi: sau `python producer/produce.py --create-topic`, lệnh `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic shop_events` cho thấy 3 partition.

- [ ] **21. Thêm lệnh gửi theo đợt**
  - Việc cần làm: `--batch N` và `--all`.
  - File: `producer/produce.py`.
  - Phụ thuộc: 20.
  - Hoàn thành khi: `python producer/produce.py --batch 1` thoát với mã 0 và in đúng số bản ghi của đợt 1.

- [ ] **22. Thêm lệnh đếm message và chạy thử trọn vẹn**
  - Việc cần làm: `--count` in tổng message trong topic. Chạy thử bằng cách xóa, tạo lại topic rồi gửi `--all`.
  - File: `producer/produce.py`.
  - Phụ thuộc: 21.
  - Hoàn thành khi: `python producer/produce.py --count` bằng `wc -l < data/events.jsonl`. Sau đó xóa và tạo lại topic để trả về trạng thái rỗng.

### Giai đoạn 5: Ba truy vấn streaming

- [ ] **23. Viết truy vấn Bronze**
  - Việc cần làm: đọc Kafka earliest, giữ value chuỗi kèm partition, offset, timestamp, thời điểm nạp, ghi append, có `--continuous`.
  - File: `streaming/01_kafka_to_bronze.py`.
  - Phụ thuộc: 9, 22.
  - Hoàn thành khi: sau khi gửi đợt 1, `python streaming/01_kafka_to_bronze.py` thoát với mã 0 và `lakehouse/bronze/_delta_log` tồn tại.

- [ ] **24. Thêm kiểm tra Bronze**
  - Việc cần làm: so số dòng với số bản ghi đến đợt chỉ định.
  - File: `verify/check_results.py`.
  - Phụ thuộc: 23.
  - Hoàn thành khi: `python verify/check_results.py --layer bronze --up-to-batch 1` thoát với mã 0.

- [ ] **25. Viết phần parse và lọc của Silver**
  - Việc cần làm: parse JSON theo schema, đổi event_time sang timestamp, lọc bản ghi lỗi và ghi số bị loại vào log. Tách thành hàm để test được.
  - File: `common/transforms.py`, `tests/test_transforms.py`.
  - Phụ thuộc: 8.
  - Hoàn thành khi: `pytest tests/test_transforms.py -k parse` pass. Test này được phép dùng Spark local, là ngoại lệ duy nhất của quy ước unit test.

- [ ] **26. Viết hàm lọc trùng trong batch** (nhóm C)
  - Việc cần làm: hàm trên DataFrame giữ một dòng cho mỗi event_id, dùng trước MERGE.
  - File: `common/transforms.py`, `tests/test_transforms.py`.
  - Hoàn thành khi: `pytest tests/test_transforms.py -k dedup` pass.

- [ ] **27. Viết truy vấn Silver với MERGE**
  - Việc cần làm: watermark 30 phút, `dropDuplicatesWithinWatermark`, foreachBatch MERGE chỉ chèn, tạo bảng nếu chưa có.
  - File: `streaming/02_bronze_to_silver.py`.
  - Phụ thuộc: 23, 25, 26.
  - Hoàn thành khi: `python streaming/02_bronze_to_silver.py` thoát với mã 0 và `lakehouse/silver/_delta_log` tồn tại.

- [ ] **28. Thêm điểm chèn lỗi FAIL_AT_BATCH**
  - Việc cần làm: ném ngoại lệ sau khi MERGE xong, trước khi foreachBatch kết thúc, mặc định tắt.
  - File: `streaming/02_bronze_to_silver.py`.
  - Phụ thuộc: 27.
  - Hoàn thành khi: dùng một checkpoint thử, `FAIL_AT_BATCH=0 python streaming/02_bronze_to_silver.py --suffix _failtest` thoát với mã khác 0. Không đặt biến thì thoát với mã 0.

- [ ] **29. Thêm kiểm tra Silver**
  - Việc cần làm: so số dòng với số event_id duy nhất đến đợt chỉ định, kiểm tra không có event_id trùng.
  - File: `verify/check_results.py`.
  - Phụ thuộc: 27.
  - Hoàn thành khi: `python verify/check_results.py --layer silver --up-to-batch 1` thoát với mã 0.

- [ ] **30. Viết truy vấn Gold**
  - Việc cần làm: tumbling window 5 phút, `revenue`, `orders`, append, tách `window_start`, `window_end`, tham số `--watermark`, `--suffix`.
  - Lưu ý: đặt `withWatermark` TRƯỚC khi lọc purchase. Nếu lọc trước, view đẩy watermark bị loại trước khi tới toán tử watermark, các cửa sổ cuối sẽ không được chốt.
  - File: `streaming/03_silver_to_gold.py`.
  - Phụ thuộc: 27.
  - Hoàn thành khi: `python streaming/03_silver_to_gold.py` thoát với mã 0 và `lakehouse/gold/_delta_log` tồn tại.

- [ ] **31. Viết vòng lặp chạy một đợt**
  - Việc cần làm: gửi một đợt rồi chạy Bronze, Silver, Gold bằng subprocess, có `--up-to-batch`.
  - File: `streaming/run_all_batches.py`.
  - Phụ thuộc: 30.
  - Hoàn thành khi: từ trạng thái sạch, `python streaming/run_all_batches.py --up-to-batch 2` thoát với mã 0 và `python verify/check_results.py --layer silver --up-to-batch 2` thoát với mã 0.

- [ ] **32. Ghi số dòng và tiến trình theo đợt**
  - Việc cần làm: mỗi truy vấn lưu `lastProgress` của mọi micro-batch, vòng lặp gom thành file của đợt. Ghi cả số micro-batch thực tế của mỗi lần chạy, vì có thể có thêm batch không dữ liệu.
  - File: `streaming/run_all_batches.py`, ba file truy vấn.
  - Phụ thuộc: 31.
  - Hoàn thành khi: từ trạng thái sạch, chạy đến đợt 2 tạo được `results/progress/batch_02.json` có số dòng ba tầng.

- [ ] **33. Thêm so sánh ngưỡng 20 phút vào vòng lặp**
  - Việc cần làm: `--compare-wm20` chạy thêm một Gold `--watermark "20 minutes" --suffix _wm20` ở MỖI đợt. Không chạy một lần sau cùng, vì khi đó toàn bộ Silver vào một batch và sẽ không có dòng nào bị loại, phép so sánh mất ý nghĩa.
  - File: `streaming/run_all_batches.py`.
  - Phụ thuộc: 32.
  - Hoàn thành khi: từ trạng thái sạch, chạy với `--compare-wm20 --up-to-batch 2` tạo được `lakehouse/gold_wm20/_delta_log`.

- [ ] **34. Chạy đủ 12 đợt và đối chiếu Gold từng cửa sổ**
  - Việc cần làm: thêm kiểm tra Gold vào `check_results.py` (cửa sổ thiếu, thừa, lệch). Chạy trọn từ trạng thái sạch. Xác nhận hai lưu ý ở task 14, và rằng cửa sổ cuối được chốt khi `availableNow` kết thúc ở đợt 12. Nếu không, ghi vào Vướng mắc.
  - File: `verify/check_results.py`.
  - Phụ thuộc: 33.
  - Hoàn thành khi: `python streaming/run_all_batches.py --compare-wm20` rồi `python verify/check_results.py --layer gold` thoát với mã 0.

- [ ] **35. Đối chiếu số dòng bị loại vì quá muộn**
  - Việc cần làm: lấy `numRowsDroppedByWatermark` từ tiến trình, so với số purchase bị loại trong đáp án vàng. Metric này chỉ đếm dòng tới toán tử có trạng thái, nên so với purchase, không so với mọi sự kiện.
  - File: `verify/check_results.py`.
  - Phụ thuộc: 34.
  - Hoàn thành khi: `python verify/check_results.py` chạy đủ các tầng và thoát với mã 0.

- [ ] **36. So sánh kết quả ngưỡng 10 phút và 20 phút**
  - Việc cần làm: tham số `--watermark-suffix`, in chênh lệch doanh thu mỗi ngưỡng so với đáp án tuyệt đối.
  - File: `verify/check_results.py`.
  - Phụ thuộc: 35.
  - Hoàn thành khi: `python verify/check_results.py --watermark-suffix _wm20` thoát với mã 0, và chênh lệch của 20 phút nhỏ hơn của 10 phút.

- [ ] **37. Kiểm tra chạy lại tái lập được**
  - Việc cần làm: lưu bảng Gold đã sắp xếp, chạy lại từ trạng thái sạch, so sánh.
  - File: không thêm file mới, chỉ chạy.
  - Phụ thuộc: 36.
  - Hoàn thành khi: hai lần chạy cho cùng một `sha256sum` trên bản CSV của Gold. Lưu ý sinh bản CSV có sắp xếp theo `window_start`.

### Giai đoạn 6: Giám sát

- [ ] **38. In tiến trình truy vấn** (nhóm D)
  - Việc cần làm: số dòng đầu vào, tốc độ, watermark, số dòng trong state, số dòng bị loại.
  - File: `verify/inspect_progress.py`.
  - Hoàn thành khi: `python verify/inspect_progress.py --progress` thoát với mã 0 và lưu `results/progress_summary.json`.

- [ ] **39. Minh họa lịch sử Delta và time travel** (nhóm D)
  - Việc cần làm: in `DESCRIBE HISTORY`, đọc Silver ở một phiên bản cũ.
  - File: `verify/inspect_progress.py`.
  - Hoàn thành khi: `python verify/inspect_progress.py --history` in số dòng Silver ở phiên bản cũ nhỏ hơn phiên bản mới nhất.

### Giai đoạn 7: Thí nghiệm lỗi

Thí nghiệm dùng `lakehouse/exp_a/`, `checkpoints/exp_a/` (tương tự với b, c) để không chạm vào kết quả chuẩn. Topic được tạo lại ở đầu mỗi thí nghiệm, nên muốn chạy lại luồng chuẩn sau đó phải chạy từ trạng thái sạch.

- [ ] **40. Cho truy vấn nhận thư mục gốc của thí nghiệm**
  - Việc cần làm: thêm tham số chọn thư mục bảng và checkpoint vào ba truy vấn, giá trị mặc định giữ nguyên hành vi cũ.
  - File: ba file truy vấn.
  - Phụ thuộc: 37.
  - Hoàn thành khi: `python verify/check_results.py` vẫn thoát với mã 0 sau khi chạy lại `python streaming/run_all_batches.py --compare-wm20` từ trạng thái sạch.

- [ ] **41. Viết tiện ích dùng chung cho thí nghiệm**
  - Việc cần làm: dọn trạng thái của thí nghiệm, tạo lại topic, chụp nội dung `offsets`, `commits`, `_delta_log` ra file văn bản, ghi ba con số kỳ vọng, thực tế, chênh lệch. Việc dọn chỉ được chạm vào thư mục `exp_*`.
  - File: `experiments/lab_utils.py`, `tests/test_lab_utils.py`.
  - Phụ thuộc: 40.
  - Hoàn thành khi: `pytest tests/test_lab_utils.py` pass, có test xác nhận không dọn được đường dẫn ngoài `exp_*`.

- [ ] **42. Viết thí nghiệm A** (nhóm E)
  - Việc cần làm: gây lỗi ở một batch Silver, chạy lại cùng checkpoint, ghi idempotent.
  - File: `experiments/exp_a_rerun_idempotent.py`.
  - Hoàn thành khi: chạy script tạo `results/exp_a.json`, và chênh lệch bằng 0 hoặc đã ghi vào Vướng mắc nếu khác.

- [ ] **43. Lưu bằng chứng thí nghiệm A**
  - Việc cần làm: chụp checkpoint và `_delta_log` trước và sau khi chạy lại.
  - File: `experiments/exp_a_rerun_idempotent.py`.
  - Phụ thuộc: 42.
  - Hoàn thành khi: có `results/evidence/exp_a_before.txt` và `exp_a_after.txt`, trong đó `before` có offsets của batch lỗi nhưng không có commits tương ứng.

- [ ] **44. Viết thí nghiệm B** (nhóm E)
  - Việc cần làm: biến thể Silver append thẳng, không MERGE, không txnAppId, gây lỗi rồi chạy lại, kèm chụp bằng chứng như task 43.
  - File: `experiments/exp_b_non_idempotent.py`.
  - Hoàn thành khi: chạy script tạo `results/exp_b.json` ghi rõ batch nào, bao nhiêu dòng, và có hai file bằng chứng. Số dòng dư được so với số dòng của batch chạy lại, lệch thì ghi vào Vướng mắc.

- [ ] **45. Viết thí nghiệm C** (nhóm E)
  - Việc cần làm: chạy xong, xóa `checkpoints/exp_c/bronze`, chạy lại Bronze, đo số dòng, rồi chạy Silver và đo. Ghi nhận xét theo số đo thật.
  - File: `experiments/exp_c_delete_checkpoint.py`.
  - Hoàn thành khi: chạy script tạo `results/exp_c.json` có số dòng Bronze và Silver trước và sau. Bronze gấp đôi hoặc đã ghi vào Vướng mắc nếu khác.

### Giai đoạn 8: Báo cáo và bàn giao

- [ ] **46. Tổng hợp `run_report.json`**
  - Việc cần làm: gom bộ phiên bản, thống kê dữ liệu, so sánh hai ngưỡng, ba thí nghiệm, mẫu checkpoint và `_delta_log`, bất thường. Mọi số đọc từ file kết quả, không gõ tay. Viết `verify/check_report.py` kiểm tra đủ khóa.
  - File: `verify/build_report.py`, `verify/check_report.py`, `results/run_report.json`.
  - Phụ thuộc: 39, 43, 44, 45.
  - Hoàn thành khi: `python verify/build_report.py` rồi `python verify/check_report.py` thoát với mã 0.

- [ ] **47. Sinh `run_report.md`**
  - Việc cần làm: dựng các bảng 4, 9, 10 và bảng phần III.4 của reading từ `run_report.json`.
  - File: `verify/build_report.py`, `results/run_report.md`.
  - Phụ thuộc: 46.
  - Hoàn thành khi: `grep -c '^|' results/run_report.md` lớn hơn 0 và `python verify/check_report.py --markdown` thoát với mã 0.

- [ ] **48. Hoàn thiện `README.md`**
  - Việc cần làm: cài đặt, cách chạy, cách test, khắc phục lỗi thường gặp, gồm các lỗi đã gặp thật.
  - File: `README.md`.
  - Phụ thuộc: 47.
  - Hoàn thành khi: `grep -c 'Khắc phục' README.md` lớn hơn 0 và mọi lệnh trong README đã được chạy ít nhất một lần.

- [ ] **49. Chạy nghiệm thu từ bản sao sạch**
  - Việc cần làm: `git clone` repo sang thư mục mới, làm theo `README.md`, không dùng file ngoài repo.
  - Phụ thuộc: 48.
  - Hoàn thành khi: trong bản sao, `pytest tests/` pass và `python verify/check_results.py` thoát với mã 0. So sánh `run_report.json` với bản gốc: số liệu dữ liệu và Gold phải giống hệt.

## 5. Vướng mắc

