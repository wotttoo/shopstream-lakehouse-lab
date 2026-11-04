# ShopStream Lab: Stream Processing vào Lakehouse

Bài thực hành mô phỏng sàn thương mại điện tử ShopStream: sự kiện `view`, `add_to_cart`, `purchase` đi qua Kafka, được Spark Structured Streaming xử lý và ghi vào Delta Lake theo kiến trúc Medallion (Bronze, Silver, Gold).

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

Bộ phiên bản chính trong `CLAUDE.md` (Spark 4.1.0, delta-spark 4.1.0) chạy được, không phải lùi về Spark 4.0.1.

Kiểm tra môi trường:

```bash
python verify/smoke_env.py --delta --kafka
```
