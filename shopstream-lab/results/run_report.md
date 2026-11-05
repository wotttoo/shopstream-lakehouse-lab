# Báo cáo kết quả chạy ShopStream Lab

Mọi số liệu dưới đây sinh tự động từ `results/` và `data/` bằng `verify/build_report.py`.

## 1. Bộ phiên bản đã chạy thật

| Thành phần | Phiên bản |
|---|---|
| java | 17.0.20.1 |
| python | 3.11.18 |
| pyspark | 4.1.0 |
| delta_spark | 4.1.0 |
| delta_jar | io.delta:delta-spark_4.1_2.13:4.1.0 |
| kafka_connector | org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 |
| kafka_image | apache/kafka:3.9.1 |
| kafka_python | 2.2.15 |
| pandas | 2.3.3 |
| numpy | 2.3.5 |
| pytest | 8.4.2 |

## 2. Bảng 9: thống kê bộ dữ liệu và đáp án vàng

| Chỉ số | Giá trị |
|---|---|
| Tổng số bản ghi gửi đi (kể cả trùng và sự kiện đẩy watermark) | 2201 |
| Số event_id duy nhất | 2001 |
| Số sự kiện nghiệp vụ duy nhất (bỏ sự kiện đẩy watermark) | 2000 |
| Số bản trùng | 200 |
| trong đó giống hệt bản gốc | 160 |
| trong đó event_time lệch vài giây | 40 |
| Số dòng Silver nếu lọc trùng theo (event_id, event_time) | 2041 |
| Số sự kiện đến muộn trong ngưỡng (1 đến 4 phút) | 300 |
| Số sự kiện đến muộn quá ngưỡng (ngưỡng Gold 10 phút) | 50 |
| trong đó purchase | 18 |
| Số user | 50 |
| Số sản phẩm | 20 |
| Số purchase duy nhất | 208 |
| Doanh thu tuyệt đối (đồng) | 55.622.000 |
| Số sự kiện add_to_cart | 391 |
| Số sự kiện purchase | 208 |
| Số sự kiện view | 1401 |

## 3. Bảng 4: so sánh ngưỡng watermark Gold

| Ngưỡng | Sự kiện quá ngưỡng (đáp án vàng) | Purchase bị loại (đo từ Gold) | numRowsDroppedByWatermark | Doanh thu Gold | Chênh lệch so với tuyệt đối | Tỷ lệ |
|---|---|---|---|---|---|---|
| 10 phút | 50 | 18 | 8 | 50.798.000 | 4.824.000 | 8.67% |
| 20 phút | 0 | 0 | 0 | 55.622.000 | 0 | 0.0% |

Doanh thu tuyệt đối: 55.622.000 đồng.

## 4. Bảng phần III.4: doanh thu từng cửa sổ và đợt chốt cửa sổ

| Cửa sổ | Tuyệt đối (đơn) | 10 phút: doanh thu (đơn) | 10 phút: bị loại | 10 phút: đợt chốt | 20 phút: doanh thu (đơn) | 20 phút: bị loại | 20 phút: đợt chốt |
|---|---|---|---|---|---|---|---|
| 10:00-10:05 | 3.598.000 (18) | 3.494.000 (17) | 1 | 4 | 3.598.000 (18) | 0 | 6 |
| 10:05-10:10 | 4.700.000 (16) | 4.405.000 (15) | 1 | 5 | 4.700.000 (16) | 0 | 7 |
| 10:10-10:15 | 6.269.000 (23) | 5.540.000 (20) | 3 | 6 | 6.269.000 (23) | 0 | 8 |
| 10:15-10:20 | 3.564.000 (15) | 2.629.000 (11) | 4 | 7 | 3.564.000 (15) | 0 | 9 |
| 10:20-10:25 | 4.004.000 (14) | 4.004.000 (14) | 0 | 8 | 4.004.000 (14) | 0 | 10 |
| 10:25-10:30 | 5.819.000 (25) | 4.778.000 (21) | 4 | 9 | 5.819.000 (25) | 0 | 11 |
| 10:30-10:35 | 5.774.000 (18) | 5.192.000 (16) | 2 | 10 | 5.774.000 (18) | 0 | 12 |
| 10:35-10:40 | 4.848.000 (17) | 3.710.000 (14) | 3 | 11 | 4.848.000 (17) | 0 | 12 |
| 10:40-10:45 | 3.587.000 (12) | 3.587.000 (12) | 0 | 12 | 3.587.000 (12) | 0 | 12 |
| 10:45-10:50 | 4.867.000 (19) | 4.867.000 (19) | 0 | 12 | 4.867.000 (19) | 0 | 12 |
| 10:50-10:55 | 3.291.000 (11) | 3.291.000 (11) | 0 | 12 | 3.291.000 (11) | 0 | 12 |
| 10:55-11:00 | 5.301.000 (20) | 5.301.000 (20) | 0 | 12 | 5.301.000 (20) | 0 | 12 |

## 5. Watermark và số dòng theo đợt

| Đợt | Bản ghi gửi | Bronze | Silver | Gold | Gold 20 phút | Watermark Silver | Watermark Gold 10 phút | Watermark Gold 20 phút |
|---|---|---|---|---|---|---|---|---|
| 1 | 138 | 138 | 133 | 0 | 0 | 09:34:57 | 09:54:57 | 09:44:57 |
| 2 | 215 | 353 | 328 | 0 | 0 | 09:39:57 | 09:59:57 | 09:49:57 |
| 3 | 166 | 519 | 479 | 0 | 0 | 09:44:52 | 10:04:52 | 09:54:52 |
| 4 | 169 | 688 | 631 | 1 | 0 | 09:49:51 | 10:09:51 | 09:59:51 |
| 5 | 171 | 859 | 784 | 2 | 0 | 09:54:55 | 10:14:55 | 10:04:55 |
| 6 | 192 | 1051 | 961 | 3 | 1 | 09:59:55 | 10:19:55 | 10:09:55 |
| 7 | 184 | 1235 | 1132 | 4 | 2 | 10:04:51 | 10:24:51 | 10:14:51 |
| 8 | 186 | 1421 | 1298 | 5 | 3 | 10:09:56 | 10:29:56 | 10:19:56 |
| 9 | 177 | 1598 | 1457 | 6 | 4 | 10:14:49 | 10:34:49 | 10:24:49 |
| 10 | 186 | 1784 | 1626 | 7 | 5 | 10:19:56 | 10:39:56 | 10:29:56 |
| 11 | 193 | 1977 | 1797 | 8 | 6 | 10:24:55 | 10:44:55 | 10:34:55 |
| 12 | 224 | 2201 | 2001 | 12 | 12 | 11:00:00 | 11:20:00 | 11:10:00 |

## 6. Bảng 10: kết quả ba thí nghiệm lỗi

| Thí nghiệm | Tầng đo | Kỳ vọng | Thực tế | Chênh lệch | Ghi chú |
|---|---|---|---|---|---|
| exp_a | silver | 2001 | 2001 | 0 | lỗi ở batch 10 (đợt 6), batch đó đã ghi 177 dòng; chạy lại cùng checkpoint, MERGE chèn 0 dòng |
| exp_b | silver | 2001 | 2178 | 177 | batch 10 chạy lại, 177 dòng bị ghi hai lần |
| exp_c | bronze | 4402 | 4402 | 0 | Bronze đọc lại 2201 dòng; Silver trước 2001, sau 2001 (kỳ vọng 2001) |

## 7. Mẫu checkpoint và _delta_log (lần chạy chuẩn)

**checkpoints/bronze/offsets/10**

```
v1
{"batchWatermarkMs":0,"batchTimestampMs":1793815619830,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"4","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
{"shop_events":{"0":800,"1":449,"2":728}}
```

**checkpoints/bronze/offsets/11**

```
v1
{"batchWatermarkMs":0,"batchTimestampMs":1793815712674,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"4","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
{"shop_events":{"0":889,"1":492,"2":820}}
```

**checkpoints/bronze/commits/10**

```
v1
{"nextBatchWatermarkMs":0}
```

**checkpoints/bronze/commits/11**

```
v1
{"nextBatchWatermarkMs":0}
```

**lakehouse/bronze/_delta_log/00000000000000000011.json**

```
{"commitInfo":{"timestamp":1793815720259,"operation":"STREAMING UPDATE","operationParameters":{"outputMode":"Append","queryId":"cd5b883e-d942-4f15-a59a-b4132b6a127f","epochId":"11"},"readVersion":10,"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{"numRemovedFiles":"0","numAddedFiles":"3","numOutputRows":"224","numOutputBytes":"20982"},"engineInfo":"Apache-Spark/4.1.0 Delta-Lake/4.1.0","txnId":"b1c1eb81-7e28-4a28-abdf-31254ecaf1dc"}}
{"txn":{"appId":"cd5b883e-d942-4f15-a59a-b4132b6a127f","version":11,"lastUpdated":1793815720234}}
{"add":{"path":"part-00000-04a0f1f4-80cf-40ae-8218-268b783bcbfe-c000.snappy.parquet","partitionValues":{},"size":7880,"modificationTime":1793815720163,"dataChange":true,"stats":"{\"numRecords\":92,\"minValues\":{\"kafka_key\":\"U003\",\"value\":\"{\\\"event_id\\\": \\\"0372989f-03bf-4d14\",\"kafka_partition\":2,\"kafka_offset\":728,\"kafka_timestamp\":\"2026-11-04T18:08:24.052Z\",\"ingest_time\":\"2026-11-04T18:08:32.674Z\"},\"maxValues\":{\"kafka_key\":\"U050\",\"value\":\"{\\\"event_id\\\": \\\"ff19a117-d643-4249\",\"kafka_partition\":2,\"kafka_offset\":819,\"kafka_timestamp\":\"2026-11-04T18:08:24.064Z\",\"ingest_time\":\"2026-11-04T18:08:32.674Z\"},\"nullCount\":{\"kafka_key\":0,\"value\":0,\"kafka_partition\":0,\"kafka_offset\":0,\"kafka_timestamp\":0,\"ingest_time\":0}}"}}
{"add":{"path":"part-00001-e7cdf467-5cdb-4512-8570-ec9339677dbf-c000.snappy.parquet","partitionValues":{},"size":7883,"modificationTime":1793815720163,"dataChange":true,"stats":"{\"numRecords\":89,\"minValues\":{\"kafka_key\":\"U001\",\"value\":\"{\\\"event_id\\\": \\\"00922246-25f1-4564\",\"kafka_partition\":0,\"kafka_offset\":800,\"kafka_timestamp\":\"2026-11-04T18:08:24.052Z\",\"ingest_time\":\"2026-11-04T18:08:32.674Z\"},\"maxValues\":{\"kafka_key\":\"U048\",\"value\":\"{\\\"event_id\\\": \\\"fff7df38-2405-40a6\",\"kafka_partition\":0,\"kafka_offset\":888,\"kafka_timestamp\":\"2026-11-04T18:08:24.064Z\",\"ingest_time\":\"2026-11-04T18:08:32.674Z\"},\"nullCount\":{\"kafka_key\":0,\"value\":0,\"kafka_partition\":0,\"kafka_offset\":0,\"kafka_timestamp\":0,\"ingest_time\":0}}"}}
{"add":{"path":"part-00002-0f1b7604-ba05-421f-bb76-370a2373f1e8-c000.snappy.parquet","partitionValues":{},"size":5219,"modificationTime":1793815720167,"dataChange":true,"stats":"{\"numRecords\":43,\"minValues\":{\"kafka_key\":\"U004\",\"value\":\"{\\\"event_id\\\": \\\"0c255e32-bf32-46de\",\"kafka_partition\":1,\"kafka_offset\":449,\"kafka_timestamp\":\"20
```

**checkpoints/silver/offsets/22**

```
v1
{"batchWatermarkMs":1768472695000,"batchTimestampMs":1793815732970,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"4","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
{"sourceVersion":1,"reservoirId":"fb7ccb68-0846-4933-a9ae-db05bf9fc583","reservoirVersion":12,"index":-1,"isStartingVersion":false}
```

**checkpoints/silver/offsets/23**

```
v1
{"batchWatermarkMs":1768474800000,"batchTimestampMs":1793815743404,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"4","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
{"sourceVersion":1,"reservoirId":"fb7ccb68-0846-4933-a9ae-db05bf9fc583","reservoirVersion":12,"index":-1,"isStartingVersion":false}
```

**checkpoints/silver/commits/22**

```
v1
{"nextBatchWatermarkMs":1768474800000}
```

**checkpoints/silver/commits/23**

```
v1
{"nextBatchWatermarkMs":1768474800000}
```

**lakehouse/silver/_delta_log/00000000000000000012.json**

```
{"commitInfo":{"timestamp":1793815742869,"operation":"MERGE","operationParameters":{"predicate":"[\"(event_id#130 = event_id#14)\"]","matchedPredicates":"[]","notMatchedPredicates":"[{\"actionType\":\"insert\"}]","notMatchedBySourcePredicates":"[]"},"readVersion":11,"isolationLevel":"Serializable","isBlindAppend":false,"operationMetrics":{"numTargetRowsCopied":"0","numTargetRowsDeleted":"0","numTargetBytesRemoved":"0","numTargetDeletionVectorsAdded":"0","numTargetRowsMatchedUpdated":"0","numTargetRowsMatchedDeleted":"0","numTargetRowsUpdated":"0","numTargetChangeFilesAdded":"0","numTargetRowsNotMatchedBySourceDeleted":"0","rewriteTimeMs":"5333","numTargetFilesAdded":"4","numTargetBytesAdded":"23185","executionTimeMs":"5361","materializeSourceTimeMs":"1","numTargetRowsInserted":"204","numTargetDeletionVectorsUpdated":"0","scanTimeMs":"0","numOutputRows":"204","numTargetDeletionVectorsRemoved":"0","numTargetRowsNotMatchedBySourceUpdated":"0","numSourceRows":"204","numTargetFilesRemoved":"0"},"engineInfo":"Apache-Spark/4.1.0 Delta-Lake/4.1.0","txnId":"6b48cf24-a3eb-4ad3-b07d-7c17e8dbc3e3"}}
{"add":{"path":"part-00000-1da39453-0965-4fc8-a782-39ee2ab64687-c000.snappy.parquet","partitionValues":{},"size":5856,"modificationTime":1793815742739,"dataChange":true,"stats":"{\"numRecords\":52,\"minValues\":{\"event_id\":\"03b46420-e982-4559-8e5b-8ad1a196\",\"user_id\":\"U001\",\"event_type\":\"add_to_cart\",\"product_id\":\"P01\",\"amount\":169000,\"event_time\":\"2026-01-15T10:37:43.000Z\",\"kafka_partition\":0,\"kafka_offset\":452,\"kafka_timestamp\":\"2026-11-04T18:08:24.052Z\"},\"maxValues\":{\"event_id\":\"fff7df38-2405-40a6-b7d7-2017e43a\",\"user_id\":\"U048\",\"event_type\":\"view\",\"product_id\":\"P20\",\"amount\":487000,\"event_time\":\"2026-01-15T10:59:55.000Z\",\"kafka_partition\":2,\"kafka_offset\":886,\"kafka_timestamp\":\"2026-11-04T18:08:24.059Z\"},\"nullCount\":{\"event_id\":0,\"user_id\":0,\"event_type\":0,\"product_id\":0,\"amount\":45,\"event_time\":0,\"kafka_partition\":0,\"kafka_offset\":0,\"kafka_timestamp\":0}}"}}
{"add":{"path":"part-00001-507f07e8-1b25-4053-8002-b29cc459c4a7-c000.snappy.parquet","partitionValues":{},"size":5626,"modificationTime":1793815742739,"dataChange":true,"stats":"{\"numRecords\":48,\"minValues\":{\"event_id\":\"0372989f-03bf-4d14-b62f-9ff36cd9\",\"user_id\":\"U001\",\"event_type\":\"add_to_cart\",\"product_id\":\"P01\",\"amount\":66000,\"event_time\":\"2026-01-15T10:38:17.000Z\",\"kafka_partition\":0,\"kafka_offset\":
```

**checkpoints/gold/offsets/22**

```
v1
{"batchWatermarkMs":1768473895000,"batchTimestampMs":1793815755679,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"4","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
{"sourceVersion":1,"reservoirId":"467e9a63-109d-46c1-b447-6741605832f7","reservoirVersion":13,"index":-1,"isStartingVersion":false}
```

**checkpoints/gold/offsets/23**

```
v1
{"batchWatermarkMs":1768476000000,"batchTimestampMs":1793815763118,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"4","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
{"sourceVersion":1,"reservoirId":"467e9a63-109d-46c1-b447-6741605832f7","reservoirVersion":13,"index":-1,"isStartingVersion":false}
```

**checkpoints/gold/commits/22**

```
v1
{"nextBatchWatermarkMs":1768476000000}
```

**checkpoints/gold/commits/23**

```
v1
{"nextBatchWatermarkMs":1768476000000}
```

**lakehouse/gold/_delta_log/00000000000000000023.json**

```
{"commitInfo":{"timestamp":1793815765357,"operation":"STREAMING UPDATE","operationParameters":{"outputMode":"Append","queryId":"33c27ae4-84ed-477d-b6f6-708be25d0e39","epochId":"23"},"readVersion":22,"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{"numRemovedFiles":"0","numAddedFiles":"3","numOutputRows":"4","numOutputBytes":"4526"},"engineInfo":"Apache-Spark/4.1.0 Delta-Lake/4.1.0","txnId":"7f64547f-fad9-44d9-849b-b2b61c4739fe"}}
{"txn":{"appId":"33c27ae4-84ed-477d-b6f6-708be25d0e39","version":23,"lastUpdated":1793815765356}}
{"add":{"path":"part-00001-76185625-4729-47c2-ad30-07bcdcc40979-c000.snappy.parquet","partitionValues":{},"size":1273,"modificationTime":1793815765339,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"window_start\":\"2026-01-15T10:40:00.000Z\",\"window_end\":\"2026-01-15T10:45:00.000Z\",\"revenue\":3587000,\"orders\":12},\"maxValues\":{\"window_start\":\"2026-01-15T10:55:00.000Z\",\"window_end\":\"2026-01-15T11:00:00.000Z\",\"revenue\":5301000,\"orders\":20},\"nullCount\":{\"window_start\":0,\"window_end\":0,\"revenue\":0,\"orders\":0}}"}}
{"add":{"path":"part-00002-9d09cf52-944e-4fc0-b3f1-6b603d516e20-c000.snappy.parquet","partitionValues":{},"size":1341,"modificationTime":1793815765339,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"window_start\":\"2026-01-15T10:45:00.000Z\",\"window_end\":\"2026-01-15T10:50:00.000Z\",\"revenue\":4867000,\"orders\":19},\"maxValues\":{\"window_start\":\"2026-01-15T10:45:00.000Z\",\"window_end\":\"2026-01-15T10:50:00.000Z\",\"revenue\":4867000,\"orders\":19},\"nullCount\":{\"window_start\":0,\"window_end\":0,\"revenue\":0,\"orders\":0}}"}}
{"add":{"path":"part-00003-18cbbe2f-c4a7-45db-877c-9039d84720ab-c000.snappy.parquet","partitionValues":{},"size":1341,"modificationTime":1793815765335,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"window_start\":\"2026-01-15T10:50:00.000Z\",\"window_end\":\"2026-01-15T10:55:00.000Z\",\"revenue\":3291000,\"orders\":11},\"maxValues\":{\"window_start\":\"2026-01-15T10:50:00.000Z\",\"window_end\":\"2026-01-15T10:55:00.000Z\",\"revenue\":3291000,\"orders\":11},\"nullCount\":{\"window_start\":0,\"window_end\":0,\"revenue\":0,\"orders\":0}}"}}
```

Bằng chứng của thí nghiệm A, B (trước và sau khi chạy lại):

- `results/evidence/exp_a_after.txt`
- `results/evidence/exp_a_before.txt`
- `results/evidence/exp_b_after.txt`
- `results/evidence/exp_b_before.txt`

## 8. Bất thường và cách xử lý

### V1. Optimizer đẩy filter purchase xuống dưới watermark ở Gold (đã giải quyết, task 30)

- Hiện tượng: chạy Gold sau đợt 1, Spark báo watermark 09:53:59, mô phỏng tính 09:54:57. Event time lớn nhất trong Silver là 10:04:57 (một view), còn purchase lớn nhất là 10:03:59.
- Đã thử: in plan đã tối ưu của micro-batch có dữ liệu. Code viết `withWatermark` rồi mới `filter(event_type = 'purchase')`, nhưng plan tối ưu là `EventTimeWatermark` nằm TRÊN `Filter (event_type = purchase)`. Quy tắc `PushPredicateThroughNonJoin` của Spark đẩy mọi điều kiện tất định không tham chiếu cột event time xuống dưới `EventTimeWatermark`.
- Kết quả: lưu ý ở task 30 ("đặt withWatermark TRƯỚC khi lọc purchase") không đủ trên Spark 4.1.0. Watermark chỉ tính trên purchase, view đẩy watermark mất tác dụng.
- Cách xử lý: bỏ filter trước khi tổng hợp, đưa điều kiện purchase vào hàm tổng hợp (`sum(when(purchase, amount))`, `count(when(purchase, 1))`), rồi lọc `orders > 0` sau khi tổng hợp. Thiết kế giữ nguyên (chỉ tính purchase, watermark 10 phút, cửa sổ 5 phút). Sau khi sửa, watermark sau đợt 1 là 09:54:57, khớp mô phỏng. Hệ quả phụ: `numRowsDroppedByWatermark` giờ đếm cả dòng không phải purchase (xem task 35).

### V2. Xác nhận hai lưu ý của task 14 và việc chốt cửa sổ cuối (task 34)

- Hiện tượng cần xác nhận: (a) watermark tính trên mọi loại sự kiện, (b) Spark loại dòng theo `window_end <= watermark`, (c) cửa sổ cuối được chốt khi `availableNow` kết thúc ở đợt 12.
- Kết quả chạy thật: (a) đúng sau khi sửa V1, watermark Gold sau từng đợt khớp mô phỏng. (b) đúng: doanh thu, số đơn và đợt chốt của cả 12 cửa sổ khớp mô phỏng theo `window_end`. (c) đúng: mỗi lần chạy `availableNow` gồm hai micro-batch (một có dữ liệu, một không dữ liệu để dọn state theo watermark mới), nên 4 cửa sổ cuối được ghi ngay trong lần chạy đợt 12.
- Không cần xử lý thêm.

### V3. numRowsDroppedByWatermark không bằng số sự kiện bị loại (task 35)

- Hiện tượng: theo đáp án vàng có 50 sự kiện quá ngưỡng (18 purchase), nhưng tổng `numRowsDroppedByWatermark` của Gold là 8.
- Nguyên nhân (đã dự đoán trước khi chạy từ physical plan, rồi đo khớp): metric đếm dòng ở đầu vào toán tử có trạng thái `StateStoreRestore`, nằm sau partial aggregation và shuffle. Mỗi cửa sổ quá muộn trong một micro-batch chỉ còn một dòng. Đáp án vàng ghi `late_windows_wm10` theo từng đợt (8 đợt, mỗi đợt 1 cửa sổ) và khớp từng đợt.
- Cách xử lý: `check_results.py` đối chiếu metric này với số cửa sổ quá muộn, và đo số purchase bị loại bằng cách so bảng Gold với đáp án tuyệt đối (18, khớp).

### V4. Độ trễ của nhóm muộn quá ngưỡng thực tế là 16 đến 20 phút (thiết kế dữ liệu, đã báo người dùng ở điểm dừng task 18)

- Hiện tượng: watermark chỉ cập nhật giữa các đợt 5 phút và Spark loại dòng theo `window_end <= watermark`, nên sự kiện ở cửa sổ thứ k chỉ bị loại khi được gửi từ đợt k+4 trở đi. Sự kiện trễ 12 đến 15 phút thường KHÔNG rơi vào cửa sổ đã chốt.
- Cách xử lý: bộ sinh chọn độ trễ trong [max(12, 20 − vị trí trong cửa sổ) + 5 giây, 20] phút, vẫn nằm trong 12 đến 20 phút của đặc tả. Độ trễ thực tế của 50 sự kiện là 16,04 đến 19,99 phút, thuộc 8 cửa sổ đầu (10:00 đến 10:40). Người dùng đã duyệt.

### V5. Thí nghiệm C: Silver không thay đổi dù Bronze gấp đôi (quan sát, không phải lỗi)

- Quan sát: Silver đọc 2201 dòng bản sao trong một micro-batch; 2200 dòng bị watermark Silver (11:00) loại như dữ liệu quá muộn, 1 dòng (sự kiện đẩy watermark 11:30) bị loại vì event_id còn trong state. foreachBatch nhận batch rỗng, Silver giữ nguyên 2001 dòng. Chi tiết trong `results/exp_c.json`.

## 9. Kiểm tra đầu cuối

`verify/check_results.py`: KHỚP (68 dòng đối chiếu, 0 dòng sai).

