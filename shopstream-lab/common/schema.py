"""Schema JSON của sự kiện ShopStream (value của message Kafka).

event_time để dạng chuỗi ISO 8601; Silver mới chuyển sang timestamp,
nhờ đó lọc được bản ghi có event_time không hợp lệ.
"""

from pyspark.sql.types import LongType, StringType, StructField, StructType

EVENT_FIELDS = [
    "event_id",
    "user_id",
    "event_type",
    "product_id",
    "amount",
    "event_time",
]

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        # Đơn vị đồng, chỉ purchase có giá trị, các loại khác là null
        StructField("amount", LongType(), True),
        StructField("event_time", StringType(), True),
    ]
)
