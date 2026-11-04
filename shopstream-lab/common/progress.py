"""Lưu tiến trình (progress) của một truy vấn streaming ra file JSON."""

import json
from pathlib import Path


def progress_to_dict(progress):
    """PySpark 4 trả về StreamingQueryProgress; lấy bản JSON gốc của Spark cho chắc chắn."""
    if hasattr(progress, "json"):
        return json.loads(progress.json)
    return dict(progress)


def save_query_progress(query, progress_file, query_name, row_count, extra=None):
    """Ghi mọi micro-batch trong recentProgress của lần chạy này, kèm số dòng của bảng đích."""
    if progress_file is None:
        return None
    micro_batches = [progress_to_dict(progress) for progress in query.recentProgress]
    record = {
        "query": query_name,
        "row_count": row_count,
        # availableNow có thể sinh thêm micro-batch không dữ liệu (để dọn state theo watermark)
        "micro_batch_count": len(micro_batches),
        "micro_batch_ids": [progress["batchId"] for progress in micro_batches],
        "progress": micro_batches,
    }
    if extra:
        record.update(extra)
    progress_file = Path(progress_file)
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    progress_file.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return record
