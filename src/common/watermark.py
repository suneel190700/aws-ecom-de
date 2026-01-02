import os
from datetime import datetime, timezone
from typing import Any, Dict

from src.common.s3_utils import put_json


def write_watermark(dataset: str) -> str:
    """
    Writes/updates a watermark file in S3.
    This is a simple 'last run time' marker.
    """
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise SystemExit("Set S3_BUCKET env var to your target bucket name.")

    now = datetime.now(timezone.utc)
    payload: Dict[str, Any] = {
        "dataset": dataset,
        "updated_ts_utc": now.isoformat(),
    }

    key = f"metadata/watermarks/{dataset}.json"
    put_json(bucket=bucket, key=key, payload=payload)

    return f"s3://{bucket}/{key}"
