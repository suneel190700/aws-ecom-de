import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.common.http_utils import build_session
from src.common.s3_utils import put_json


def ingest_list_endpoint_to_s3(endpoint_url: str, dataset_name: str) -> str:
    """
    Fetches a list JSON response from endpoint_url and writes it to S3 bronze
    partitioned by ingest_date.

    Returns the S3 URI of the written object.
    """
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise SystemExit("Set S3_BUCKET env var to your target bucket name.")

    now = datetime.now(timezone.utc)
    ingest_date = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    session = build_session()
    resp = session.get(endpoint_url, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"Expected list JSON from {endpoint_url}, got {type(data)}")

    payload: Dict[str, Any] = {
        "meta": {
            "source": "fakestoreapi",
            "endpoint": endpoint_url,
            "dataset": dataset_name,
            "ingest_ts_utc": now.isoformat(),
            "record_count": len(data),
        },
        "data": data,
    }

    key = f"bronze/api/{dataset_name}/ingest_date={ingest_date}/{dataset_name}_{ts}.json"
    put_json(bucket=bucket, key=key, payload=payload)

    return f"s3://{bucket}/{key}"
