import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://fakestoreapi.com"
ORDERS_URL = f"{BASE_URL}/carts"


def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def fetch_orders() -> List[Dict[str, Any]]:
    s = build_session()
    r = s.get(ORDERS_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"Expected list JSON, got {type(data)}")
    return data


def put_json(bucket: str, key: str, payload: Dict[str, Any]) -> None:
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def main() -> None:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise SystemExit("Set S3_BUCKET env var to your target bucket name.")

    now = datetime.now(timezone.utc)
    ingest_date = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    orders = fetch_orders()

    payload = {
        "meta": {
            "source": "fakestoreapi",
            "endpoint": ORDERS_URL,
            "ingest_ts_utc": now.isoformat(),
            "record_count": len(orders),
        },
        "data": orders,
    }

    key = f"bronze/api/orders/ingest_date={ingest_date}/orders_{ts}.json"
    put_json(bucket=bucket, key=key, payload=payload)

    print(f"SUCCESS: wrote {len(orders)} records to s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
