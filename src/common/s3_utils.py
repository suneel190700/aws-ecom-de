import json
from typing import Any, Dict

import boto3


def put_json(bucket: str, key: str, payload: Dict[str, Any]) -> None:
    """
    Writes a JSON payload to S3 at s3://bucket/key

    bucket: S3 bucket name
    key: S3 object key (path)
    payload: Python dict to serialize to JSON
    """
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
