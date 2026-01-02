from src.ingestion.ingest_dataset import ingest_list_endpoint_to_s3
from src.common.watermark import write_watermark

BASE = "https://fakestoreapi.com"
URL = f"{BASE}/users"  # customers-like

if __name__ == "__main__":
    out = ingest_list_endpoint_to_s3(URL, "customers")
    wm = write_watermark("customers")
    print(f"SUCCESS customers -> {out}")
    print(f"UPDATED watermark -> {wm}")
