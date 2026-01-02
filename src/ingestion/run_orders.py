from src.ingestion.ingest_dataset import ingest_list_endpoint_to_s3
from src.common.watermark import write_watermark

BASE = "https://fakestoreapi.com"
URL = f"{BASE}/carts"  # orders-like

if __name__ == "__main__":
    out = ingest_list_endpoint_to_s3(URL, "orders")
    wm = write_watermark("orders")
    print(f"SUCCESS orders -> {out}")
    print(f"UPDATED watermark -> {wm}")
