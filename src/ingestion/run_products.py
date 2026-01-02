from src.ingestion.ingest_dataset import ingest_list_endpoint_to_s3
from src.common.watermark import write_watermark

BASE = "https://fakestoreapi.com"
URL = f"{BASE}/products"

if __name__ == "__main__":
    out = ingest_list_endpoint_to_s3(URL, "products")
    wm = write_watermark("products")
    print(f"SUCCESS products -> {out}")
    print(f"UPDATED watermark -> {wm}")
