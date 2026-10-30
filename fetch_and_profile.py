import getpass
import json
import math
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
RAW_DIR = Path("data") / "raw"
PAGE_LIMIT = 500
PAUSE_SECONDS = 1.0   # be polite: wait between requests
MAX_PAGES = 50        # safety stop so a bug can never loop forever
TIMESTAMP_FORMAT = "%d-%m-%Y %H:%M:%S"
NUMERIC_COLUMNS = ("latitude", "longitude", "min_value", "max_value", "avg_value")


def fetch_all_pages(api_key: str) -> list[dict]:
    """Download every page. Returns a list of {offset, limit, response} dicts."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "RaysAI/0.1 (learning project)",
    }
    pages: list[dict] = []
    offset = 0

    for _ in range(MAX_PAGES):
        params = {"api-key": api_key, "format": "json", "offset": offset, "limit": PAGE_LIMIT}
        response = requests.get(URL, params=params, headers=headers, timeout=(10, 30))
        response.raise_for_status()
        data = response.json()

        records = data.get("records", [])
        total = int(data["total"])
        pages.append({"offset": offset, "limit": PAGE_LIMIT, "response": data})
        print(f"offset={offset}: received {len(records)} records (total reported: {total})")

        if not records:
            break
        offset += len(records)  # advance by what we actually received
        if offset >= total:
            break
        time.sleep(PAUSE_SECONDS)

    return pages


def classify(value) -> str:
    """Label a raw string value: numeric, NA, empty, non_numeric or non_finite."""
    if value is None or str(value).strip() == "":
        return "empty"
    text = str(value).strip()
    if text.upper() == "NA":
        return "NA"
    try:
        number = float(text)
    except ValueError:
        return "non_numeric"
    return "numeric" if math.isfinite(number) else "non_finite"


def profile(records: list[dict], total_reported: int) -> None:
    print("\n=== PROFILE ===")
    print(f"Rows fetched: {len(records)} | total reported by API: {total_reported}")

    print("\nPollutant IDs:")
    for name, count in Counter(r.get("pollutant_id") for r in records).most_common():
        print(f"  {name}: {count}")

    print("\nDistinct states:", len({r.get("state") for r in records}))
    print("Distinct cities:", len({(r.get("state"), r.get("city")) for r in records}))
    print("Distinct stations:", len({(r.get("city"), r.get("station")) for r in records}))

    print("\nValue quality per column:")
    for column in NUMERIC_COLUMNS:
        counts = Counter(classify(r.get(column)) for r in records)
        print(f"  {column}: {dict(counts)}")

    numbers = [float(r["avg_value"]) for r in records if classify(r.get("avg_value")) == "numeric"]
    if numbers:
        print(f"\navg_value numeric range: {min(numbers)} to {max(numbers)}")
        print(f"avg_value negatives: {sum(1 for n in numbers if n < 0)}")

    parsed: list[datetime] = []
    bad_timestamps = 0
    for r in records:
        try:
            parsed.append(datetime.strptime(r.get("last_update", ""), TIMESTAMP_FORMAT))
        except ValueError:
            bad_timestamps += 1
    print(f"\nTimestamps: {len(parsed)} parsed, {bad_timestamps} unparseable")
    if parsed:
        print(f"  oldest last_update: {min(parsed)}")
        print(f"  newest last_update: {max(parsed)}")
    print("  most common last_update values:")
    for value, count in Counter(r.get("last_update") for r in records).most_common(8):
        print(f"    {value}: {count}")

    keys = Counter(
        (r.get("state"), r.get("city"), r.get("station"), r.get("pollutant_id"), r.get("last_update"))
        for r in records
    )
    duplicates = sum(1 for count in keys.values() if count > 1)
    print(f"\nDuplicate (station, pollutant, last_update) keys: {duplicates}")


def main() -> None:
    api_key = getpass.getpass("Paste your data.gov.in API key (hidden): ").strip()
    try:
        pages = fetch_all_pages(api_key)
    except (requests.exceptions.RequestException, ValueError, KeyError) as exc:
        print("Fetch failed:", type(exc).__name__)
        print("Message:", str(exc).replace(api_key, "***REDACTED***"))
        return

    retrieved_at = datetime.now(timezone.utc)
    stamp = retrieved_at.strftime("%Y%m%dT%H%M%SZ")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / f"aqi_snapshot_{stamp}.json"
    payload = {"retrieved_at_utc": retrieved_at.isoformat(), "pages": pages}
    out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print("\nSaved raw snapshot to:", out_path)

    records = [record for page in pages for record in page["response"].get("records", [])]
    total_reported = int(pages[-1]["response"]["total"]) if pages else 0
    profile(records, total_reported)


if __name__ == "__main__":
    main()