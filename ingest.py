import argparse
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

DATAGOV_URL = "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
RAW_DIR = Path("data") / "raw"
IST = timezone(timedelta(hours=5, minutes=30))  # ASSUMPTION: source times are IST
SOURCE_TIME_FORMAT = "%d-%m-%Y %H:%M:%S"
POLLUTANT_NAMES = {"OZONE": "O3"}
VALUE_COLUMNS = ("min_value", "max_value", "avg_value")
PAGE_LIMIT = 500
MAX_PAGES = 50
CHUNK_SIZE = 250
MAX_ATTEMPTS = 3

STATION_SQL = (
    "INSERT OR IGNORE INTO stations (state, city, station_name, latitude, longitude) "
    "VALUES (?, ?, ?, ?, ?)"
)
READING_SQL = (
    "INSERT OR IGNORE INTO readings "
    "(station_id, pollutant_id, observed_at, min_value, max_value, avg_value, retrieved_at) "
    "VALUES ((SELECT station_id FROM stations "
    "WHERE state = ? AND city = ? AND station_name = ?), ?, ?, ?, ?, ?, ?)"
)
RUN_SQL = (
    "INSERT INTO ingestion_runs (started_at, finished_at, status, total_reported, "
    "rows_fetched, rows_inserted, rows_skipped_na, error_message) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("raysai")


# ---------- small helpers ----------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def redact(text: str) -> str:
    """Remove secrets from any text before it is logged or stored."""
    for name in ("TURSO_AUTH_TOKEN", "DATAGOV_API_KEY"):
        secret = os.getenv(name)
        if secret:
            text = text.replace(secret, "***REDACTED***")
    return text


def is_transient(exc: Exception) -> bool:
    """Retry network problems and 5xx/429 responses, but not 4xx like a bad token."""
    if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return True


def with_retries(action, description: str):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return action()
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_ATTEMPTS or not is_transient(exc):
                raise
            wait = 2 ** attempt
            log.warning("%s failed (%s); retrying in %ss", description, type(exc).__name__, wait)
            time.sleep(wait)


def to_number(text):
    """Return a finite float, or None if the text is not a usable number."""
    try:
        value = float(str(text).strip())
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def chunks(items: list, size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


# ---------- getting the raw data ----------
def fetch_all_pages(api_key: str) -> list[dict]:
    headers = {"Accept": "application/json", "User-Agent": "RaysAI/0.1 (learning project)"}
    pages: list[dict] = []
    offset = 0
    for _ in range(MAX_PAGES):
        params = {"api-key": api_key, "format": "json", "offset": offset, "limit": PAGE_LIMIT}

        def get_page():
            response = requests.get(DATAGOV_URL, params=params, headers=headers, timeout=(10, 30))
            response.raise_for_status()
            return response.json()

        data = with_retries(get_page, f"fetch offset={offset}")
        if "records" not in data or "total" not in data:
            raise ValueError("Unexpected API response structure (no 'records' or 'total')")
        pages.append({"offset": offset, "limit": PAGE_LIMIT, "response": data})
        received = len(data["records"])
        log.info("offset=%s: received %s records (total reported %s)", offset, received, data["total"])
        offset += received
        if received == 0 or offset >= int(data["total"]):
            break
        time.sleep(1.0)  # be polite to the API
    return pages


def save_raw_snapshot(pages: list[dict], retrieved_at: str) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RAW_DIR / f"aqi_snapshot_{stamp}.json"
    payload = {"retrieved_at_utc": retrieved_at, "pages": pages}
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    log.info("Saved raw snapshot: %s", path)


def load_latest_snapshot() -> dict:
    files = sorted(RAW_DIR.glob("aqi_snapshot_*.json"))
    if not files:
        raise FileNotFoundError("No snapshot found in data/raw")
    log.info("Using saved snapshot: %s", files[-1].name)
    return json.loads(files[-1].read_text(encoding="utf-8"))


# ---------- cleaning ----------
def clean_records(raw_records: list[dict]):
    """Return (readings, stations, stats). Missing values are skipped, never turned into 0."""
    readings: dict[tuple, tuple] = {}
    stations: dict[tuple, tuple] = {}
    stats = {"skipped_na": 0, "skipped_invalid": 0, "duplicates": 0, "conflicts": 0}

    for record in raw_records:
        raw_values = [str(record.get(col, "")).strip() for col in VALUE_COLUMNS]
        if all(value.upper() == "NA" for value in raw_values):
            stats["skipped_na"] += 1
            continue
        try:
            state = record["state"].strip()
            city = record["city"].strip()
            station = record["station"].strip()
            raw_pollutant = record["pollutant_id"].strip()
            pollutant = POLLUTANT_NAMES.get(raw_pollutant, raw_pollutant)
            observed = (
                datetime.strptime(record["last_update"].strip(), SOURCE_TIME_FORMAT)
                .replace(tzinfo=IST)
                .isoformat()
            )
        except (KeyError, AttributeError, ValueError):
            stats["skipped_invalid"] += 1
            continue

        values = tuple(to_number(value) for value in raw_values)
        if None in values or not (state and city and station and pollutant):
            stats["skipped_invalid"] += 1
            continue

        key = (state, city, station, pollutant, observed)
        if key in readings:
            stats["duplicates"] += 1
            if readings[key] != values:
                stats["conflicts"] += 1
            continue
        readings[key] = values
        stations.setdefault(
            (state, city, station),
            (to_number(record.get("latitude")), to_number(record.get("longitude"))),
        )

    return readings, stations, stats


# ---------- talking to Turso ----------
def arg(value) -> dict:
    """Convert a Python value to Turso's typed argument format."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, float):
        return {"type": "float", "value": value}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    return {"type": "text", "value": str(value)}


def run_pipeline(statements: list[dict]) -> list[dict]:
    """Send statements to Turso in one request. Raises if any statement fails."""
    db_url = os.environ["TURSO_DATABASE_URL"].strip()
    token = os.environ["TURSO_AUTH_TOKEN"].strip()
    endpoint = db_url.replace("libsql://", "https://", 1).rstrip("/") + "/v2/pipeline"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "requests": [{"type": "execute", "stmt": s} for s in statements] + [{"type": "close"}]
    }

    def post():
        response = requests.post(endpoint, json=body, headers=headers, timeout=(10, 60))
        response.raise_for_status()
        return response.json()

    data = with_retries(post, "turso request")
    results = []
    for result in data["results"][: len(statements)]:
        if result.get("type") == "error":
            raise RuntimeError("Turso statement failed: " + result["error"].get("message", "?"))
        results.append(result["response"]["result"])
    return results


def count_affected(results: list[dict]) -> int:
    return sum(result.get("affected_row_count", 0) for result in results)


def load_to_turso(stations: dict, readings: dict, retrieved_at: str) -> tuple[int, int]:
    station_statements = [
        {"sql": STATION_SQL, "args": [arg(s), arg(c), arg(n), arg(lat), arg(lon)]}
        for (s, c, n), (lat, lon) in stations.items()
    ]
    reading_statements = [
        {
            "sql": READING_SQL,
            "args": [arg(s), arg(c), arg(n), arg(p), arg(t),
                     arg(mn), arg(mx), arg(av), arg(retrieved_at)],
        }
        for (s, c, n, p, t), (mn, mx, av) in readings.items()
    ]

    new_stations = new_readings = 0
    for batch in chunks(station_statements, CHUNK_SIZE):
        new_stations += count_affected(run_pipeline(batch))
    for number, batch in enumerate(chunks(reading_statements, CHUNK_SIZE), start=1):
        new_readings += count_affected(run_pipeline(batch))
        log.info("Loaded reading batch %s (%s new readings so far)", number, new_readings)
    return new_stations, new_readings


def table_count(table: str) -> int:
    result = run_pipeline([{"sql": f"SELECT COUNT(*) FROM {table}"}])[0]
    return int(result["rows"][0][0]["value"])


def log_run(started_at, status, total_reported, rows_fetched, rows_inserted, skipped_na, error):
    args = [arg(started_at), arg(utc_now_iso()), arg(status), arg(total_reported),
            arg(rows_fetched), arg(rows_inserted), arg(skipped_na), arg(error)]
    run_pipeline([{"sql": RUN_SQL, "args": args}])


# ---------- main ----------
def main() -> None:
    parser = argparse.ArgumentParser(description="RaysAI ingestion")
    parser.add_argument("--snapshot", action="store_true",
                        help="load the latest saved raw snapshot instead of calling the API")
    options = parser.parse_args()

    if not os.getenv("TURSO_DATABASE_URL") or not os.getenv("TURSO_AUTH_TOKEN"):
        log.error("Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN in .env")
        raise SystemExit(1)

    started_at = utc_now_iso()
    status = "failed"
    total_reported = rows_fetched = None
    inserted = skipped_na = 0
    error = None

    try:
        if options.snapshot:
            payload = load_latest_snapshot()
            pages = payload["pages"]
            retrieved_at = payload["retrieved_at_utc"]
        else:
            api_key = os.getenv("DATAGOV_API_KEY", "").strip()
            if not api_key:
                raise RuntimeError("Missing DATAGOV_API_KEY in .env (needed for live mode)")
            pages = fetch_all_pages(api_key)
            retrieved_at = utc_now_iso()
            save_raw_snapshot(pages, retrieved_at)

        raw_records = [r for page in pages for r in page["response"]["records"]]
        total_reported = int(pages[-1]["response"]["total"])
        rows_fetched = len(raw_records)
        if rows_fetched != total_reported:
            log.warning("Fetched %s rows but API reported %s", rows_fetched, total_reported)

        readings, stations, stats = clean_records(raw_records)
        skipped_na = stats["skipped_na"]
        log.info("Cleaning: %s readings kept, %s", len(readings), stats)
        accounted = len(readings) + sum(stats[k] for k in
                                        ("skipped_na", "skipped_invalid", "duplicates"))
        log.info("Reconciliation: fetched %s, accounted for %s", rows_fetched, accounted)

        observed_times = sorted({key[4] for key in readings})
        if observed_times:
            log.info("Observation times in this fetch: %s to %s", observed_times[0], observed_times[-1])

        new_stations, inserted = load_to_turso(stations, readings, retrieved_at)
        log.info("Inserted %s new stations and %s new readings", new_stations, inserted)
        status = "snapshot_load" if options.snapshot else "success"

        log.info("Database totals: %s stations, %s readings",
                 table_count("stations"), table_count("readings"))
    except Exception as exc:  # report every failure, but never leak secrets
        error = redact(f"{type(exc).__name__}: {exc}")
        log.error("Ingestion failed: %s", error)

    try:
        log_run(started_at, status, total_reported, rows_fetched, inserted, skipped_na, error)
        log.info("Run recorded with status: %s", status)
    except Exception as exc:
        log.error("Could not record the run: %s", redact(str(exc)))


if __name__ == "__main__":
    main()