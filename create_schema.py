import os

import requests
from dotenv import load_dotenv

load_dotenv()

STATEMENTS = [
    "DROP TABLE IF EXISTS connection_test",
    """
    CREATE TABLE IF NOT EXISTS stations (
        station_id   INTEGER PRIMARY KEY,
        state        TEXT NOT NULL,
        city         TEXT NOT NULL,
        station_name TEXT NOT NULL,
        latitude     REAL,
        longitude    REAL,
        UNIQUE (state, city, station_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS readings (
        station_id   INTEGER NOT NULL REFERENCES stations (station_id),
        pollutant_id TEXT    NOT NULL,
        observed_at  TEXT    NOT NULL,
        min_value    REAL    NOT NULL,
        max_value    REAL    NOT NULL,
        avg_value    REAL    NOT NULL,
        retrieved_at TEXT    NOT NULL,
        PRIMARY KEY (station_id, pollutant_id, observed_at)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_readings_time ON readings (observed_at)",
    """
    CREATE TABLE IF NOT EXISTS ingestion_runs (
        run_id          INTEGER PRIMARY KEY,
        started_at      TEXT NOT NULL,
        finished_at     TEXT,
        status          TEXT NOT NULL,
        total_reported  INTEGER,
        rows_fetched    INTEGER,
        rows_inserted   INTEGER,
        rows_skipped_na INTEGER,
        error_message   TEXT
    )
    """,
    "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
]


def main() -> None:
    db_url = os.getenv("TURSO_DATABASE_URL", "").strip()
    token = os.getenv("TURSO_AUTH_TOKEN", "").strip()
    if not db_url or not token:
        print("Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN in .env")
        return

    endpoint = db_url.replace("libsql://", "https://", 1).rstrip("/") + "/v2/pipeline"
    requests_body = [{"type": "execute", "stmt": {"sql": sql}} for sql in STATEMENTS]
    requests_body.append({"type": "close"})
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        response = requests.post(
            endpoint, json={"requests": requests_body}, headers=headers, timeout=(10, 30)
        )
    except requests.exceptions.RequestException as exc:
        print("Request failed:", type(exc).__name__)
        print("Message:", str(exc).replace(token, "***REDACTED***"))
        return

    print("HTTP status:", response.status_code)
    if response.status_code != 200:
        print("Body:", response.text[:300].replace(token, "***REDACTED***"))
        return

    results = response.json()["results"]
    for sql, result in zip(STATEMENTS, results):
        label = " ".join(sql.split())[:60]
        if result.get("type") == "error":
            print(f"ERROR  {label} -> {result['error'].get('message')}")
        else:
            print(f"ok     {label}")

    last = results[len(STATEMENTS) - 1]
    if last.get("type") == "ok":
        print("\nTables now in the database:")
        for row in last["response"]["result"]["rows"]:
            print("  ", row[0]["value"])


if __name__ == "__main__":
    main()