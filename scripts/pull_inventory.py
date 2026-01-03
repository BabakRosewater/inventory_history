#!/usr/bin/env python3
import csv
import datetime as dt
import gzip
import hashlib
import io
import pathlib
import urllib.request

FEED_URLS = [
    # Prefer HTTP because HTTPS cert is currently invalid in your screenshot
    "http://dtfeed.camclarkautogroup.com/ftp/MP16607.csv",
    # Keep HTTPS as fallback (may fail until cert is fixed)
    "https://dtfeed.camclarkautogroup.com/ftp/MP16607.csv",
]

LATEST_PATH = pathlib.Path("data/latest/MP16607.csv")
SNAPSHOT_ROOT = pathlib.Path("data/snapshots")
MANIFEST_PATH = pathlib.Path("data/manifest.csv")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def fetch_feed() -> tuple[str, bytes]:
    last_err = None
    for url in FEED_URLS:
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "inventory-history-bot"},
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=60) as r:
                data = r.read()
            if not data:
                raise RuntimeError("Empty response")
            return url, data
        except Exception as e:
            last_err = e
    raise SystemExit(f"Feed download failed. Last error: {last_err}")


def count_csv_rows(data: bytes) -> int:
    # “Good enough” row count for manifest (not used for logic)
    text = data.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    n = 0
    for _ in reader:
        n += 1
    return n


def ensure_dirs():
    LATEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_ROOT.mkdir(parents=True, exist_ok=True)


def read_latest_sha() -> str | None:
    if not LATEST_PATH.exists():
        return None
    return sha256_bytes(LATEST_PATH.read_bytes())


def append_manifest(row: dict):
    is_new = not MANIFEST_PATH.exists()
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp_utc",
        "url_used",
        "sha256",
        "bytes",
        "csv_rows_including_header",
        "latest_path",
        "snapshot_path",
    ]

    with MANIFEST_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if is_new:
            w.writeheader()
        w.writerow(row)


def main():
    ensure_dirs()

    url_used, data = fetch_feed()
    new_sha = sha256_bytes(data)
    old_sha = read_latest_sha()

    # If nothing changed, do nothing (avoids repo bloat)
    if old_sha == new_sha:
        print("No change detected. Exiting.")
        return

    # Write latest (uncompressed so your app can load it easily)
    LATEST_PATH.write_bytes(data)

    # Snapshot path: data/snapshots/YYYY-MM-DD/MP16607_YYYYMMDD_HHMMSSZ.csv.gz
    now = dt.datetime.utcnow().replace(microsecond=0)
    day_dir = SNAPSHOT_ROOT / now.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)

    ts = now.strftime("%Y%m%d_%H%M%SZ")
    snap_name = f"MP16607_{ts}.csv.gz"
    snap_path = day_dir / snap_name

    with gzip.open(snap_path, "wb", compresslevel=9) as gz:
        gz.write(data)

    rows = count_csv_rows(data)

    append_manifest(
        {
            "timestamp_utc": now.isoformat() + "Z",
            "url_used": url_used,
            "sha256": new_sha,
            "bytes": str(len(data)),
            "csv_rows_including_header": str(rows),
            "latest_path": str(LATEST_PATH.as_posix()),
            "snapshot_path": str(snap_path.as_posix()),
        }
    )

    print(f"Updated latest + wrote snapshot: {snap_path}")


if __name__ == "__main__":
    main()
