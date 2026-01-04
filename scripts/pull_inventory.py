#!/usr/bin/env python3
"""
pull_inventory.py

Downloads the dtfeed CSV and writes:
- data/latest/MP16607.csv              (latest raw)
- data/snapshots/YYYY-MM-DD/*.csv.gz   (timestamped snapshots when content changes)
- data/manifest.csv                    (append-only log)

It tries HTTPS first, then HTTP. If HTTPS has a cert problem, you can allow
insecure HTTPS by setting env var:
  DTFEED_INSECURE_HTTPS=1
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import hashlib
import io
import os
import pathlib
import ssl
import urllib.request
from typing import Tuple


FEED_URLS = [
    "https://dtfeed.camclarkautogroup.com/ftp/MP16607.csv",
    "http://dtfeed.camclarkautogroup.com/ftp/MP16607.csv",
]

LATEST_PATH = pathlib.Path("data/latest/MP16607.csv")
SNAPSHOT_ROOT = pathlib.Path("data/snapshots")
MANIFEST_PATH = pathlib.Path("data/manifest.csv")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def count_csv_rows(data: bytes) -> int:
    # Handle BOM if present
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))
    return sum(1 for _ in reader)


def ensure_dirs() -> None:
    LATEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_ROOT.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)


def read_latest_sha() -> str | None:
    if not LATEST_PATH.exists():
        return None
    return sha256_bytes(LATEST_PATH.read_bytes())


def append_manifest(row: dict) -> None:
    is_new = not MANIFEST_PATH.exists()
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


def _download(url: str, *, allow_insecure_https: bool) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "inventory-history-bot"},
        method="GET",
    )

    if url.lower().startswith("https://") and allow_insecure_https:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
            return r.read()

    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def fetch_feed() -> Tuple[str, bytes]:
    insecure = os.environ.get("DTFEED_INSECURE_HTTPS", "").strip() in ("1", "true", "TRUE", "yes", "YES")
    last_err: Exception | None = None

    for url in FEED_URLS:
        try:
            data = _download(url, allow_insecure_https=insecure)
            if not data:
                raise RuntimeError("Empty response")
            return url, data
        except Exception as e:
            last_err = e

    raise SystemExit(f"Feed download failed. Last error: {last_err}")


def main() -> None:
    ensure_dirs()

    url_used, data = fetch_feed()
    new_sha = sha256_bytes(data)
    old_sha = read_latest_sha()

    if old_sha == new_sha:
        print("No change detected in raw feed.")
        return

    # Write latest raw
    LATEST_PATH.write_bytes(data)

    # Snapshot
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
