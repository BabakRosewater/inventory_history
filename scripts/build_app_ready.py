#!/usr/bin/env python3
"""
build_app_ready.py

Builds an app-friendly CSV while preserving ALL original feed columns.

Input:
- data/latest/MP16607.csv

Outputs:
- data/latest/app_ready.csv        (computed columns + ALL raw headers)
- data/latest/meta.json
- data/latest/delta.json
- data/state/first_seen.json

Env vars (optional):
  INP   = data/latest/MP16607.csv
  OUT   = data/latest/app_ready.csv
  STATE = data/state/first_seen.json
  META  = data/latest/meta.json
  DELTA = data/latest/delta.json
"""

from __future__ import annotations

import csv
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set


# These columns are added FIRST, then the 35 raw headers follow in their original order
COMPUTED_FIELDS: List[str] = [
    "key",                      # vin (preferred) else vehicle_id, uppercased
    "stock",                    # normalized stock number (from "Stock #", fallback to key)
    "trim",                     # normalized trim (from "Trim")
    "state_of_vehicle_norm",    # NEW / USED (CPO -> USED)
    "age_days",                 # feed Age (int) fallback to age_days_since_first_seen
    "age_days_since_first_seen",
    "price_usd",
    "sale_price_usd",
    "discount_usd",
    "image_url",                # primary photo chosen for the app
    "first_seen_utc",
]


def pick(row: Dict[str, Any], *names: str) -> str:
    for n in names:
        v = row.get(n)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    # Handles "32570 USD", "$32,570", "32570", etc.
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = re.sub(r"[^\d\-]", "", s)
    try:
        return int(s)
    except ValueError:
        return None


def norm_state(s: str) -> str:
    s = (s or "").strip().upper()
    if s in ("NEW", "N"):
        return "NEW"
    if s in ("USED", "U", "PREOWNED", "PRE-OWNED", "CPO"):
        return "USED"
    return s or ""


def load_json(path: str, default: Any) -> Any:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path: str, obj: Any) -> None:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def first_photo_from_list(photo_list: str) -> str:
    if not photo_list:
        return ""
    parts = [p.strip() for p in str(photo_list).split(",") if p.strip()]
    return parts[0] if parts else ""


def parse_first_seen_date(iso_ts: str, fallback_date):
    try:
        return datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).date()
    except Exception:
        return fallback_date


def main() -> None:
    INP = os.environ.get("INP", "data/latest/MP16607.csv")
    OUT = os.environ.get("OUT", "data/latest/app_ready.csv")
    STATE = os.environ.get("STATE", "data/state/first_seen.json")
    META = os.environ.get("META", "data/latest/meta.json")
    DELTA = os.environ.get("DELTA", "data/latest/delta.json")

    now = datetime.now(timezone.utc)
    today = now.date()

    if not os.path.exists(INP):
        raise SystemExit(f"Input not found: {INP}")

    # key -> ISO timestamp
    first_seen: Dict[str, str] = load_json(STATE, {})

    # Read prior OUT for delta comparisons (key + sale_price_usd)
    prev: Dict[str, Dict[str, Any]] = {}
    if os.path.exists(OUT):
        with open(OUT, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                k = (row.get("key") or "").strip()
                if k:
                    prev[k] = row

    rows_out: List[Dict[str, Any]] = []
    now_keys: Set[str] = set()

    # Read raw feed
    with open(INP, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        headers: List[str] = rdr.fieldnames or []

        if not headers:
            raise SystemExit("Input CSV has no headers.")

        # Validate keyability
        lower_headers = [h.lower() for h in headers]
        if "vin" not in lower_headers and "vehicle_id" not in lower_headers:
            raise SystemExit("No vin or vehicle_id column found; can’t key rows.")

        # This is the exact output column order:
        # computed columns first, then raw headers in their original order
        out_fields: List[str] = COMPUTED_FIELDS + headers

        for row in rdr:
            # Preserve ALL raw columns exactly by header name
            raw_map: Dict[str, Any] = {h: (row.get(h) if row.get(h) is not None else "") for h in headers}

            vin_raw = pick(row, "vin").strip().upper()
            vid_raw = pick(row, "vehicle_id").strip().upper()

            key = (vin_raw or vid_raw).strip()
            if not key:
                continue

            now_keys.add(key)

            if key not in first_seen:
                first_seen[key] = now.isoformat()

            fs_date = parse_first_seen_date(first_seen[key], today)
            age_days_since_first_seen = max(0, (today - fs_date).days)

            # Feed age (preferred for "age_days")
            feed_age = to_int(pick(row, "Age"))
            age_days = feed_age if feed_age is not None else age_days_since_first_seen

            price_usd = to_float(pick(row, "price"))
            sale_usd = to_float(pick(row, "sale_price"))

            discount_usd: Optional[float] = None
            if price_usd is not None and sale_usd is not None and price_usd > sale_usd:
                discount_usd = round(price_usd - sale_usd, 2)

            state_raw = pick(row, "state_of_vehicle", "condition", "availability")
            state_norm = norm_state(state_raw)

            stock = pick(row, "Stock #").strip()
            if not stock:
                stock = key

            trim = pick(row, "Trim", "trim").strip()

            image_url = pick(row, "image[0].url").strip()
            if not image_url:
                image_url = first_photo_from_list(pick(row, "Photo Url List"))

            # Build full output row
            out_row: Dict[str, Any] = {}

            # computed
            out_row["key"] = key
            out_row["stock"] = stock
            out_row["trim"] = trim
            out_row["state_of_vehicle_norm"] = state_norm
            out_row["age_days"] = age_days
            out_row["age_days_since_first_seen"] = age_days_since_first_seen
            out_row["price_usd"] = "" if price_usd is None else round(price_usd, 2)
            out_row["sale_price_usd"] = "" if sale_usd is None else round(sale_usd, 2)
            out_row["discount_usd"] = "" if (discount_usd is None or discount_usd <= 0) else discount_usd
            out_row["image_url"] = image_url
            out_row["first_seen_utc"] = first_seen[key]

            # raw columns appended
            out_row.update(raw_map)

            rows_out.append(out_row)

    # Stable sort for consistent output
    def sort_key(r: Dict[str, Any]):
        return (
            str(r.get("state_of_vehicle_norm", "")),
            str(r.get("model", "")),
            str(r.get("key", "")),
        )

    rows_out.sort(key=sort_key)

    # Ensure output folder exists
    out_dir = os.path.dirname(OUT)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # Recompute out_fields deterministically even if there are 0 rows
    # (we can re-open INP header to ensure we always write the same header list)
    with open(INP, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        hdrs = csv.DictReader(f).fieldnames or []
    out_fields = COMPUTED_FIELDS + hdrs

    # Write app_ready.csv with deterministic column order
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()
        for r in rows_out:
            w.writerow({k: r.get(k, "") for k in out_fields})

    # Delta based on key set difference + sale price changes
    added = sorted(now_keys - set(prev.keys()))
    removed = sorted(set(prev.keys()) - now_keys)

    cur_price = {r["key"]: to_float(r.get("sale_price_usd")) for r in rows_out}
    price_changed: List[str] = []
    for k in (now_keys & set(prev.keys())):
        old_p = to_float(prev[k].get("sale_price_usd"))
        new_p = cur_price.get(k)
        if old_p is not None and new_p is not None and abs(old_p - new_p) > 0.1:
            price_changed.append(k)

    save_json(
        DELTA,
        {
            "ts_utc": now.isoformat(),
            "added": added,
            "removed": removed,
            "price_changed": sorted(price_changed),
            "counts": {"now": len(now_keys), "prev": len(prev)},
        },
    )

    save_json(
        META,
        {
            "ts_utc": now.isoformat(),
            "rows": len(rows_out),
            "source": INP,
            "out": OUT,
            "computed_fields": COMPUTED_FIELDS,
            "raw_headers": hdrs,
            "out_fields": out_fields,
        },
    )

    save_json(STATE, first_seen)

    print(f"OK: Processed {len(rows_out)} vehicles.")


if __name__ == "__main__":
    main()
