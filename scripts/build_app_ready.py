#!/usr/bin/env python3
"""
build_app_ready.py

Turns the raw dtfeed CSV into app-friendly outputs:
- data/latest/app_ready.csv
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


OUT_FIELDS = [
    "key",
    "vin",
    "vehicle_id",
    "stock",
    "year",
    "model",
    "trim",
    "state_of_vehicle",
    "age_days",
    "sale_price_usd",
    "discount_usd",
    "url",
    "image_url",
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
    s = re.sub(r"[^\d.\-]", "", s)  # remove $ , etc.
    try:
        return float(s)
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


def find_image_cols(headers: List[str]) -> List[str]:
    """
    Detect columns like image[0].url, image[1].url, ... and return them in numeric order.
    """
    found: List[tuple[int, str]] = []
    for h in headers:
        name = (h or "").strip()
        m = re.match(r"^image\[(\d+)\]\.url$", name, re.IGNORECASE)
        if m:
            found.append((int(m.group(1)), h))
    found.sort(key=lambda x: x[0])
    return [h for _, h in found]


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

    first_seen: Dict[str, str] = load_json(STATE, {})  # key -> iso ts

    # previous OUT for delta
    prev: Dict[str, Dict[str, Any]] = {}
    if os.path.exists(OUT):
        with open(OUT, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                k = (row.get("key") or "").strip()
                if k:
                    prev[k] = row

    rows_out: List[Dict[str, Any]] = []
    now_keys: Set[str] = set()

    with open(INP, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        headers = rdr.fieldnames or []
        image_cols = find_image_cols(headers)

        # Prefer VIN if present in file; otherwise use vehicle_id
        has_vin = "vin" in [h.lower() for h in headers]
        has_vid = "vehicle_id" in [h.lower() for h in headers]
        if not (has_vin or has_vid):
            raise SystemExit("No vin or vehicle_id column found; can’t key rows.")

        for row in rdr:
            vin = pick(row, "vin").upper()
            vid = pick(row, "vehicle_id").upper()
            key = vin or vid
            if not key:
                continue

            now_keys.add(key)

            if key not in first_seen:
                first_seen[key] = now.isoformat()

            fs_date = parse_first_seen_date(first_seen[key], today)
            age_days = max(0, (today - fs_date).days)

            msrp = to_float(pick(row, "price", "msrp"))
            sale = to_float(pick(row, "sale_price", "sale_price_usd", "sale"))
            if sale is None and msrp is not None:
                sale = msrp

            discount: Optional[float] = None
            if msrp is not None and sale is not None and msrp > sale:
                discount = round(msrp - sale, 2)

            state = norm_state(pick(row, "state_of_vehicle", "condition", "availability"))

            stock = pick(row, "stock", "Stock #", "stock_number", "stockNumber", "stock_no", "stock_id")
            if not stock:
                stock = vid or key

            image_url = pick(
                row,
                *(image_cols[:1] if image_cols else []),
                "image_url",
                "photo_url",
                "Photo Url List",
            )

            rows_out.append(
                {
                    "key": key,
                    "vin": vin,
                    "vehicle_id": vid,
                    "stock": stock,
                    "year": pick(row, "year"),
                    "model": pick(row, "model"),
                    "trim": pick(row, "Trim", "trim"),
                    "state_of_vehicle": state,
                    "age_days": age_days,
                    "sale_price_usd": "" if sale is None else round(sale, 2),
                    "discount_usd": "" if (discount is None or discount <= 0) else discount,
                    "url": pick(row, "url"),
                    "image_url": image_url,
                    "first_seen_utc": first_seen[key],
                }
            )

    # stable ordering
    rows_out.sort(key=lambda r: (r["state_of_vehicle"], r["model"], r["key"]))

    # write outputs
    out_dir = os.path.dirname(OUT)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        w.writeheader()
        for r in rows_out:
            w.writerow({k: r.get(k, "") for k in OUT_FIELDS})

    # delta
    added = sorted(now_keys - set(prev.keys()))
    removed = sorted(set(prev.keys()) - now_keys)

    cur_price = {r["key"]: to_float(r.get("sale_price_usd")) for r in rows_out}
    price_changed: List[str] = []
    for k in (now_keys & set(prev.keys())):
        old_p = to_float(prev[k].get("sale_price_usd"))
        new_p = cur_price.get(k)
        if old_p is not None and new_p is not None and abs(old_p - new_p) > 0.1:
            price_changed.append(k)

    save_json(DELTA, {
        "ts_utc": now.isoformat(),
        "added": added,
        "removed": removed,
        "price_changed": sorted(price_changed),
        "counts": {"now": len(now_keys), "prev": len(prev)},
    })

    save_json(META, {
        "ts_utc": now.isoformat(),
        "rows": len(rows_out),
        "source": INP,
        "out": OUT,
    })

    save_json(STATE, first_seen)

    print(f"OK: Processed {len(rows_out)} vehicles.")


if __name__ == "__main__":
    main()
