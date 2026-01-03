#!/usr/bin/env python3
"""
build_app_ready.py

Turns the raw dtfeed CSV into app-friendly outputs:
- data/latest/app_ready.csv
- data/latest/meta.json
- data/latest/delta.json
- data/state/first_seen.json

Key behaviors:
- Stable key = VIN if present else vehicle_id
- Track first_seen per key and compute age_days = (today - first_seen_date)
- Light normalization of state_of_vehicle + numeric price parsing
- Only overwrites output files if content actually changed (avoids empty commits)

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
from typing import Any, Dict, List, Optional, Set, Tuple


# -------------------------
# Helpers
# -------------------------
def pick(row: Dict[str, Any], *names: str) -> str:
    for n in names:
        v = row.get(n, "")
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
    s = re.sub(r"[^\d.\-]", "", s)
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


def save_json_if_changed(path: str, obj: Any) -> bool:
    """Write JSON only if different; returns True if file changed/written."""
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    new_text = json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=False) + "\n"
    old_text = ""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            old_text = f.read()

    if old_text == new_text:
        return False

    with open(path, "w", encoding="utf-8") as f:
        f.write(new_text)
    return True


def parse_iso_date(iso_ts: str, fallback_date):
    try:
        clean = (iso_ts or "").replace("Z", "+00:00")
        return datetime.fromisoformat(clean).date()
    except Exception:
        return fallback_date


def find_image_cols(headers: List[str]) -> List[str]:
    cols: List[Tuple[int, str]] = []
    for h in headers:
        m = re.match(r"^image\[(\d+)\]\.url$", (h or "").strip(), re.IGNORECASE)
        if m:
            cols.append((int(m.group(1)), h))
    cols.sort(key=lambda x: x[0])
    return [h for _, h in cols]


def write_csv_if_changed(path: str, fieldnames: List[str], rows: List[Dict[str, Any]]) -> bool:
    """Write CSV only if different; returns True if file changed/written."""
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    # Build CSV string in-memory
    import io

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    new_text = buf.getvalue()

    old_text = ""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            old_text = f.read()

    if old_text == new_text:
        return False

    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(new_text)
    return True


# -------------------------
# Main
# -------------------------
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

    # Load first_seen map
    first_seen: Dict[str, str] = load_json(STATE, {})

    # Load previous OUT for delta comparisons
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
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        image_cols = find_image_cols(headers)

        has_vin = "vin" in (h.lower() for h in headers)

        for row in reader:
            vin = pick(row, "vin").upper()
            vid = pick(row, "vehicle_id").upper()

            key = vin or vid
            if not key:
                continue

            now_keys.add(key)

            if key not in first_seen:
                first_seen[key] = now.isoformat()

            fs_date = parse_iso_date(first_seen[key], today)
            age_days = max(0, (today - fs_date).days)

            year = pick(row, "year")
            model = pick(row, "model")
            trim = pick(row, "Trim", "trim")
            url = pick(row, "url")

            # price logic
            msrp = to_float(pick(row, "price", "msrp"))
            sale = to_float(pick(row, "sale_price", "sale_price_usd", "sale"))
            if sale is None and msrp is not None:
                sale = msrp

            discount: Optional[float] = None
            if msrp is not None and sale is not None and msrp > sale:
                discount = round(msrp - sale, 2)

            state_of_vehicle = norm_state(pick(row, "state_of_vehicle", "condition", "availability"))

            stock = pick(row, "Stock #", "stock", "stock_number", "stockNumber", "stock_no", "stock_id")
            if not stock:
                stock = vid or key

            # image: prefer image[0].url if present, else fallbacks
            image_url = ""
            if image_cols:
                image_url = pick(row, image_cols[0])
            if not image_url:
                image_url = pick(row, "image[0].url", "image_url", "photo_url", "Photo Url List")

            rows_out.append(
                {
                    "key": key,
                    "vin": vin if vin else (key if has_vin else ""),
                    "vehicle_id": vid,
                    "stock": stock,
                    "year": year,
                    "model": model,
                    "trim": trim,
                    "state_of_vehicle": state_of_vehicle,
                    "age_days": age_days,
                    "sale_price_usd": "" if sale is None else round(sale, 2),
                    "discount_usd": "" if (discount is None or discount <= 0) else discount,
                    "url": url,
                    "image_url": image_url,
                    "first_seen_utc": first_seen[key],
                }
            )

    # Sort for stability (reduces noisy diffs)
    rows_out.sort(key=lambda r: (r.get("state_of_vehicle", ""), r.get("model", ""), r.get("key", "")))

    # Compute delta
    added = sorted(list(now_keys - set(prev.keys())))
    removed = sorted(list(set(prev.keys()) - now_keys))

    cur_price: Dict[str, Optional[float]] = {}
    for r in rows_out:
        cur_price[r["key"]] = to_float(r.get("sale_price_usd"))

    price_changed: List[str] = []
    for k in (now_keys & set(prev.keys())):
        old_p = to_float(prev[k].get("sale_price_usd"))
        new_p = cur_price.get(k)
        if old_p is not None and new_p is not None and abs(old_p - new_p) > 0.01:
            price_changed.append(k)
    price_changed = sorted(price_changed)

    # Write outputs only if changed
    fieldnames = [
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

    changed_any = False
    changed_any |= write_csv_if_changed(OUT, fieldnames, rows_out)
    changed_any |= save_json_if_changed(STATE, first_seen)

    # Only update meta/delta when something meaningful changed
    if changed_any:
        save_json_if_changed(
            META,
            {
                "ts_utc": now.isoformat(),
                "rows": len(rows_out),
                "source": INP,
                "out": OUT,
            },
        )
        save_json_if_changed(
            DELTA,
            {
                "ts_utc": now.isoformat(),
                "added": added,
                "removed": removed,
                "price_changed": price_changed,
                "counts": {"now": len(now_keys), "prev": len(prev)},
            },
        )

    print(f"OK: Processed {len(rows_out)} vehicles. Changed={changed_any}")


if __name__ == "__main__":
    main()
