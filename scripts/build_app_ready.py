#!/usr/bin/env python3
"""
build_app_ready.py

Turns the raw dtfeed CSV into an app-friendly CSV with:
- stable key (vin preferred, else vehicle_id)
- first_seen tracking + computed age_days
- simple price + discount fields
- meta.json + delta.json outputs

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


def pick(row: Dict[str, Any], *names: str) -> str:
    """Return the first non-empty trimmed value for the given candidate column names."""
    for n in names:
        v = row.get(n)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def to_float(x: Any) -> Optional[float]:
    """Parse currency/number strings like '$12,345.67' into float."""
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = re.sub(r"[^\d.\-]", "", s)  # strip $ , etc
    try:
        return float(s)
    except ValueError:
        return None


def norm_state(s: str) -> str:
    """Normalize vehicle condition/state."""
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


def parse_iso_date(iso_ts: str, fallback_date):
    """Best-effort parse for stored ISO timestamps."""
    try:
        return datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).date()
    except Exception:
        return fallback_date


def find_image_cols(headers: List[str]) -> List[str]:
    """Find image[n].url columns in sorted order."""
    cols = []
    for h in headers:
        m = re.match(r"^image\[(\d+)\]\.url$", (h or "").strip(), re.IGNORECASE)
        if m:
            cols.append((int(m.group(1)), h))
    cols.sort(key=lambda x: x[0])
    return [h for _, h in cols]


def main() -> None:
    INP = os.environ.get("INP", "data/latest/MP16607.csv")
    OUT = os.environ.get("OUT", "data/latest/app_ready.csv")
    STATE = os.environ.get("STATE", "data/state/first_seen.json")
    META = os.environ.get("META", "data/latest/meta.json")
    DELTA = os.environ.get("DELTA", "data/latest/delta.json")

    now = datetime.now(timezone.utc)
    now_date = now.date()

    if not os.path.exists(INP):
        raise SystemExit(f"Input not found: {INP}")

    first_seen: Dict[str, str] = load_json(STATE, {})  # key -> iso ts

    # Load previous OUT to compute delta
    prev: Dict[str, Dict[str, Any]] = {}
    if os.path.exists(OUT):
        with open(OUT, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                k = (row.get("key") or "").strip()
                if k:
                    prev[k] = row

    rows_out: List[Dict[str, Any]] = []
    now_keys: Set[str] = set()

    with open(INP, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        headers = rdr.fieldnames or []
        image_cols = find_image_cols(headers)

        # sanity: require at least one ID column exists in headers
        has_vin = any(h.strip().lower() == "vin" for h in headers)
        has_vid = any(h.strip().lower() == "vehicle_id" for h in headers)
        if not (has_vin or has_vid):
            raise SystemExit("No vin or vehicle_id column found; can’t key rows.")

        for row in rdr:
            vin = pick(row, "vin").upper()
            vehicle_id = pick(row, "vehicle_id").upper()

            # ROW-LEVEL fallback: VIN preferred else vehicle_id
            key = vin or vehicle_id
            if not key:
                continue

            now_keys.add(key)

            if key not in first_seen:
                first_seen[key] = now.isoformat()

            fs_date = parse_iso_date(first_seen[key], now_date)
            age_days = max(0, (now_date - fs_date).days)

            year = pick(row, "year")
            model = pick(row, "model")
            trim = pick(row, "Trim", "trim")

            state = norm_state(pick(row, "state_of_vehicle", "condition", "availability"))

            # keep overlay/exterior so your UI filters still have them
            overlay = pick(row, "fb_page_id", "overlay_handle", "overlay")
            exterior = pick(row, "exterior_color", "exterior", "color")

            url = pick(row, "url")

            # best effort image: first image[n].url if present, else image_url/photo_url
            img = pick(row, *(image_cols[:1] or []), "image_url", "photo_url")

            # prices
            msrp = to_float(pick(row, "price", "msrp"))
            sale = to_float(pick(row, "sale_price", "sale_price_usd", "sale"))

            if sale is None and msrp is not None:
                sale = msrp

            discount: Optional[float] = None
            if msrp is not None and sale is not None and msrp > sale:
                discount = round(msrp - sale, 2)

            stock = pick(row, "stock", "stock_number", "stockNumber", "stock_no", "stock_id")
            if not stock:
                stock = vehicle_id or key

            rows_out.append(
                {
                    "key": key,
                    "vin": vin,
                    "vehicle_id": vehicle_id,
                    "stock": stock,
                    "year": year,
                    "model": model,
                    "trim": trim,
                    "state_of_vehicle": state,
                    "overlay_handle": overlay,
                    "exterior_color": exterior,
                    "age_days": age_days,
                    "sale_price_usd": "" if sale is None else round(sale, 2),
                    "discount_usd": "" if (discount is None or discount <= 0) else discount,
                    "url": url,
                    "image_url": img,
                    "first_seen_utc": first_seen[key],
                }
            )

    # Write OUT (stable ordering helps diffs/commits)
    rows_out.sort(key=lambda r: (r.get("state_of_vehicle", ""), r.get("model", ""), r.get("trim", ""), r["key"]))

    out_dir = os.path.dirname(OUT)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    fieldnames = [
        "key",
        "vin",
        "vehicle_id",
        "stock",
        "year",
        "model",
        "trim",
        "state_of_vehicle",
        "overlay_handle",
        "exterior_color",
        "age_days",
        "sale_price_usd",
        "discount_usd",
        "url",
        "image_url",
        "first_seen_utc",
    ]

    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_out)

    # Delta
    added = sorted(now_keys - set(prev.keys()))
    removed = sorted(set(prev.keys()) - now_keys)

    cur_price = {r["key"]: str(r.get("sale_price_usd", "")).strip() for r in rows_out}
    price_changed: List[str] = []
    for k in (now_keys & set(prev.keys())):
        a = str(prev[k].get("sale_price_usd", "")).strip()
        b = cur_price.get(k, "").strip()
        if a and b and a != b:
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
        },
    )

    save_json(STATE, first_seen)

    print(f"OK: wrote {OUT} ({len(rows_out)} rows)")
    print(f"OK: wrote {META} and {DELTA}")
    print(f"OK: updated {STATE} ({len(first_seen)} keys)")


if __name__ == "__main__":
    main()
