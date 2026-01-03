#!/usr/bin/env python3
import csv
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

def pick(row: Dict[str, Any], *names: str) -> str:
    for n in names:
        v = row.get(n)
        if v is not None:
            s = str(v).strip()
            if s: return s
    return ""

def to_float(x: Any) -> Optional[float]:
    if x is None: return None
    s = re.sub(r"[^\d.\-]", "", str(x).strip())
    try:
        return float(s)
    except ValueError:
        return None

def load_json(path: str, default: Any) -> Any:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path: str, obj: Any) -> None:
    folder = os.path.dirname(path)
    if folder: os.makedirs(folder, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def find_image_cols(headers: List[str]) -> List[str]:
    cols = []
    for h in headers:
        m = re.match(r"^image\[(\d+)\]\.url$", (h or "").strip(), re.IGNORECASE)
        if m: cols.append((int(m.group(1)), h))
    cols.sort(key=lambda x: x[0])
    return [h for _, h in cols]

def main() -> None:
    INP = os.environ.get("INP", "data/latest/MP16607.csv")
    OUT = os.environ.get("OUT", "data/latest/app_ready.csv")
    STATE = os.environ.get("STATE", "data/state/first_seen.json")
    META = os.environ.get("META", "data/latest/meta.json")
    DELTA = os.environ.get("DELTA", "data/latest/delta.json")

    now = datetime.now(timezone.utc)
    if not os.path.exists(INP):
        print(f"Input not found: {INP}")
        return

    first_seen = load_json(STATE, {})
    prev_data = {}
    if os.path.exists(OUT):
        with open(OUT, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("key"): prev_data[row["key"]] = row

    rows_out = []
    now_keys = set()

    with open(INP, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        image_cols = find_image_cols(headers)

        for row in reader:
            vin = pick(row, "vin").upper()
            vid = pick(row, "vehicle_id").upper()
            key = vin or vid
            if not key: continue
            now_keys.add(key)

            if key not in first_seen:
                first_seen[key] = now.isoformat()
            
            fs_date = datetime.fromisoformat(first_seen[key].replace("Z", "+00:00")).date()
            age_days = max(0, (now.date() - fs_date).days)

            msrp = to_float(pick(row, "price", "msrp"))
            sale = to_float(pick(row, "sale_price", "sale")) or msrp
            discount = round(msrp - sale, 2) if (msrp and sale and msrp > sale) else 0

            rows_out.append({
                "key": key,
                "vin": vin,
                "vehicle_id": vid,
                "stock": pick(row, "Stock #", "stock", "stock_number") or vid,
                "year": pick(row, "year"),
                "model": pick(row, "model"),
                "trim": pick(row, "Trim", "trim"),
                "state_of_vehicle": pick(row, "state_of_vehicle", "condition").upper(),
                "age_days": age_days,
                "sale_price_usd": round(sale, 2) if sale else "",
                "discount_usd": discount if discount > 0 else "",
                "url": pick(row, "url"),
                "image_url": pick(row, *(image_cols[:1] or []), "image_url", "photo_url", "Photo Url List"),
                "first_seen_utc": first_seen[key],
            })

    rows_out.sort(key=lambda r: (r["state_of_vehicle"], r["model"], r["key"]))
    save_json(STATE, first_seen)
    
    # Write CSV
    if rows_out:
        with open(OUT, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
            w.writeheader()
            w.writerows(rows_out)

    # Delta logic with Float comparison
    added = sorted(now_keys - set(prev_data.keys()))
    removed = sorted(set(prev_data.keys()) - now_keys)
    price_changed = []
    for k in (now_keys & set(prev_data.keys())):
        old_p = to_float(prev_data[k].get("sale_price_usd"))
        new_item = next((r for r in rows_out if r["key"] == k), None)
        new_p = to_float(new_item["sale_price_usd"]) if new_item else None
        if old_p and new_p and abs(old_p - new_p) > 0.1:
            price_changed.append(k)

    save_json(DELTA, {"ts_utc": now.isoformat(), "added": added, "removed": removed, "price_changed": price_changed})
    save_json(META, {"ts_utc": now.isoformat(), "rows": len(rows_out)})
    print(f"OK: Processed {len(rows_out)} vehicles.")

if __name__ == "__main__":
    main()
